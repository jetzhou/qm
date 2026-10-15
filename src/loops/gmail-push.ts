import { createRemoteJWKSet, jwtVerify } from "jose";
import type { ConnectorTokenSource } from "./sources/adapter.ts";
import type { IngressEntry, LatestEvent } from "./ingress.ts";

export interface GmailPushConfig {
  topic: string;
  audience: string;
  serviceAccount: string;
}

export interface GmailCursor {
  email: string;
  historyId: string;
  expiresAt: number;
}

const keys = createRemoteJWKSet(new URL("https://www.googleapis.com/oauth2/v3/certs"));

export async function verifyGmailPush(authorization: string | undefined, config: GmailPushConfig): Promise<boolean> {
  if (!authorization?.startsWith("Bearer ")) return false;
  try {
    const { payload } = await jwtVerify(authorization.slice(7), keys, {
      issuer: ["https://accounts.google.com", "accounts.google.com"],
      audience: config.audience,
      algorithms: ["RS256"],
    });
    return payload.email === config.serviceAccount && payload.email_verified === true;
  } catch {
    return false;
  }
}

interface GmailMessage {
  id: string;
  threadId: string;
  internalDate: string;
  snippet?: string;
  labelIds?: string[];
  payload?: { headers?: Array<{ name: string; value: string }> };
}

/** How long a polled (no Pub/Sub) cursor is trusted before the profile is reread. */
const POLL_CURSOR_TTL_MS = 7 * 86_400_000;

/**
 * Gmail change reader. With Pub/Sub config it registers a push watch; without it
 * the same history cursor is polled, so mailbox events still reach the Loop.
 */
export function createGmailPushClient(
  tokens: ConnectorTokenSource,
  config: GmailPushConfig | undefined,
  fetchImpl = fetch,
) {
  async function request<T>(owner: string, path: string, body?: unknown): Promise<T> {
    const token = await tokens.connectorAccessToken("gmail.googleapis.com", owner, "personal");
    if (!token) throw new Error("Connect a personal Gmail account before enabling Gmail Pub/Sub.");
    const response = await fetchImpl(`https://gmail.googleapis.com/gmail/v1/users/me/${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(20_000),
    });
    if (!response.ok)
      throw Object.assign(new Error(`Gmail ${path.split("?")[0]} returned ${response.status}`), {
        status: response.status,
      });
    return (await response.json()) as T;
  }

  return {
    async watch(owner: string): Promise<GmailCursor> {
      if (!config) {
        const profile = await request<{ emailAddress: string; historyId: string }>(owner, "profile");
        if (!profile.emailAddress || !/^\d+$/.test(String(profile.historyId)))
          throw new Error("Gmail returned an invalid profile");
        return {
          email: profile.emailAddress.toLowerCase(),
          historyId: String(profile.historyId),
          expiresAt: Date.now() + POLL_CURSOR_TTL_MS,
        };
      }
      const profile = await request<{ emailAddress: string }>(owner, "profile");
      const watch = await request<{ historyId: string; expiration: string }>(owner, "watch", {
        topicName: config.topic,
        // SENT lets the owner's own replies reach the Loop as events.
        labelIds: ["INBOX", "SENT"],
        labelFilterBehavior: "include",
      });
      if (!profile.emailAddress || !/^\d+$/.test(watch.historyId) || !Number.isFinite(Number(watch.expiration)))
        throw new Error("Gmail returned an invalid watch response");
      return {
        email: profile.emailAddress.toLowerCase(),
        historyId: watch.historyId,
        expiresAt: Number(watch.expiration),
      };
    },
    async changes(
      owner: string,
      loopId: string,
      cursor: GmailCursor,
      ingest: (entries: IngressEntry[]) => Promise<unknown>,
    ): Promise<string> {
      const profile = await request<{ emailAddress: string; historyId: string }>(owner, "profile");
      if (profile.emailAddress.toLowerCase() !== cursor.email)
        throw new Error("Connected Gmail account changed; reconnect this ingestion source.");
      const importMessages = async (ids: string[]) => {
        for (const id of new Set(ids)) {
          let message: GmailMessage;
          try {
            message = await request<GmailMessage>(owner, `messages/${encodeURIComponent(id)}?format=metadata`);
          } catch (error) {
            if ((error as { status?: number }).status === 404) continue;
            throw error;
          }
          const sent = message.labelIds?.includes("SENT") === true;
          if (!sent && !message.labelIds?.includes("INBOX")) continue;
          const headers = message.payload?.headers ?? [];
          const header = (name: string) => headers.find((entry) => entry.name.toLowerCase() === name)?.value;
          const at = Number(message.internalDate);
          if (!message.threadId || !Number.isFinite(at)) throw new Error("Gmail returned invalid message metadata");
          const latestEvent: LatestEvent = {
            author: sent ? "owner" : "other",
            at,
            ...(message.snippet ? { text: message.snippet } : {}),
            ...(!sent && header("from") ? { from: header("from") } : {}),
          };
          if (sent) {
            // The owner's own mail only updates a thread already on the ledger;
            // the Loop's work stage decides whether it settled the conversation.
            await ingest([
              {
                loopId,
                source: "gmail",
                dedupeKey: message.threadId,
                sourceAt: at,
                merge: true,
                sourcePayload: { latestEvent },
              },
            ]);
            continue;
          }
          await ingest([
            {
              loopId,
              source: "gmail",
              dedupeKey: message.threadId,
              summary: message.snippet ?? "New email",
              sourceAt: at,
              sourcePayload: {
                source: "gmail",
                title: header("subject") ?? "(No subject)",
                from: header("from") ?? cursor.email,
                snippet: message.snippet ?? "New email",
                receivedAt: at,
                latestEvent,
                externalUrl: `https://mail.google.com/mail/u/${encodeURIComponent(cursor.email)}/#inbox/${message.threadId}`,
                gmail: {
                  threadId: message.threadId,
                  messageId: message.id,
                  rfcMessageId: header("message-id"),
                  subject: header("subject"),
                  to: [header("reply-to") ?? header("from") ?? ""],
                  accountEmail: cursor.email,
                  accountType: "personal",
                },
              },
            },
          ]);
        }
      };
      let pageToken: string | undefined;
      let latest: string;
      try {
        do {
          const query = new URLSearchParams({ startHistoryId: cursor.historyId, maxResults: "100" });
          if (pageToken) query.set("pageToken", pageToken);
          const page = await request<{
            historyId: string;
            nextPageToken?: string;
            history?: Array<{
              messagesAdded?: Array<{ message: GmailMessage }>;
              labelsAdded?: Array<{ message: GmailMessage; labelIds: string[] }>;
            }>;
          }>(owner, `history?${query}`);
          await importMessages(
            (page.history ?? []).flatMap((entry) => [
              ...(entry.messagesAdded ?? []).map((row) => row.message.id),
              ...(entry.labelsAdded ?? []).filter((row) => row.labelIds.includes("INBOX")).map((row) => row.message.id),
            ]),
          );
          pageToken = page.nextPageToken;
          latest = page.historyId;
        } while (pageToken);
      } catch (error) {
        if ((error as { status?: number }).status !== 404) throw error;
        pageToken = undefined;
        do {
          const query = new URLSearchParams({ labelIds: "INBOX", maxResults: "100" });
          if (pageToken) query.set("pageToken", pageToken);
          const page = await request<{ nextPageToken?: string; messages?: Array<{ id: string }> }>(
            owner,
            `messages?${query}`,
          );
          await importMessages((page.messages ?? []).map((message) => message.id));
          pageToken = page.nextPageToken;
        } while (pageToken);
        latest = profile.historyId;
      }
      if (!/^\d+$/.test(latest)) throw new Error("Gmail returned an invalid history cursor");
      return latest;
    },
  };
}
