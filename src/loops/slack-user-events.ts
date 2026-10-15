import { LRUCache } from "lru-cache";
import type { LoopStore } from "./loop-store.ts";
import type { ConversationEvent } from "./sources/adapter.ts";
import { tokenFor, type ConnectorTokenSource } from "./sources/adapter.ts";
import { slackConversationRef } from "./sources/slack.ts";
import { getVerifier, type VerifierInput } from "../webhooks/verifiers.ts";
import { normalizeSlackApiUrl } from "../slack/config.ts";
import { swallow } from "../util/errors.ts";

/**
 * Slack Events API receiver for the Slack connector app (the user-token app people
 * connect from the keychain, not QM's bot). With user-token event subscriptions
 * (message.im, message.mpim, message.channels, message.groups) Slack sends every
 * message in conversations the connected person can see, including their own
 * replies. Each message becomes a conversation event for that person's inbox Loops,
 * where the Loop's work stage decides what it means.
 */
export interface SlackUserEventsDeps {
  /** The connector app's signing secret; read per request so an admin can set it without a restart. */
  signingSecret: () => Promise<string | undefined>;
  /** Optional app-level token (authorizations:read) to fan an event out to every connected person. */
  appToken?: () => Promise<string | undefined>;
  loops: Pick<LoopStore, "list">;
  tokens: ConnectorTokenSource;
  onConversationEvent: (event: ConversationEvent, opts: { owners: string[] }) => Promise<void>;
  slackApiUrl?: string;
  fetchImpl?: typeof fetch;
}

const HANDLED_SUBTYPES = new Set([undefined, "thread_broadcast", "file_share", "me_message"]);
const IDENTITY_TTL_MS = 3_600_000;
const UNKNOWN_RETRY_MS = 60_000;

type Obj = Record<string, unknown>;
const isObj = (value: unknown): value is Obj => typeof value === "object" && value !== null;

function urlChallenge(rawBody: string): string | null {
  try {
    const body: unknown = JSON.parse(rawBody);
    return isObj(body) && body.type === "url_verification" && typeof body.challenge === "string"
      ? body.challenge.slice(0, 200)
      : null;
  } catch {
    return null;
  }
}

export function createSlackUserEvents(deps: SlackUserEventsDeps) {
  const fetchImpl = deps.fetchImpl ?? fetch;
  const base = normalizeSlackApiUrl(deps.slackApiUrl ?? "https://slack.com/api");
  const seen = new LRUCache<string, true>({ max: 5_000, ttl: 3_600_000 });
  /** "<team>:<user>" -> QM owners whose Slack connector is that person. */
  let identities = new Map<string, string[]>();
  let identitiesAt = 0;
  let rebuilding: Promise<void> | undefined;

  async function slack(method: string, token: string, params: Record<string, string> = {}): Promise<Obj> {
    const response = await fetchImpl(`${base}/${method}`, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams(params).toString(),
      signal: AbortSignal.timeout(10_000),
    });
    const body = (await response.json().catch(() => ({}))) as Obj;
    if (!response.ok || body.ok !== true)
      throw new Error(`Slack ${method} failed: ${String(body.error ?? response.status)}`);
    return body;
  }

  async function rebuildIdentities(): Promise<void> {
    const owners = new Set<string>();
    for (const loop of await deps.loops.list())
      if (loop.state !== "archived" && (loop.surface === "inbox" || loop.surface === "inbox:slack"))
        owners.add(loop.owner);
    const next = new Map<string, string[]>();
    await Promise.all(
      [...owners].map(async (owner) => {
        try {
          const token = await tokenFor(deps.tokens, "slack.com", owner);
          if (!token) return;
          const me = await slack("auth.test", token);
          if (typeof me.user_id !== "string" || typeof me.team_id !== "string") return;
          const key = `${me.team_id}:${me.user_id}`;
          next.set(key, [...(next.get(key) ?? []), owner]);
        } catch (error) {
          swallow("slack user events: identify connector", error);
        }
      }),
    );
    identities = next;
    identitiesAt = Date.now();
  }

  async function ownersFor(team: string, user: string): Promise<string[]> {
    const key = `${team}:${user}`;
    const age = Date.now() - identitiesAt;
    if (age > IDENTITY_TTL_MS || (!identities.has(key) && age > UNKNOWN_RETRY_MS)) {
      rebuilding ??= rebuildIdentities().finally(() => {
        rebuilding = undefined;
      });
      await rebuilding;
    }
    return identities.get(key) ?? [];
  }

  async function authorizedUsers(payload: Obj): Promise<Array<{ team: string; user: string }>> {
    const listed = Array.isArray(payload.authorizations) ? payload.authorizations : [];
    const users = listed
      .filter(isObj)
      .filter((a) => a.is_bot !== true && typeof a.user_id === "string" && typeof a.team_id === "string")
      .map((a) => ({ team: String(a.team_id), user: String(a.user_id) }));
    // Slack lists one authorization per event; the app token reveals everyone else it reached.
    const appToken = await deps.appToken?.();
    if (appToken && typeof payload.event_context === "string") {
      try {
        const more = await slack("apps.event.authorizations.list", appToken, { event_context: payload.event_context });
        for (const a of Array.isArray(more.authorizations) ? more.authorizations.filter(isObj) : [])
          if (a.is_bot !== true && typeof a.user_id === "string" && typeof a.team_id === "string")
            users.push({ team: a.team_id, user: a.user_id });
      } catch (error) {
        swallow("slack user events: list authorizations", error);
      }
    }
    return [...new Map(users.map((u) => [`${u.team}:${u.user}`, u])).values()];
  }

  async function handle(payload: Obj): Promise<void> {
    const event = payload.event;
    if (!isObj(event) || event.type !== "message") return;
    if (!HANDLED_SUBTYPES.has(event.subtype as string | undefined) || event.bot_id) return;
    const channel = typeof event.channel === "string" ? event.channel : "";
    const ts = typeof event.ts === "string" ? event.ts : "";
    const at = Math.round(Number(ts) * 1000);
    if (!channel || !Number.isFinite(at) || at <= 0) return;
    const threadTs = typeof event.thread_ts === "string" && event.thread_ts !== ts ? event.thread_ts : undefined;
    const channelType = typeof event.channel_type === "string" ? event.channel_type : "";
    const isDirectMessage = channelType ? channelType === "im" || channelType === "mpim" : undefined;
    const conversationRef = slackConversationRef(channel, ts, threadTs, isDirectMessage);
    const text = typeof event.text === "string" ? event.text : undefined;
    const sender = typeof event.user === "string" ? event.user : "";
    for (const { team, user } of await authorizedUsers(payload)) {
      for (const owner of await ownersFor(team, user)) {
        await deps.onConversationEvent(
          {
            source: "slack",
            conversationRef,
            at,
            ...(text ? { text } : {}),
            // Only the person's own messages carry their identity; the Loop decides the rest.
            ...(sender === user ? { senderEmail: owner } : {}),
          },
          { owners: [owner] },
        );
      }
    }
  }

  return {
    /** Verify, acknowledge fast, and process in the background (Slack retries after 3s). */
    async receive(request: {
      headers: VerifierInput["headers"];
      rawBody: string;
    }): Promise<{ status: number; body: string }> {
      const secret = await deps.signingSecret();
      const verifier = getVerifier("slack")!;
      if (!secret) {
        // Slack checks the Request URL while the app is being created, before its
        // signing secret can be configured here. Echoing a challenge reveals nothing.
        const challenge = urlChallenge(request.rawBody);
        return challenge ? { status: 200, body: challenge } : { status: 404, body: "Not found" };
      }
      const input = { ...request, secret };
      if (!verifier.verify(input)) return { status: 401, body: "Invalid signature" };
      let payload: unknown;
      try {
        payload = JSON.parse(request.rawBody);
      } catch {
        return { status: 400, body: "JSON body required" };
      }
      const challenge = verifier.handshake?.(input, payload);
      if (challenge) return { status: 200, body: challenge };
      if (!isObj(payload) || payload.type !== "event_callback") return { status: 200, body: "ignored" };
      const id = verifier.deliveryId(input);
      if (seen.has(id)) return { status: 200, body: "duplicate" };
      seen.set(id, true);
      void handle(payload).catch((error) => swallow("slack user events", error));
      return { status: 200, body: "ok" };
    },
    /** For tests: process a parsed payload synchronously. */
    handle,
  };
}

export type SlackUserEvents = ReturnType<typeof createSlackUserEvents>;
