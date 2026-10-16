import type { LoopStore } from "./loop-store.ts";
import { isObj, tokenFor, type ConnectorTokenSource, type ConversationEvent } from "./sources/adapter.ts";
import { slackConversationRef } from "./sources/slack.ts";
import { getVerifier, type VerifierInput } from "../webhooks/verifiers.ts";
import { swallow } from "../util/errors.ts";

export interface SlackUserEventsDeps {
  signingSecret: () => Promise<string | undefined>;
  loops: Pick<LoopStore, "list">;
  tokens: ConnectorTokenSource;
  slackClient: (token: string) => { auth: { test(): Promise<{ user_id?: string; team_id?: string }> } };
  onConversationEvent: (event: ConversationEvent, opts: { owners: string[] }) => Promise<void>;
}

const HANDLED_SUBTYPES = new Set([undefined, "thread_broadcast", "file_share", "me_message"]);
const IDENTITY_TTL_MS = 3_600_000;
const UNKNOWN_RETRY_MS = 60_000;

export function createSlackUserEvents(deps: SlackUserEventsDeps) {
  let identities = new Map<string, string[]>();
  let identitiesAt = 0;
  let rebuilding: Promise<void> | undefined;

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
          const me = await deps.slackClient(token).auth.test();
          if (!me.user_id || !me.team_id) return;
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

  async function handle(payload: Record<string, unknown>): Promise<void> {
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
    for (const authorization of Array.isArray(payload.authorizations) ? payload.authorizations : []) {
      if (!isObj(authorization) || authorization.is_bot === true) continue;
      const { team_id: team, user_id: user } = authorization;
      if (typeof team !== "string" || typeof user !== "string") continue;
      for (const owner of await ownersFor(team, user))
        await deps.onConversationEvent(
          {
            source: "slack",
            conversationRef,
            at,
            ...(text ? { text } : {}),
            ...(event.user === user ? { senderEmail: owner } : {}),
          },
          { owners: [owner] },
        );
    }
  }

  return {
    async receive(request: {
      headers: VerifierInput["headers"];
      rawBody: string;
    }): Promise<{ status: number; body: string }> {
      const secret = await deps.signingSecret();
      if (!secret) return { status: 404, body: "Not found" };
      const verifier = getVerifier("slack")!;
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
      if (isObj(payload) && payload.type === "event_callback") await handle(payload);
      return { status: 200, body: "ok" };
    },
  };
}

export type SlackUserEvents = ReturnType<typeof createSlackUserEvents>;
