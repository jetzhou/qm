import assert from "node:assert/strict";
import { test } from "node:test";
import { createHmac } from "node:crypto";
import { createSlackUserEvents } from "../src/loops/slack-user-events.ts";
import type { ConversationEvent } from "../src/loops/sources/adapter.ts";
import type { Loop } from "../src/types.ts";

const SECRET = "test-signing-secret";
const loops = [
  { id: "l-ada", owner: "ada@example.com", surface: "inbox:slack", state: "enabled" },
  { id: "l-sam", owner: "sam@example.com", surface: "inbox:slack", state: "enabled" },
] as unknown as Loop[];
const tokensByOwner: Record<string, string> = { "ada@example.com": "tok-ada", "sam@example.com": "tok-sam" };
const people: Record<string, string> = { "tok-ada": "UADA", "tok-sam": "USAM" };

function setup(opts: { secret?: string; fail?: boolean } = {}) {
  const seen: Array<{ event: ConversationEvent; owners: string[] }> = [];
  let authTests = 0;
  const events = createSlackUserEvents({
    signingSecret: async () => ("secret" in opts ? opts.secret : SECRET),
    loops: { list: async () => loops },
    tokens: { connectorAccessToken: async (_host, owner) => tokensByOwner[owner] ?? null },
    slackClient: (token) => ({
      auth: {
        test: async () => {
          authTests++;
          return { team_id: "T1", user_id: people[token] };
        },
      },
    }),
    onConversationEvent: async (event, { owners }) => {
      if (opts.fail) throw new Error("queue unavailable");
      seen.push({ event, owners });
    },
  });
  return { events, seen, authTests: () => authTests };
}

function signed(body: unknown, secret = SECRET, at = Math.floor(Date.now() / 1000)) {
  const rawBody = JSON.stringify(body);
  const sig = `v0=${createHmac("sha256", secret).update(`v0:${at}:${rawBody}`).digest("hex")}`;
  return { rawBody, headers: { "x-slack-signature": sig, "x-slack-request-timestamp": String(at) } };
}

const message = (event: Record<string, unknown>, user = "UADA") =>
  signed({
    type: "event_callback",
    event_id: `Ev${Math.random()}`,
    authorizations: [{ team_id: "T1", user_id: user, is_bot: false }],
    event: { type: "message", channel: "D1", channel_type: "im", ts: "1700000000.000100", ...event },
  });

test("verifies signatures, answers the URL challenge, and is off without a signing secret", async () => {
  const { events } = setup();
  const challenge = { type: "url_verification", challenge: "abc" };
  assert.equal((await events.receive(signed(challenge, "wrong"))).status, 401);
  assert.deepEqual(await events.receive(signed(challenge)), { status: 200, body: "abc" });
  const unconfigured = setup({ secret: undefined }).events;
  assert.equal((await unconfigured.receive(signed(challenge))).status, 404);
  assert.equal((await unconfigured.receive(message({ user: "UBOB" }))).status, 404);
});

test("the connected person's own message carries their identity; others' do not", async () => {
  const { events, seen, authTests } = setup();
  await events.receive(message({ user: "UADA", text: "on it" }));
  await events.receive(message({ user: "UBOB", text: "any update?", ts: "1700000001.000100" }));
  assert.deepEqual(seen, [
    {
      event: {
        source: "slack",
        conversationRef: "D1",
        at: 1700000000000,
        text: "on it",
        senderEmail: "ada@example.com",
      },
      owners: ["ada@example.com"],
    },
    {
      event: { source: "slack", conversationRef: "D1", at: 1700000001000, text: "any update?" },
      owners: ["ada@example.com"],
    },
  ]);
  assert.equal(authTests(), 2);
});

test("thread replies key by thread root and edits, bots and unknown people are skipped", async () => {
  const { events, seen } = setup();
  await events.receive(
    message({ channel: "C9", channel_type: "channel", ts: "200.2", thread_ts: "100.1", user: "UBOB" }),
  );
  await events.receive(message({ subtype: "message_changed" }));
  await events.receive(message({ bot_id: "B1", user: "UBOB" }));
  await events.receive(message({ user: "UBOB" }, "USTRANGER"));
  assert.deepEqual(
    seen.map((s) => s.event.conversationRef),
    ["C9:100.1"],
  );
});

test("an event is acknowledged only after it is queued", async () => {
  await assert.rejects(setup({ fail: true }).events.receive(message({ user: "UBOB" })), /queue unavailable/);
});
