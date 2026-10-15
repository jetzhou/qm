import assert from "node:assert/strict";
import { test } from "node:test";
import { createHmac } from "node:crypto";
import { createSlackUserEvents } from "../src/loops/slack-user-events.ts";
import type { ConversationEvent } from "../src/loops/sources/adapter.ts";
import type { Loop } from "../src/types.ts";

const SECRET = "test-signing-secret";
const loops = [
  { id: "l-josh", owner: "josh@example.com", surface: "inbox:slack", state: "enabled" },
  { id: "l-mark", owner: "mark@example.com", surface: "inbox:slack", state: "enabled" },
] as unknown as Loop[];
const tokensByOwner: Record<string, string> = { "josh@example.com": "tok-josh", "mark@example.com": "tok-mark" };
const people: Record<string, string> = { "tok-josh": "UJOSH", "tok-mark": "UMARK" };

function setup(opts: { secret?: string; appToken?: string } = {}) {
  const seen: Array<{ event: ConversationEvent; owners: string[] }> = [];
  let authTests = 0;
  const events = createSlackUserEvents({
    signingSecret: async () => ("secret" in opts ? opts.secret : SECRET),
    ...(opts.appToken ? { appToken: async () => opts.appToken } : {}),
    loops: { list: async () => loops },
    tokens: {
      connectorAccessToken: async (_host, owner) => tokensByOwner[owner] ?? null,
    },
    onConversationEvent: async (event, { owners }) => {
      seen.push({ event, owners });
    },
    fetchImpl: async (input, init) => {
      const method = String(input).split("/").at(-1);
      const token = String(((init?.headers ?? {}) as Record<string, string>).authorization).slice(7);
      if (method === "auth.test") {
        authTests++;
        return Response.json({ ok: true, team_id: "T1", user_id: people[token] });
      }
      if (method === "apps.event.authorizations.list")
        return Response.json({ ok: true, authorizations: [{ team_id: "T1", user_id: "UMARK", is_bot: false }] });
      return Response.json({ ok: false, error: "unknown_method" });
    },
  });
  return { events, seen, authTests: () => authTests };
}

function signed(body: unknown, secret = SECRET, at = Math.floor(Date.now() / 1000)) {
  const rawBody = JSON.stringify(body);
  const sig = `v0=${createHmac("sha256", secret).update(`v0:${at}:${rawBody}`).digest("hex")}`;
  return { rawBody, headers: { "x-slack-signature": sig, "x-slack-request-timestamp": String(at) } };
}

const message = (event: Record<string, unknown>, user = "UJOSH") => ({
  type: "event_callback",
  event_id: `Ev${Math.random()}`,
  event_context: "ctx",
  authorizations: [{ team_id: "T1", user_id: user, is_bot: false }],
  event: { type: "message", channel: "D1", channel_type: "im", ts: "1700000000.000100", ...event },
});

test("verifies signatures, answers the URL challenge, and is off without a signing secret", async () => {
  const { events } = setup();
  assert.equal((await events.receive(signed({ type: "url_verification", challenge: "abc" }, "wrong"))).status, 401);
  assert.deepEqual(await events.receive(signed({ type: "url_verification", challenge: "abc" })), {
    status: 200,
    body: "abc",
  });
  const unconfigured = setup({ secret: undefined }).events;
  assert.equal((await unconfigured.receive(signed(message({ user: "UBOB" })))).status, 404);
  assert.deepEqual(
    await unconfigured.receive({
      headers: {},
      rawBody: JSON.stringify({ type: "url_verification", challenge: "abc" }),
    }),
    {
      status: 200,
      body: "abc",
    },
  );
});

test("the connected person's own message carries their identity; others' do not", async () => {
  const { events, seen } = setup();
  await events.handle(message({ user: "UJOSH", text: "on it" }));
  await events.handle(message({ user: "UBOB", text: "any update?", ts: "1700000001.000100" }));
  assert.deepEqual(seen, [
    {
      event: {
        source: "slack",
        conversationRef: "D1",
        at: 1700000000000,
        text: "on it",
        senderEmail: "josh@example.com",
      },
      owners: ["josh@example.com"],
    },
    {
      event: { source: "slack", conversationRef: "D1", at: 1700000001000, text: "any update?" },
      owners: ["josh@example.com"],
    },
  ]);
});

test("thread replies key by thread root and edits, bots and unknown people are skipped", async () => {
  const { events, seen } = setup();
  await events.handle(
    message({ channel: "C9", channel_type: "channel", ts: "200.2", thread_ts: "100.1", user: "UBOB" }),
  );
  await events.handle(message({ subtype: "message_changed" }));
  await events.handle(message({ bot_id: "B1", user: "UBOB" }));
  await events.handle(message({ user: "UBOB" }, "USTRANGER"));
  assert.deepEqual(
    seen.map((s) => s.event.conversationRef),
    ["C9:100.1"],
  );
});

test("an app token fans one event out to every connected person, each scoped to their own Loops", async () => {
  const { events, seen, authTests } = setup({ appToken: "xapp" });
  await events.handle(message({ user: "UJOSH", text: "hey mark" }));
  assert.deepEqual(
    seen.map((s) => [s.owners[0], s.event.senderEmail]),
    [
      ["josh@example.com", "josh@example.com"],
      ["mark@example.com", undefined],
    ],
  );
  await events.handle(message({ user: "UMARK", ts: "1700000002.0" }));
  assert.equal(authTests(), 2, "identities are cached");
});

test("Slack retries of the same event are acknowledged once", async () => {
  const { events, seen } = setup();
  const body = message({ user: "UBOB" });
  assert.equal((await events.receive(signed(body))).body, "ok");
  assert.equal((await events.receive(signed(body))).body, "duplicate");
  for (let i = 0; i < 50 && !seen.length; i++) await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(seen.length, 1);
});
