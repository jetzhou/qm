import assert from "node:assert/strict";
import { test } from "node:test";
import { createLoopItemLedger, type IngestEntryInput } from "../src/loops/item-ledger.ts";
import { createInboxRealtime } from "../src/loops/inbox-realtime.ts";
import { renderSourceInboxTask } from "../src/loops/inbox-loop.ts";
import { slackConversationRef } from "../src/loops/sources/slack.ts";
import type { LedgerEvent } from "../src/loops/ledger-events.ts";
import type { Loop } from "../src/types.ts";
import { simulatedLoopEvents } from "./support/inbox-events.ts";

const LOOP = { id: "loop-inbox", owner: "josh@example.com", surface: "inbox" } as unknown as Loop;

function slackEntry(over: Partial<IngestEntryInput> = {}): IngestEntryInput {
  return {
    loopId: LOOP.id,
    dedupeKey: "D123",
    source: "slack",
    summary: "any update?",
    sourcePayload: {
      source: "slack",
      title: "DM with Mark",
      from: "Mark",
      snippet: "any update?",
      slack: { channelId: "D123", ts: "1000.100" },
    },
    sourceAt: 1_000_100,
    ...over,
  };
}

function gmailEntry(over: Partial<IngestEntryInput> = {}): IngestEntryInput {
  return {
    loopId: LOOP.id,
    dedupeKey: "gmail:t-1",
    source: "gmail",
    summary: "contract question",
    sourcePayload: {
      source: "gmail",
      title: "Contract question",
      from: "Ada",
      snippet: "contract question",
      gmail: { threadId: "t-1" },
    },
    sourceAt: 1_000_100,
    ...over,
  };
}

function realtime(items: ReturnType<typeof createLoopItemLedger>, opts: { work?: boolean } = {}) {
  const events = simulatedLoopEvents(items, opts);
  const rt = createInboxRealtime({ loops: { list: async () => [LOOP] }, items, enqueue: events.enqueue });
  return Object.assign(rt, { calls: events.calls });
}

test("the owner's own reply is queued for the work stage, which settles the card as replied", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items);
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 2_000_000,
    text: "on it!",
    senderEmail: "Josh@Example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "skipped");
  assert.equal(item!.actionKind, "replied");
  assert.equal(item!.actionResult, "on it!");
  assert.equal(rt.calls.length, 1);
  const [entry] = rt.calls[0]!.entries;
  assert.equal(entry!.merge, true);
  assert.deepEqual(entry!.sourcePayload, { latestEvent: { author: "owner", at: 2_000_000, text: "on it!" } });
  // The card keeps what it was about; the owner's words never become the ask.
  assert.equal(item!.sourcePayload!.snippet, "any update?");
  assert.equal(item!.sourceSummary, "any update?");
  assert.deepEqual(item!.sourcePayload!.slack, { channelId: "D123", ts: "1000.100" });
});

test("someone else's messages each become an event that bumps the card", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items, { work: false });
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 3_000_000,
    text: "also, when works?",
    senderEmail: "mark@example.com",
  });
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 3_001_000,
    text: "ping",
    senderEmail: "mark@example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  assert.equal(item!.sourceAt, 3_001_000);
  assert.equal(item!.sourceSummary, "ping");
  assert.equal(item!.sourcePayload!.snippet, "ping");
  assert.deepEqual(item!.sourcePayload!.latestEvent, {
    author: "other",
    at: 3_001_000,
    text: "ping",
    from: "mark@example.com",
  });
  assert.equal(rt.calls.length, 2);
  assert.notEqual(rt.calls[0]!.eventKey, rt.calls[1]!.eventKey);
});

test("a new message on a dismissed card brings it back with a fresh draft", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items);
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 2_000_000,
    text: "on it",
    senderEmail: LOOP.owner,
  });
  assert.equal((await items.byLoop(LOOP.id))[0]!.status, "skipped");
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 3_000_000,
    text: "one more thing",
    senderEmail: "mark@example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "ready");
  assert.equal(item!.proposal?.data.body, "redraft");
  assert.equal(item!.sourcePayload!.snippet, "one more thing");
});

test("events in unrelated conversations or other sources leave the ledger alone", async () => {
  const items = createLoopItemLedger();
  await items.ingest([
    slackEntry({
      dedupeKey: "C9:100.1",
      sourcePayload: {
        source: "slack",
        title: "#eng",
        from: "Ada",
        snippet: "thread ask",
        slack: { channelId: "C9", ts: "100.1", threadTs: "100.1" },
      },
    }),
    gmailEntry(),
  ]);
  const rt = realtime(items);

  await rt.onConversationEvent({ source: "slack", conversationRef: "C9:200.2", at: 2_000_000, text: "chatter" });
  assert.equal(rt.calls.length, 0);
  await rt.onConversationEvent({ source: "slack", conversationRef: "D999", at: 2_000_000, text: "other dm" });

  await rt.onConversationEvent({ source: "slack", conversationRef: "t-1", at: 2_000_000, text: "collide" });
  for (const item of await items.byLoop(LOOP.id)) {
    assert.equal(item.status, "queued");
    assert.equal(item.sourceAt, 1_000_100);
  }
  assert.equal(rt.calls.length, 0);
});

test("a thread reply resolves a channel card anchored at the thread root", async () => {
  const items = createLoopItemLedger();
  await items.ingest([
    slackEntry({
      dedupeKey: "C9:100.1",
      sourcePayload: {
        source: "slack",
        title: "#eng",
        from: "Ada",
        snippet: "thread ask",
        slack: { channelId: "C9", ts: "100.1" },
      },
    }),
  ]);
  const rt = realtime(items);
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "C9:100.1",
    at: 2_000_000,
    text: "done",
    senderEmail: "josh@example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "skipped");
  assert.equal(item!.actionKind, "replied");
});

test("a gmail event matches cards by threadId only", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry()]);
  const rt = realtime(items);
  await rt.onConversationEvent({
    source: "gmail",
    conversationRef: "t-other",
    at: 2_000_000,
    senderEmail: "josh@example.com",
  });
  let [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  await rt.onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 2_000_000,
    text: "sent, thanks",
    senderEmail: "josh@example.com",
  });
  [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "skipped");
  assert.equal(item!.actionResult, "sent, thanks");
});

test("someone else's gmail reply bumps the card for a redraft", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry()]);
  const rt = realtime(items, { work: false });
  await rt.onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 3_000_000,
    text: "bumping this — any word?",
    senderEmail: "ada@example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  assert.equal(item!.sourceAt, 3_000_000);
  assert.equal(item!.sourceSummary, "bumping this — any word?");
  assert.deepEqual(
    rt.calls.map((call) => call.loop.id),
    [LOOP.id],
  );
});

test("slack conversation refs key DMs by channel and channel asks by thread root", () => {
  assert.equal(slackConversationRef("D123", "100.1"), "D123");
  assert.equal(slackConversationRef("G77", "100.1", "100.1"), "G77:100.1");
  assert.equal(slackConversationRef("C77", "100.1", undefined, true), "C77");
  assert.equal(slackConversationRef("C9", "200.2"), "C9:200.2");
  assert.equal(slackConversationRef("C9", "200.2", "100.1"), "C9:100.1");
});

test("ledger mutations emit change events", async () => {
  const events: LedgerEvent[] = [];
  const items = createLoopItemLedger(undefined, (e) => events.push(e));
  await items.ingest([slackEntry()]);
  const [item] = await items.byLoop(LOOP.id);
  await items.setProposal(item!.id, { data: { body: "draft" }, by: "agent" });
  await items.recordAction(item!.id, { kind: "replied", outcome: "dismissed" });
  assert.deepEqual(
    events.map((e) => e.op),
    ["ingest", "proposal", "action"],
  );
  assert.ok(events.every((e) => e.loopId === LOOP.id && e.itemId === item!.id));
});

test("private-channel mentions stay anchored to their thread, not the channel tail", () => {
  assert.equal(slackConversationRef("GPRIVATE", "200.2", "100.1"), "GPRIVATE:100.1");
});

test("an old own reply cannot close a newer unanswered message", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry({ sourceAt: 3_000_000 })]);
  await realtime(items).onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 2_000_000,
    senderEmail: LOOP.owner,
  });
  assert.notEqual((await items.byLoop(LOOP.id))[0]!.status, "skipped");
});

test("reply closure advances the source watermark so an older scan cannot resurrect it", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry()]);
  await realtime(items).onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 3_000_000,
    senderEmail: LOOP.owner,
  });
  await items.ingest([gmailEntry({ sourceAt: 2_000_000 })]);
  assert.equal((await items.byLoop(LOOP.id))[0]!.status, "skipped");
});

test("replying in an unrelated DM thread cannot dismiss an unthreaded ask", async () => {
  const items = createLoopItemLedger();
  await items.ingest([
    slackEntry({ sourcePayload: { slack: { channelId: "D123", ts: "2.0", isDirectMessage: true } } }),
  ]);
  await realtime(items).onConversationEvent({
    source: "slack",
    conversationRef: "D123:1.0",
    at: 3_000_000,
    senderEmail: LOOP.owner,
  });
  assert.notEqual((await items.byLoop(LOOP.id))[0]!.status, "skipped");
});

test("a new inbound message keeps its full text on the card", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry()]);
  const rt = realtime(items);
  const text = `Thanks for the demo. ${"Here is everything we tried last week. ".repeat(12)}The one feature that I needed was model routing.`;
  await rt.onConversationEvent({ source: "gmail", conversationRef: "t-1", at: 2_000_000, text });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.sourcePayload!.snippet, text);
  assert.ok(item!.sourceSummary!.length <= 200);
});

test("the inbox sync task asks for the full waiting message", () => {
  const task = renderSourceInboxTask("loop-inbox", "gmail");
  assert.match(task, /snippet \(the full text of the waiting message/);
  assert.doesNotMatch(task, /snippet \(the waiting message, <=200 chars\)/);
});

test("an event seen through one person's Slack only touches that person's Loops", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items, { work: false });
  await rt.onConversationEvent(
    { source: "slack", conversationRef: "D123", at: 2_000_000, text: "hi" },
    { owners: ["someone@example.com"] },
  );
  assert.equal(rt.calls.length, 0);
  await rt.onConversationEvent(
    { source: "slack", conversationRef: "D123", at: 2_000_000, text: "hi" },
    { owners: ["JOSH@example.com"] },
  );
  assert.equal(rt.calls.length, 1);
});

test("the inbox sync task reports reconciled replies as Loop events instead of closing items", () => {
  const task = renderSourceInboxTask("loop-inbox", "slack");
  assert.match(task, /\/v1\/loops\/loop-inbox\/events/);
  assert.match(task, /Never resolve, dismiss, or redraft these items yourself/);
  assert.match(task, /Only if the events endpoint answers 403 or 404/);
});

test("events never touch an inbox Loop whose owner has the inbox_loops flag off", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const events = simulatedLoopEvents(items);
  const asked: string[] = [];
  const rt = createInboxRealtime({
    loops: { list: async () => [LOOP] },
    items,
    enqueue: events.enqueue,
    enabledFor: async (owner) => {
      asked.push(owner);
      return false;
    },
  });
  await rt.onConversationEvent({ source: "slack", conversationRef: "D123", at: 2_000_000, text: "ping" });
  assert.deepEqual(asked, [LOOP.owner]);
  assert.equal(events.calls.length, 0);
  assert.equal((await items.byLoop(LOOP.id))[0]!.status, "queued");
});
