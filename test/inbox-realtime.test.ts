import assert from "node:assert/strict";
import { test } from "node:test";
import { createLoopItemLedger, type IngestEntryInput } from "../src/loops/item-ledger.ts";
import { createInboxRealtime } from "../src/loops/inbox-realtime.ts";
import { renderSourceInboxTask } from "../src/loops/inbox-loop.ts";
import { slackConversationRef } from "../src/loops/sources/slack.ts";
import type { LedgerEvent } from "../src/loops/ledger-events.ts";
import type { Loop } from "../src/types.ts";
import { ledgerEntry, type IngressEntry } from "../src/loops/ingress.ts";

const LOOP = { id: "loop-inbox", owner: "owner@example.com", surface: "inbox" } as unknown as Loop;

function slackEntry(over: Partial<IngestEntryInput> = {}): IngestEntryInput {
  return {
    loopId: LOOP.id,
    dedupeKey: "D123",
    source: "slack",
    summary: "any update?",
    sourcePayload: {
      source: "slack",
      title: "DM with Sam",
      from: "Sam",
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

function realtime(items: ReturnType<typeof createLoopItemLedger>) {
  const calls: Array<{ loop: { id: string; owner: string }; entries: IngressEntry[] }> = [];
  const rt = createInboxRealtime({
    loops: { list: async () => [LOOP] },
    items,
    enqueue: async (loop, entries) => {
      calls.push({ loop, entries });
      for (const raw of entries) {
        const entry = ledgerEntry(
          raw,
          (await items.byLoop(raw.loopId)).find((item) => item.sourceKey === raw.dedupeKey),
        );
        if (entry) await items.ingest([entry]);
      }
    },
  });
  return Object.assign(rt, { calls });
}

test("the owner's own reply is queued as an owner event and keeps the card's ask", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items);
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 2_000_000,
    text: "on it!",
    senderEmail: "Owner@Example.com",
  });
  assert.equal(rt.calls.length, 1);
  assert.deepEqual(rt.calls[0]!.entries[0]!.sourcePayload, {
    latestEvent: { author: "owner", at: 2_000_000, text: "on it!" },
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  assert.equal(item!.sourceAt, 2_000_000);
  assert.equal(item!.sourcePayload!.snippet, "any update?");
  assert.equal(item!.sourceSummary, "any update?");
  assert.deepEqual(item!.sourcePayload!.slack, { channelId: "D123", ts: "1000.100" });
});

test("someone else's messages become other events that bump the card", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items);
  for (const [at, text] of [
    [3_000_000, "also, when works?"],
    [3_001_000, "ping"],
  ] as const)
    await rt.onConversationEvent({
      source: "slack",
      conversationRef: "D123",
      at,
      text,
      senderEmail: "sam@example.com",
    });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  assert.equal(item!.sourceAt, 3_001_000);
  assert.equal(item!.sourcePayload!.receivedAt, 3_001_000);
  assert.deepEqual(item!.sourcePayload!.latestEvent, {
    author: "other",
    at: 3_001_000,
    text: "ping",
    from: "sam@example.com",
  });
  assert.equal(rt.calls.length, 2);
});

test("a new message on a dismissed card reopens it for the work stage", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const [before] = await items.byLoop(LOOP.id);
  await items.recordAction(before!.id, { kind: "dismiss", outcome: "dismissed" });
  await realtime(items).onConversationEvent({
    source: "slack",
    conversationRef: "D123",
    at: 3_000_000,
    text: "one more thing",
    senderEmail: "sam@example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  assert.equal(item!.actionKind, undefined);
  assert.equal(item!.sourceAt, 3_000_000);
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
  await rt.onConversationEvent({ source: "slack", conversationRef: "D999", at: 2_000_000, text: "other dm" });
  await rt.onConversationEvent({ source: "slack", conversationRef: "t-1", at: 2_000_000, text: "collide" });
  await rt.onConversationEvent({ source: "gmail", conversationRef: "t-other", at: 2_000_000, text: "other mail" });
  assert.equal(rt.calls.length, 0);
  for (const item of await items.byLoop(LOOP.id)) assert.equal(item.sourceAt, 1_000_100);
});

test("a thread reply reaches a channel card anchored at the thread root", async () => {
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
    senderEmail: LOOP.owner,
  });
  assert.deepEqual(
    rt.calls[0]!.entries.map((entry) => [
      entry.dedupeKey,
      (entry.sourcePayload!.latestEvent as { author: string }).author,
    ]),
    [["C9:100.1", "owner"]],
  );
});

test("someone else's gmail reply bumps the card for a redraft", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry()]);
  await realtime(items).onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 3_000_000,
    text: "bumping this — any word?",
    senderEmail: "ada@example.com",
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.status, "queued");
  assert.equal(item!.sourceAt, 3_000_000);
  assert.equal((item!.sourcePayload!.latestEvent as { text: string }).text, "bumping this — any word?");
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

test("an old own reply cannot refresh a newer unanswered message", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry({ sourceAt: 3_000_000 })]);
  await realtime(items).onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 2_000_000,
    senderEmail: LOOP.owner,
  });
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.sourceAt, 3_000_000);
  assert.equal(item!.sourcePayload!.latestEvent, undefined);
});

test("an owner event advances the source watermark so an older scan cannot replace it", async () => {
  const items = createLoopItemLedger();
  await items.ingest([gmailEntry()]);
  await realtime(items).onConversationEvent({
    source: "gmail",
    conversationRef: "t-1",
    at: 3_000_000,
    senderEmail: LOOP.owner,
  });
  await items.ingest([gmailEntry({ sourceAt: 2_000_000 })]);
  const [item] = await items.byLoop(LOOP.id);
  assert.equal(item!.sourceAt, 3_000_000);
  assert.equal((item!.sourcePayload!.latestEvent as { author: string }).author, "owner");
});

test("replying in an unrelated DM thread does not reach an unthreaded ask", async () => {
  const items = createLoopItemLedger();
  await items.ingest([
    slackEntry({ sourcePayload: { slack: { channelId: "D123", ts: "2.0", isDirectMessage: true } } }),
  ]);
  const rt = realtime(items);
  await rt.onConversationEvent({
    source: "slack",
    conversationRef: "D123:1.0",
    at: 3_000_000,
    senderEmail: LOOP.owner,
  });
  assert.equal(rt.calls.length, 0);
});

test("the inbox sync task asks for the full waiting message", () => {
  const task = renderSourceInboxTask("loop-inbox", "gmail");
  assert.match(task, /snippet \(the full text of the waiting message/);
  assert.doesNotMatch(task, /snippet \(the waiting message, <=200 chars\)/);
});

test("an event seen through one person's Slack only touches that person's Loops", async () => {
  const items = createLoopItemLedger();
  await items.ingest([slackEntry()]);
  const rt = realtime(items);
  await rt.onConversationEvent(
    { source: "slack", conversationRef: "D123", at: 2_000_000, text: "hi" },
    { owners: ["someone@example.com"] },
  );
  assert.equal(rt.calls.length, 0);
  await rt.onConversationEvent(
    { source: "slack", conversationRef: "D123", at: 2_000_000, text: "hi" },
    { owners: ["OWNER@example.com"] },
  );
  assert.equal(rt.calls.length, 1);
});
