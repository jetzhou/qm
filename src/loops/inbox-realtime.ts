import type { LoopItem } from "../types.ts";
import type { LoopStore } from "./loop-store.ts";
import type { LoopItemLedger } from "./item-ledger.ts";
import { conversationEvent, type IngressEntry } from "./ingress.ts";
import type { ConversationEvent } from "./sources/adapter.ts";
import { sourceAdapter } from "./sources/index.ts";

export interface InboxRealtimeDeps {
  loops: Pick<LoopStore, "list">;
  items: Pick<LoopItemLedger, "byLoop">;
  enqueue: (loop: { id: string; owner: string }, entries: IngressEntry[]) => Promise<unknown>;
}

const CACHE_TTL_MS = 60_000;

export function createInboxRealtime(deps: InboxRealtimeDeps): {
  onConversationEvent(event: ConversationEvent, opts?: { owners?: string[] }): Promise<void>;
} {
  let cachedInboxLoops: Array<{ id: string; owner: string }> = [];
  let cacheAt = 0;
  const ledgers = new Map<string, { at: number; items: Promise<LoopItem[]> }>();

  async function inboxLoops(): Promise<Array<{ id: string; owner: string }>> {
    const now = Date.now();
    if (now - cacheAt > CACHE_TTL_MS) {
      const all = await deps.loops.list();
      cachedInboxLoops = all
        .filter((loop) => (loop.surface === "inbox" || loop.surface?.startsWith("inbox:")) && loop.state !== "archived")
        .map((loop) => ({ id: loop.id, owner: loop.owner }));
      cacheAt = now;
    }
    return cachedInboxLoops;
  }

  function conversations(loopId: string): Promise<LoopItem[]> {
    const cached = ledgers.get(loopId);
    if (cached && Date.now() - cached.at <= CACHE_TTL_MS) return cached.items;
    const items = deps.items.byLoop(loopId).then((all) =>
      all.map((item) => ({
        ...item,
        sourcePayload: {
          source: item.sourcePayload?.source,
          slack: item.sourcePayload?.slack,
          gmail: item.sourcePayload?.gmail,
        },
      })),
    );
    ledgers.set(loopId, { at: Date.now(), items });
    items.catch(() => ledgers.delete(loopId));
    return items;
  }

  return {
    async onConversationEvent(event, opts) {
      if (!Number.isFinite(event.at)) return;
      const adapter = sourceAdapter(event.source);
      if (!adapter) return;
      const owners = opts?.owners?.map((owner) => owner.toLowerCase());
      for (const loop of await inboxLoops()) {
        if (owners && !owners.includes(loop.owner.toLowerCase())) continue;
        const matched = (await conversations(loop.id)).filter(
          (item) =>
            (item.source ?? item.sourcePayload?.source) === event.source &&
            adapter.matchesEvent(item, event.conversationRef),
        );
        if (!matched.length) continue;
        const owner = Boolean(event.senderEmail && event.senderEmail.toLowerCase() === loop.owner.toLowerCase());
        const latest = {
          author: owner ? ("owner" as const) : ("other" as const),
          at: event.at,
          ...(event.text ? { text: event.text } : {}),
          ...(event.senderEmail ? { from: event.senderEmail } : {}),
        };
        await deps.enqueue(
          loop,
          matched.map((item) => conversationEvent(item, latest)),
        );
      }
    },
  };
}
