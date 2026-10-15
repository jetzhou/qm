import { ledgerEntry, type IngressEntry } from "../../src/loops/ingress.ts";
import type { LoopItemLedger } from "../../src/loops/item-ledger.ts";

export interface EnqueuedEvent {
  loop: { id: string; owner: string };
  eventKey: string;
  entries: IngressEntry[];
}

/**
 * A stand-in for a Loop's event queue plus its LLM work stage. Entries are applied
 * the way the ingress worker applies them; the decision mimics the work stage
 * (the owner's latest message settles the item, anything else gets a redraft).
 */
export function simulatedLoopEvents(items: LoopItemLedger, opts: { work?: boolean } = {}) {
  const calls: EnqueuedEvent[] = [];
  const enqueue = async (loop: { id: string; owner: string }, eventKey: string, entries: IngressEntry[]) => {
    calls.push({ loop, eventKey, entries });
    for (const raw of entries) {
      const existing = (await items.byLoop(raw.loopId)).find((item) => item.sourceKey === raw.dedupeKey);
      const entry = ledgerEntry(raw, existing);
      if (entry) await items.ingest([entry]);
    }
    if (opts.work === false) return;
    for (const item of await items.queued(loop.id)) {
      const latest = item.sourcePayload?.latestEvent as { author?: string; text?: string } | undefined;
      if (latest?.author === "owner")
        await items.recordAction(item.id, { kind: "replied", outcome: "dismissed", result: latest.text ?? "" });
      else await items.setProposal(item.id, { data: { body: "redraft" }, by: "agent" });
    }
  };
  return { enqueue, calls };
}
