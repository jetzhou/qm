import type { LoopItem } from "../types.ts";
import type { LoopStore } from "./loop-store.ts";
import type { LoopItemLedger } from "./item-ledger.ts";
import type { IngressEntry, LatestEvent } from "./ingress.ts";
import type { ConversationEvent } from "./sources/adapter.ts";
import { sourceAdapter } from "./sources/index.ts";
import { hashId } from "../util/crypto.ts";
import { swallow } from "../util/errors.ts";

export interface InboxRealtimeDeps {
  loops: Pick<LoopStore, "list">;
  items: Pick<LoopItemLedger, "byLoop">;
  /** The Loop's event queue. Every event is judged by the Loop's work stage. */
  enqueue: (loop: { id: string; owner: string }, eventKey: string, entries: IngressEntry[]) => Promise<unknown>;
  /** The inbox_loops flag: events never touch a Loop whose owner has it off. */
  enabledFor?: (owner: string) => Promise<boolean>;
}

const LOOP_CACHE_TTL_MS = 60_000;
const MAX_TEXT_CHARS = 2_000;
const MAX_SUMMARY_CHARS = 200;
const MAX_SNIPPET_CHARS = 10_000;

/**
 * Turn a message QM saw in a conversation already on an inbox ledger into a Loop
 * event. Both the owner's own replies and other people's follow-ups are queued,
 * whatever the item's status, so a dismissed card can come back and an open card
 * can settle. The decision belongs to the Loop's LLM work stage, not to this hook.
 */
export function createInboxRealtime(deps: InboxRealtimeDeps): {
  /** `owners` limits the event to those people's Loops (events seen through one person's Slack). */
  onConversationEvent(event: ConversationEvent, opts?: { owners?: string[] }): Promise<void>;
} {
  let cachedInboxLoops: Array<{ id: string; owner: string }> = [];
  let cacheAt = 0;

  async function inboxLoops(): Promise<Array<{ id: string; owner: string }>> {
    const now = Date.now();
    if (now - cacheAt > LOOP_CACHE_TTL_MS) {
      const all = await deps.loops.list();
      cachedInboxLoops = all
        .filter((loop) => (loop.surface === "inbox" || loop.surface?.startsWith("inbox:")) && loop.state !== "archived")
        .map((loop) => ({ id: loop.id, owner: loop.owner }));
      cacheAt = now;
    }
    return cachedInboxLoops;
  }

  return {
    async onConversationEvent(event, opts) {
      if (!Number.isFinite(event.at)) return;
      const adapter = sourceAdapter(event.source);
      if (!adapter) return;
      const owners = opts?.owners?.map((owner) => owner.toLowerCase());
      for (const loop of await inboxLoops()) {
        if (owners && !owners.includes(loop.owner.toLowerCase())) continue;
        if (deps.enabledFor && !(await deps.enabledFor(loop.owner).catch(() => false))) continue;
        let matched: LoopItem[];
        try {
          matched = (await deps.items.byLoop(loop.id)).filter(
            (item) =>
              (item.source ?? item.sourcePayload?.source) === event.source &&
              adapter.matchesEvent(item, event.conversationRef),
          );
        } catch (e) {
          swallow("inbox realtime: read ledger", e);
          continue;
        }
        if (!matched.length) continue;
        const owner = Boolean(event.senderEmail && event.senderEmail.toLowerCase() === loop.owner.toLowerCase());
        const text = event.text?.trim();
        const latestEvent: LatestEvent = {
          author: owner ? "owner" : "other",
          at: event.at,
          ...(text ? { text: text.slice(0, MAX_TEXT_CHARS) } : {}),
          ...(!owner && event.senderEmail ? { from: event.senderEmail } : {}),
        };
        const entries: IngressEntry[] = matched.map((item) => ({
          loopId: loop.id,
          dedupeKey: item.sourceKey,
          ...(item.source !== undefined ? { source: item.source } : {}),
          ...(!owner && text ? { summary: text.slice(0, MAX_SUMMARY_CHARS) } : {}),
          sourceAt: event.at,
          merge: true,
          sourcePayload: {
            latestEvent,
            ...(!owner ? { receivedAt: event.at, ...(text ? { snippet: text.slice(0, MAX_SNIPPET_CHARS) } : {}) } : {}),
          },
        }));
        try {
          await deps.enqueue(loop, hashId([event.source, event.conversationRef, String(event.at)]), entries);
        } catch (e) {
          swallow("inbox realtime: enqueue event", e);
        }
      }
    },
  };
}
