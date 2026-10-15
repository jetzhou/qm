import { parseScopeId, scopeId } from "../../types.ts";
import { sendJson, readRawBody, PayloadTooLargeError } from "../http.ts";
import { loadAdministrable } from "./loops.ts";
import { isObj } from "./shared.ts";
import type { ApiCtx, BaseCtx, Route } from "./route.ts";
import type { LoopIngress, IngressSetup, IngressEntry } from "../../loops/ingress.ts";
import { errMessage } from "../../util/errors.ts";
import { hashId } from "../../util/crypto.ts";

const MAX_EVENTS = 20;
const MAX_EVENT_TEXT = 2_000;

function view(source: Omit<LoopIngress, "secret">, publicUrl?: string) {
  const { secret: _secret, ...safe } = source as LoopIngress;
  const path = `/v1/loop-ingress/${source.kind === "gmail" ? "gmail" : source.id}`;
  return { ...safe, url: publicUrl ? new URL(path, publicUrl).href : path };
}

async function list(ctx: ApiCtx) {
  const loaded = await loadAdministrable(ctx);
  if (!loaded) return;
  if (!(await ctx.deps.featureFlags?.enabled("inbox_loops", scopeId("personal", loaded.acting.actorId))))
    return sendJson(ctx.res, 403, { error: "feature_disabled" });
  const ingress = ctx.deps.loopIngress;
  if (!ingress) return sendJson(ctx.res, 404, { error: "unavailable" });
  return sendJson(ctx.res, 200, {
    sources: (await ingress.list(loaded.loop.id)).map((source) => view(source, ctx.deps.publicUrl)),
    gmailAvailable: ingress.gmailAvailable,
    ...(ingress.gmailMode ? { gmailMode: ingress.gmailMode } : {}),
  });
}

async function change(ctx: ApiCtx) {
  const loaded = await loadAdministrable(ctx);
  if (!loaded) return;
  if (!(await ctx.deps.featureFlags?.enabled("inbox_loops", scopeId("personal", loaded.acting.actorId))))
    return sendJson(ctx.res, 403, { error: "feature_disabled" });
  const ingress = ctx.deps.loopIngress;
  if (!ingress) return sendJson(ctx.res, 404, { error: "unavailable" });
  if (!loaded.acting.liveHuman)
    return sendJson(ctx.res, 403, {
      error: "human_required",
      message: "Configuring event ingestion requires a live human actor",
    });
  if (loaded.loop.state === "archived") return sendJson(ctx.res, 409, { error: "archived" });
  if (!isObj(ctx.body)) return sendJson(ctx.res, 400, { error: "bad_request" });
  try {
    if (ctx.params.sourceId) {
      if (typeof ctx.body.enabled !== "boolean") return sendJson(ctx.res, 400, { error: "bad_request" });
      await ingress.setEnabled(loaded.loop.id, ctx.params.sourceId, ctx.body.enabled);
      return sendJson(ctx.res, 200, { ok: true });
    }
    if (
      !["webhook", "slack", "gmail"].includes(String(ctx.body.kind)) ||
      (ctx.body.secret !== undefined && typeof ctx.body.secret !== "string") ||
      (ctx.body.teamId !== undefined && typeof ctx.body.teamId !== "string") ||
      (ctx.body.channels !== undefined &&
        (!Array.isArray(ctx.body.channels) ||
          ctx.body.channels.length > 100 ||
          ctx.body.channels.some((channel) => typeof channel !== "string")))
    )
      return sendJson(ctx.res, 400, { error: "bad_request", message: "Invalid ingestion settings" });
    const source = await ingress.create(loaded.loop, ctx.body as unknown as IngressSetup);
    return sendJson(ctx.res, 201, {
      source: view(source, ctx.deps.publicUrl),
      ...(source.kind === "webhook" ? { secret: source.secret } : {}),
    });
  } catch (error) {
    return sendJson(ctx.res, 400, { error: "setup_failed", message: errMessage(error) });
  }
}

/**
 * Report new messages in conversations already on the Loop ledger (for example a
 * reply the scheduled sync noticed). Events take the same path as Gmail and Slack
 * events: queued, merged into the item, then judged by the Loop's work stage.
 */
async function events(ctx: ApiCtx) {
  const loaded = await loadAdministrable(ctx);
  if (!loaded) return;
  if (!(await ctx.deps.featureFlags?.enabled("inbox_loops", scopeId("personal", loaded.loop.owner))))
    return sendJson(ctx.res, 403, { error: "feature_disabled" });
  const ingress = ctx.deps.loopIngress;
  if (!ingress) return sendJson(ctx.res, 404, { error: "unavailable" });
  if (loaded.loop.state === "archived") return sendJson(ctx.res, 409, { error: "archived" });
  // An agent may report events only from the owner's own scope, like ledger reads.
  if (
    ctx.capability &&
    parseScopeId(loaded.loop.ownerScopeId).kind === "personal" &&
    ctx.capability.privateScope !== true
  )
    return sendJson(ctx.res, 403, {
      error: "forbidden",
      message: "personal loop events come only from its owner's scope",
    });
  const raw = isObj(ctx.body) ? ctx.body.events : undefined;
  if (!Array.isArray(raw) || !raw.length || raw.length > MAX_EVENTS)
    return sendJson(ctx.res, 400, { error: "bad_request", message: `events must be 1-${MAX_EVENTS} items` });
  const entries: IngressEntry[] = [];
  for (const event of raw) {
    if (
      !isObj(event) ||
      typeof event.sourceKey !== "string" ||
      !event.sourceKey ||
      (event.author !== "owner" && event.author !== "other") ||
      typeof event.at !== "number" ||
      !Number.isFinite(event.at) ||
      event.at <= 0 ||
      (event.text !== undefined && typeof event.text !== "string") ||
      (event.from !== undefined && typeof event.from !== "string")
    )
      return sendJson(ctx.res, 400, {
        error: "bad_request",
        message: 'each event needs {sourceKey, author: "owner"|"other", at (ms), text?, from?}',
      });
    const text = typeof event.text === "string" ? event.text.trim().slice(0, MAX_EVENT_TEXT) : undefined;
    const other = event.author === "other";
    entries.push({
      loopId: loaded.loop.id,
      dedupeKey: event.sourceKey,
      sourceAt: event.at,
      merge: true,
      ...(other && text ? { summary: text.slice(0, 200) } : {}),
      sourcePayload: {
        latestEvent: {
          author: event.author,
          at: event.at,
          ...(text ? { text } : {}),
          ...(other && typeof event.from === "string" ? { from: event.from.slice(0, 300) } : {}),
        },
        ...(other ? { receivedAt: event.at, ...(text ? { snippet: text } : {}) } : {}),
      },
    });
  }
  const key = hashId(["api", ...entries.map((entry) => `${entry.dedupeKey}:${entry.sourceAt}`)]);
  const queued = await ingress.enqueueConversation(loaded.loop, key, entries);
  return sendJson(
    ctx.res,
    queued ? 202 : 409,
    queued ? { ok: true, queued: entries.length } : { error: "not_accepting" },
  );
}

async function receive(ctx: BaseCtx) {
  if (!ctx.deps.loopIngress) {
    ctx.req.resume();
    return sendJson(ctx.res, 404, { error: "not_found" });
  }
  try {
    const rawBody = await readRawBody(ctx.req);
    const result = await ctx.deps.loopIngress.receive(ctx.params.id!, { headers: ctx.req.headers, rawBody });
    ctx.res.writeHead(result.status, { "content-type": "text/plain", "cache-control": "no-store" });
    ctx.res.end(result.body);
  } catch (error) {
    if (error instanceof PayloadTooLargeError) return sendJson(ctx.res, 413, { error: "payload_too_large" });
    throw error;
  }
}

export const loopIngressRoutes: ReadonlyArray<Route<ApiCtx>> = [
  { method: "GET", path: "/v1/loops/:id/ingestion", auth: "source", handle: list },
  { method: "POST", path: "/v1/loops/:id/ingestion", auth: "source", handle: change },
  { method: "PATCH", path: "/v1/loops/:id/ingestion/:sourceId", auth: "source", handle: change },
  { method: "POST", path: "/v1/loops/:id/events", auth: "either", handle: events },
];

/** Slack Events API for the connector app (user-token events), see slack-user-events.ts. */
async function slackUserEvents(ctx: BaseCtx) {
  if (!ctx.deps.slackUserEvents) {
    ctx.req.resume();
    return sendJson(ctx.res, 404, { error: "not_found" });
  }
  try {
    const rawBody = await readRawBody(ctx.req);
    const result = await ctx.deps.slackUserEvents.receive({ headers: ctx.req.headers, rawBody });
    ctx.res.writeHead(result.status, { "content-type": "text/plain", "cache-control": "no-store" });
    ctx.res.end(result.body);
  } catch (error) {
    if (error instanceof PayloadTooLargeError) return sendJson(ctx.res, 413, { error: "payload_too_large" });
    throw error;
  }
}

export const loopIngressRawRoutes: ReadonlyArray<Route<BaseCtx>> = [
  { method: "POST", path: "/v1/slack/user-events", auth: "public", handle: slackUserEvents },
  { method: "POST", path: "/v1/loop-ingress/:id", auth: "public", handle: receive },
];
