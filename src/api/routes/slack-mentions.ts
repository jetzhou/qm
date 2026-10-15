import type { DirectoryStore } from "../../directory/directory-store.ts";
import { slackMentionName } from "../../slack/mrkdwn.ts";

const BARE_MENTION = /<@([UW][A-Z0-9]+)>/g;

export async function labelSlackMentions<T>(value: T, directory: DirectoryStore | undefined): Promise<T> {
  const json = JSON.stringify(value);
  const ids = [...new Set([...json.matchAll(BARE_MENTION)].map((m) => m[1]!))];
  if (!ids.length) return value;
  const names = new Map<string, string>();
  await Promise.all(
    ids.map(async (id) => {
      const hit = await directory?.resolve(id).catch(() => null);
      const name = hit?.kind === "one" && hit.member.slackId === id ? hit.member.displayName : slackMentionName(id);
      const clean = name?.replace(/[<>|]/g, "").trim();
      if (clean) names.set(id, clean);
    }),
  );
  if (!names.size) return value;
  return JSON.parse(
    json.replace(BARE_MENTION, (m, id: string) =>
      names.has(id) ? `<@${id}|${JSON.stringify(names.get(id)).slice(1, -1)}>` : m,
    ),
  ) as T;
}
