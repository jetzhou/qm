import { html, nothing, type TemplateResult } from "lit";
import type { DirectiveResult } from "lit/directive.js";
import { ref } from "lit/directives/ref.js";
import { Check, Copy } from "lucide";
import { markdown } from "./message-markdown";
import { icon } from "./icons";
import { formatMessageTime } from "./message-time.ts";
import { tip } from "./tooltip";
import { copyText } from "./ui";
import { suggestedActivities } from "./suggested-activities";

type Content = TemplateResult | DirectiveResult | typeof nothing;

export type AssistantSidebarContext =
  | { kind: "thread"; source: string; hasDraft: boolean; sent: boolean; resolved: boolean }
  | { kind: "inbox"; hasEmail: boolean; hasSlack: boolean; hasDrafts: boolean };

export function assistantPrompts(context: AssistantSidebarContext): readonly string[] {
  if (context.kind === "inbox") {
    const prompts = [context.hasEmail && !context.hasSlack ? "Summarize my recent emails" : "Summarize my inbox"];
    prompts.push(context.hasDrafts ? "Which drafts should I review first?" : "What needs my attention?");
    return prompts;
  }
  if (context.sent) return ["Summarize this email", "What should I follow up on?"];
  if (context.source === "generic") return ["Explain the proposal", "What needs my input?"];
  if (context.resolved) return ["Summarize this thread", "Does this still need a reply?"];
  if (context.hasDraft) return ["Make it shorter", "Make it more friendly", "Remove the salutations"];
  return ["Summarize this thread", "Help me write a reply"];
}

function promptIcon(prompt: string): string {
  if (/^(Summarize|Explain)/.test(prompt)) return "📝";
  if (/^(Make|Remove|Help me write)/.test(prompt)) return "✏️";
  return "👀";
}

export function assistantSuggestions(
  context: AssistantSidebarContext,
  onPrompt: (prompt: string, origin: HTMLElement | null) => void,
  busy = false,
): TemplateResult | typeof nothing {
  let origin: HTMLElement | null = null;
  return html`<div class="assistant-suggestions" ${ref((el) => (origin = (el as HTMLElement | undefined) ?? null))}>
    ${suggestedActivities(
      assistantPrompts(context).map((prompt) => ({ id: prompt, title: prompt, prompt, icon: promptIcon(prompt) })),
      (activity) => onPrompt(activity.prompt, origin),
      busy,
    )}
  </div>`;
}

function messageMeta(text: string, at?: number): TemplateResult {
  return html`<div class="message-meta">
    ${at !== undefined ? html`<span class="message-time">${formatMessageTime(at)}</span>` : nothing}
    <button
      class="msg-copy"
      type="button"
      ${tip("Copy")}
      aria-label="Copy message"
      @click=${(e: Event) => void copyText(text, e.currentTarget as HTMLButtonElement)}
    >
      ${icon(Copy, 13)}${icon(Check, 13)}
    </button>
  </div>`;
}

/** One transcript row, in the same markup the main chat renders (user bubble / assistant body). */
export function assistantMessage(options: {
  role: "human" | "agent" | "system";
  text: string;
  at?: number;
}): TemplateResult {
  const meta = options.text.trim() ? messageMeta(options.text, options.at) : nothing;
  if (options.role === "human")
    return html`<article class="message-row user-row">
      <div class="message-bubble user-bubble" ?hidden=${!options.text.trim()}>
        <div class="pin-content">${markdown(options.text)}</div>
      </div>
      ${meta}
    </article>`;
  return html`<article class="message-row assistant-row ${options.role === "system" ? "system-row" : ""}">
    <div class="assistant-body">${markdown(options.text)} ${meta}</div>
  </article>`;
}

/** The main chat shell in pane mode, fed by a ledger thread instead of a live session. */
export function assistantSidebar(options: {
  context: AssistantSidebarContext;
  messages: Content;
  composer: Content;
  showPrompts: boolean;
  busy?: boolean;
  status?: Content;
  cta?: string;
  onPrompt: (prompt: string, origin: HTMLElement | null) => void;
}): TemplateResult {
  const empty = options.showPrompts;
  return html`<div class="custom-chat inbox-chat">
    <div class="custom-chat-shell in-pane ${empty ? "empty-chat" : ""}">
      <section class="chat-scroll inbox-chat-log" tabindex="0" aria-label="Conversation">
        <div class="message-stack ${empty ? "empty-stack" : ""}">
          ${options.messages} ${options.status ?? nothing}
          ${empty && options.cta ? html`<h1 class="chat-cta">${options.cta}</h1>` : nothing}
          ${empty ? assistantSuggestions(options.context, options.onPrompt, options.busy) : nothing}
        </div>
      </section>
      <div class="chat-bottom-dock inbox-chat-composer">${options.composer}</div>
    </div>
  </div>`;
}
