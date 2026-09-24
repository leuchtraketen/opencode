import type { OpenCodeClient, SessionInboxUser } from "@opencode/client"
import type { PromptMention } from "@opencode/schema"
import { projectedPromptInput } from "./codec"
import { promptOffsetWidth } from "./display"
import { emptyPrompt, type PromptInfo } from "./history"
import { realignPromptMentions } from "./mention"

export type QueuedPrompt = Pick<SessionInboxUser, "payload">

// Combine withdrawn prompts into one draft in enqueue order, separated by blank lines.
// Mentions are located by their text instead of trusting the stored offsets, since
// the wire text may differ from the editor text they were measured against.
export function combineQueuedPrompts(items: readonly QueuedPrompt[]): PromptInfo {
  const result: PromptInfo = { ...emptyPrompt(), mode: "normal" }
  const files: NonNullable<PromptInfo["files"]> = []
  const agents: NonNullable<PromptInfo["agents"]> = []
  const skills: NonNullable<PromptInfo["skills"]> = []

  for (const [index, item] of items.entries()) {
    if (index > 0) result.text += "\n\n"
    const offset = promptOffsetWidth(result.text)
    const input = projectedPromptInput(item.payload)
    const itemFiles = input.files ?? []
    const itemAgents = input.agents ?? []
    const itemSkills = input.skills ?? []
    let text = input.text
    const aligned = realignPromptMentions(
      text,
      [...itemFiles, ...itemAgents, ...itemSkills].map((part) => part.mention),
    )
    const shift = (mention: PromptMention): PromptMention => ({
      ...mention,
      start: mention.start + offset,
      end: mention.end + offset,
    })
    // Agents and skills only survive editing through an extmark, so give one to
    // every mention that cannot be located. Mentionless files are kept as-is.
    const marker = (value: string): PromptMention => {
      if (text && !/\s$/.test(text)) text += " "
      const start = offset + promptOffsetWidth(text)
      text += value
      return { start, end: start + promptOffsetWidth(value), text: value }
    }

    files.push(
      ...itemFiles.map((file, position) => {
        const mention = aligned[position]
        return { ...file, mention: mention?.text ? shift(mention) : undefined }
      }),
    )
    agents.push(
      ...itemAgents.map((agent, position) => {
        const mention = aligned[itemFiles.length + position]
        return { ...agent, mention: mention?.text ? shift(mention) : marker(`@${agent.name}`) }
      }),
    )
    skills.push(
      ...itemSkills.map((skill, position) => {
        const mention = aligned[itemFiles.length + itemAgents.length + position]
        return { ...skill, mention: mention?.text ? shift(mention) : marker(`@${skill.id}`) }
      }),
    )
    result.text += text
  }

  result.files = files
  result.agents = agents
  result.skills = skills
  return result
}

// Withdrawal pads the draft with a trailing blank line so the cursor has somewhere
// to type. That padding is an editing affordance, not content, so drop it before the
// prompt goes on the wire. Never cut into a mention or paste placeholder: a range
// that reaches the end of the text means the trailing whitespace is part of it.
export function trimPromptTail(prompt: PromptInfo): PromptInfo {
  const trimmed = prompt.text.replace(/\s+$/, "")
  if (trimmed === prompt.text) return prompt

  const width = promptOffsetWidth(trimmed)
  const ranges = [
    ...(prompt.files ?? []).map((file) => file.mention),
    ...(prompt.agents ?? []).map((agent) => agent.mention),
    ...(prompt.skills ?? []).map((skill) => skill.mention),
    ...prompt.pasted.map((part) => part.source),
  ]
  if (ranges.some((range) => range && range.end > width)) return prompt

  return { ...prompt, text: trimmed }
}

export type WithdrawalTarget = {
  // Whether the prompt that requested the withdrawal is still empty and mounted.
  current(): boolean
  restore(prompt: PromptInfo): void
  save(prompt: PromptInfo): void
  empty(): void
}

export function createPromptQueue(client: { readonly api: OpenCodeClient }) {
  const admissions = new Map<string, Set<Promise<unknown>>>()
  const withdrawals = new Set<string>()
  const requests = new Map<string, string>()
  return {
    busy(sessionID: string) {
      return withdrawals.has(sessionID)
    },
    // Register an in-flight prompt admission so a withdrawal that follows it
    // includes the prompt once the server has accepted it.
    admit<Value>(sessionID: string, admitted: Promise<Value>) {
      const pending = admissions.get(sessionID) ?? new Set<Promise<unknown>>()
      admissions.set(sessionID, pending)
      pending.add(admitted)
      const settle = () => {
        pending.delete(admitted)
        if (pending.size === 0 && admissions.get(sessionID) === pending) admissions.delete(sessionID)
      }
      admitted.then(settle, settle)
      return admitted
    },
    async withdraw(sessionID: string, target: WithdrawalTarget) {
      if (withdrawals.has(sessionID)) return
      withdrawals.add(sessionID)
      try {
        const retry = requests.has(sessionID)
        // A rejected admission was rolled back, so only settlement matters.
        if (!retry) await Promise.allSettled(admissions.get(sessionID)?.values() ?? [])
        const requestID = requests.get(sessionID) ?? crypto.randomUUID()
        requests.set(sessionID, requestID)
        // Keep the ID on failure: the server replays the receipt for it, so the
        // retry cannot withdraw a newer batch while the first response is lost.
        const response = await client.api.session.inbox.withdraw({ sessionID, requestID })
        if (response.length === 0) {
          // A replayed empty receipt says nothing about prompts admitted since then.
          if (!retry && target.current()) target.empty()
          requests.delete(sessionID)
          return
        }
        const prompt = combineQueuedPrompts(response)
        if (target.current()) target.restore(prompt)
        else target.save(prompt)
        requests.delete(sessionID)
      } finally {
        withdrawals.delete(sessionID)
      }
    },
  }
}

export type PromptQueue = ReturnType<typeof createPromptQueue>

const queues = new WeakMap<object, PromptQueue>()

// One coordinator per client connection: retry identities and in-flight guards
// must outlive the prompt component, which remounts per session tab.
export function promptQueue(client: { readonly api: OpenCodeClient }) {
  const existing = queues.get(client)
  if (existing) return existing
  const queue = createPromptQueue(client)
  queues.set(client, queue)
  return queue
}
