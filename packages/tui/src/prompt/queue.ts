import type { OpencodeClient, SessionPromptPromptInput } from "@opencode-ai/sdk/v2"
import { displaySlice, promptOffsetWidth } from "./display"
import type { PromptInfo } from "./history"
import { expandTrackedPastedText } from "./part"

// The wire prompt contains expanded paste text, so attachment offsets must refer
// to that text too, not to the shorter placeholders in the editor.
export function promptParts(prompt: PromptInfo): SessionPromptPromptInput["parts"] {
  const pastes = prompt.parts.flatMap((part) =>
    part.type === "text" && part.source ? [{ ...part.source.text, text: part.text }] : [],
  )
  return [
    { type: "text", text: expandTrackedPastedText(prompt.input, pastes) },
    ...prompt.parts.flatMap((part) => {
      if (part.type === "text" && part.source) return []
      const copy = structuredClone(part)
      const source = copy.type === "agent" ? copy.source : copy.type === "file" ? copy.source?.text : undefined
      if (source) {
        const shift = pastes
          .filter((paste) => paste.end <= source.start)
          .reduce((sum, paste) => sum + promptOffsetWidth(paste.text) - (paste.end - paste.start), 0)
        source.start += shift
        source.end += shift
      }
      return [copy]
    }),
  ]
}

export function combineQueuedPrompts(prompts: SessionPromptPromptInput[]): PromptInfo {
  const result: PromptInfo = { input: "", parts: [], mode: "normal" }
  for (const [index, prompt] of prompts.entries()) {
    if (index > 0) result.input += "\n\n"
    const offset = promptOffsetWidth(result.input)
    let text = prompt.parts
      .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
      .join("\n\n")
    for (const part of prompt.parts) {
      if (part.type === "subtask") continue // Subtasks are not eligible for withdrawal.
      if (part.type === "text" && !part.synthetic && !part.ignored) continue
      const { id: _id, ...copy } = structuredClone(part)
      if (copy.type === "text") {
        result.parts.push(copy)
        continue
      }
      const source = copy.type === "agent" ? copy.source : copy.source?.text
      if (source && displaySlice(text, source.start, source.end) === source.value && source.value) {
        source.start += offset
        source.end += offset
      } else {
        // API callers can attach files without a visible marker. Give those
        // attachments an extmark so editing the draft cannot silently drop them.
        const value = copy.type === "agent" ? `@${copy.name}` : `[${copy.filename || copy.mime}]`
        if (text && !text.endsWith("\n") && !text.endsWith(" ")) text += " "
        const start = offset + promptOffsetWidth(text)
        const marker = { value, start, end: start + promptOffsetWidth(value) }
        text += value
        if (copy.type === "agent") copy.source = marker
        else
          copy.source = copy.source
            ? { ...copy.source, text: marker }
            : { type: "file", path: copy.filename ?? "", text: marker }
      }
      result.parts.push(copy)
    }
    result.input += text
  }
  return result
}

export function createPromptQueue(client: OpencodeClient) {
  const admissions = new Map<string, Set<Promise<unknown>>>()
  const withdrawals = new Set<string>()
  const requests = new Map<string, string>()
  return {
    busy(sessionID: string) {
      return withdrawals.has(sessionID)
    },
    submit(input: SessionPromptPromptInput) {
      if (withdrawals.has(input.sessionID)) return Promise.reject(new Error("Queued prompts are being withdrawn"))
      const pending = admissions.get(input.sessionID) ?? new Set<Promise<unknown>>()
      admissions.set(input.sessionID, pending)
      // promptAsync acknowledges admission, not completion of the active turn.
      const admitted = client.session.promptAsync(input, { throwOnError: true })
      pending.add(admitted)
      return admitted.finally(() => {
        pending.delete(admitted)
        if (pending.size === 0) admissions.delete(input.sessionID)
      })
    },
    async withdraw(
      sessionID: string,
      target: {
        current(): boolean
        restore(prompt: PromptInfo): void
        save(prompt: PromptInfo): void
        empty(): void
      },
    ) {
      if (withdrawals.has(sessionID)) return
      withdrawals.add(sessionID)
      try {
        const retry = requests.has(sessionID)
        if (!retry) await Promise.all(admissions.get(sessionID)?.values() ?? [])
        const requestID = requests.get(sessionID) ?? crypto.randomUUID()
        requests.set(sessionID, requestID)
        // Do not cancel a destructive request when the prompt/provider unmounts.
        // Keep the ID on failure: replay must not withdraw a newer batch.
        const response = await client.session.withdraw({ sessionID, requestID }, { throwOnError: true, signal: null })
        if (response.data.length === 0) {
          // An old empty receipt says nothing about prompts admitted since then.
          if (!retry && target.current()) target.empty()
          requests.delete(sessionID)
          return
        }
        const prompt = combineQueuedPrompts(response.data)
        if (target.current()) target.restore(prompt)
        else target.save(prompt)
        requests.delete(sessionID)
      } finally {
        withdrawals.delete(sessionID)
      }
    },
  }
}
