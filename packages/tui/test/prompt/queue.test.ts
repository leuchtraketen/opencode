import { describe, expect, test } from "bun:test"
import { OpenCode, type SessionInboxUser } from "@opencode/client"
import { Skill } from "@opencode/schema/skill"
import { combineQueuedPrompts, createPromptQueue, promptQueue, trimPromptTail } from "../../src/prompt/queue"
import { displaySlice, promptOffsetWidth } from "../../src/prompt/display"
import type { PromptInfo } from "../../src/prompt/history"

const image = { data: "AAA", mime: "image/png", source: { type: "inline" as const } }

function queued(text: string, payload: Partial<SessionInboxUser["payload"]> = {}): SessionInboxUser {
  return {
    id: `msg_${text.length}`,
    sessionID: "ses_test",
    time: { created: 1 },
    type: "user",
    delivery: "queue",
    payload: { text, ...payload },
  }
}

function mentions(prompt: PromptInfo) {
  return [...(prompt.files ?? []), ...(prompt.agents ?? []), ...(prompt.skills ?? [])].map((part) => part.mention)
}

function harness() {
  const requests: { request: Request; response: ReturnType<typeof Promise.withResolvers<Response>> }[] = []
  const api = OpenCode.make({
    baseUrl: "http://localhost:4096",
    fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init)
      const response = Promise.withResolvers<Response>()
      requests.push({ request, response })
      return response.promise
    }) as typeof fetch,
  })
  const queue = createPromptQueue({ api })
  let current = true
  let empty = 0
  const restored: PromptInfo[] = []
  const saved: PromptInfo[] = []
  const target = {
    current: () => current,
    restore: (value: PromptInfo) => void restored.push(value),
    save: (value: PromptInfo) => void saved.push(value),
    empty: () => void empty++,
  }
  return {
    api,
    queue,
    requests,
    target,
    restored,
    saved,
    empty: () => empty,
    invalidate: () => void (current = false),
    async request(index: number) {
      for (let count = 0; count < 100 && !requests[index]; count++) await Bun.sleep(1)
      const item = requests[index]
      if (!item) throw new Error(`Missing request ${index}`)
      return item
    },
  }
}

describe("queued prompt conversion", () => {
  test("combines enqueue order, keeps duplicate attachments and rebases wide/multiline offsets", () => {
    const first = queued("你好\n[Image 1] first", {
      files: [{ ...image, mention: { start: 5, end: 14, text: "[Image 1]" } }],
    })
    const second = queued("second [Image 1] @build @review", {
      files: [{ ...image, mention: { start: 7, end: 16, text: "[Image 1]" } }],
      agents: [{ name: "build", mention: { start: 17, end: 23, text: "@build" } }],
      skills: [{ id: "review", name: "review", mention: { start: 24, end: 31, text: "@review" } }],
    })
    const before = structuredClone([first, second])
    const draft = combineQueuedPrompts([first, second])
    expect(draft.text).toBe("你好\n[Image 1] first\n\nsecond [Image 1] @build @review")
    expect(draft.mode).toBe("normal")
    expect(draft.pasted).toEqual([])
    expect(draft.files).toHaveLength(2)
    expect(draft.files?.every((file) => file.uri === "data:image/png;base64,AAA")).toBe(true)
    expect(draft.agents).toEqual([{ name: "build", mention: { start: 39, end: 45, text: "@build" } }])
    expect(draft.skills).toEqual([{ id: Skill.ID.make("review"), mention: { start: 46, end: 53, text: "@review" } }])
    const sources = mentions(draft)
    expect(sources.map((source) => source?.start)).toEqual([5, 29, 39, 46])
    for (const source of sources) {
      expect(displaySlice(draft.text, source!.start, source!.end)).toBe(source!.text)
    }
    expect([first, second]).toEqual(before)
  })

  test("locates mentions whose stored offsets do not match the text", () => {
    // Offsets measured against a paste placeholder are shorter than the expanded text.
    const draft = combineQueuedPrompts([
      queued("line one\nline two\nline three [Image 1] @build", {
        files: [{ ...image, mention: { start: 8, end: 17, text: "[Image 1]" } }],
        agents: [{ name: "build", mention: { start: 18, end: 24, text: "@build" } }],
      }),
    ])
    expect(draft.text).toBe("line one\nline two\nline three [Image 1] @build")
    expect(draft.files?.[0]?.mention).toEqual({ start: 29, end: 38, text: "[Image 1]" })
    expect(draft.agents?.[0]?.mention).toEqual({ start: 39, end: 45, text: "@build" })
  })

  test("mentionless files stay mentionless while agents and skills acquire editable markers", () => {
    const draft = combineQueuedPrompts([
      queued("look", {
        files: [
          { ...image, name: "pic.png" },
          { ...image, mention: { start: 0, end: 9, text: "[Missing]" } },
        ],
        agents: [{ name: "build" }],
        skills: [{ id: "review", name: "review", mention: { start: 0, end: 5, text: "@gone" } }],
      }),
      queued("", { agents: [{ name: "plan", mention: { start: 0, end: 5, text: "@plan" } }] }),
    ])
    expect(draft.text).toBe("look @build @review\n\n@plan")
    expect(draft.files).toEqual([
      { uri: "data:image/png;base64,AAA", name: "pic.png", description: undefined, mention: undefined },
      { uri: "data:image/png;base64,AAA", name: undefined, description: undefined, mention: undefined },
    ])
    expect(draft.agents).toEqual([
      { name: "build", mention: { start: 5, end: 11, text: "@build" } },
      { name: "plan", mention: { start: 21, end: 26, text: "@plan" } },
    ])
    expect(draft.skills).toEqual([{ id: Skill.ID.make("review"), mention: { start: 12, end: 19, text: "@review" } }])
    for (const source of mentions(draft)) {
      if (!source) continue
      expect(displaySlice(draft.text, source.start, source.end)).toBe(source.text)
    }
  })

  test("combines an empty batch into an empty draft", () => {
    expect(combineQueuedPrompts([])).toEqual({
      text: "",
      files: [],
      agents: [],
      skills: [],
      pasted: [],
      mode: "normal",
    })
  })

  test("submission drops the withdrawal's trailing padding without disturbing attachments", () => {
    const draft = combineQueuedPrompts([queued("first"), queued("second")])
    const padded: PromptInfo = { ...draft, text: `${draft.text}\n\n` }
    expect(trimPromptTail(padded).text).toBe("first\n\nsecond")
    // An untouched draft must be returned as-is, not needlessly copied.
    expect(trimPromptTail(draft)).toBe(draft)
    // Deliberate trailing blank lines the user typed are padding too: the wire
    // prompt should never carry them.
    expect(trimPromptTail({ ...draft, text: "text\n   \n\t" }).text).toBe("text")
    expect(trimPromptTail({ text: "   ", files: [], agents: [], skills: [], pasted: [] }).text).toBe("")
  })

  test("trailing whitespace inside a mention or paste placeholder is never trimmed", () => {
    const draft = combineQueuedPrompts([queued("look ", { agents: [{ name: "build" }] })])
    const marker = draft.agents![0]!.mention!
    // Force the marker to cover the trailing whitespace so trimming would corrupt it.
    const risky: PromptInfo = { ...draft, text: `${draft.text}\n` }
    marker.end = promptOffsetWidth(risky.text)
    expect(trimPromptTail(risky)).toBe(risky)

    const pasted: PromptInfo = {
      text: "[Pasted ~3 lines]\n",
      files: [],
      agents: [],
      skills: [],
      pasted: [{ text: "a\nb\nc", source: { start: 0, end: 18, text: "[Pasted ~3 lines]" } }],
    }
    expect(trimPromptTail(pasted)).toBe(pasted)
  })
})

describe("queued prompt coordination", () => {
  test("one atomic withdrawal: duplicate Up cannot duplicate a draft", async () => {
    const h = harness()
    const first = h.queue.withdraw("ses_test", h.target)
    const second = h.queue.withdraw("ses_test", h.target)
    expect(h.queue.busy("ses_test")).toBe(true)
    const request = await h.request(0)
    expect(request.request.method).toBe("POST")
    expect(new URL(request.request.url).pathname).toBe("/api/session/ses_test/inbox/withdraw")
    expect(request.request.headers.get("content-type")).toBe("application/json")
    expect(await request.request.json()).toEqual({ requestID: expect.any(String) })
    request.response.resolve(Response.json({ data: [queued("one"), queued("two")] }))
    await Promise.all([first, second])
    expect(h.requests).toHaveLength(1)
    expect(h.restored).toEqual([{ text: "one\n\ntwo", files: [], agents: [], skills: [], pasted: [], mode: "normal" }])
    expect(h.saved).toEqual([])
    expect(h.empty()).toBe(0)
    expect(h.queue.busy("ses_test")).toBe(false)
  })

  test("waits for all in-flight admissions without waiting for the active turn", async () => {
    const h = harness()
    const first = Promise.withResolvers<void>()
    const second = Promise.withResolvers<void>()
    h.queue.admit("ses_test", first.promise)
    h.queue.admit("ses_test", second.promise)
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    await Bun.sleep(5)
    expect(h.requests).toHaveLength(0)
    second.resolve()
    await Bun.sleep(5)
    expect(h.requests).toHaveLength(0)
    first.resolve()
    const withdrawal = await h.request(0)
    withdrawal.response.resolve(Response.json({ data: [queued("one")] }))
    await withdrawing
    expect(h.restored[0]?.text).toBe("one")
  })

  test("a rejected admission was rolled back, so the withdrawal proceeds once it settles", async () => {
    const h = harness()
    const admission = Promise.withResolvers<void>()
    const admitted = h.queue.admit("ses_test", admission.promise).catch(() => undefined)
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    await Bun.sleep(5)
    expect(h.requests).toHaveLength(0)
    admission.reject(new Error("network failed"))
    await admitted
    const withdrawal = await h.request(0)
    withdrawal.response.resolve(Response.json({ data: [] }))
    await withdrawing
    expect(h.empty()).toBe(1)
    expect(h.queue.busy("ses_test")).toBe(false)
  })

  test("typing, navigation or unmount keeps withdrawn content in recovery instead of touching the new input", async () => {
    const h = harness()
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    const request = await h.request(0)
    h.invalidate()
    request.response.resolve(Response.json({ data: [queued("never lose this")] }))
    await withdrawing
    expect(h.restored).toEqual([])
    expect(h.saved[0]?.text).toBe("never lose this")
  })

  test("only an empty successful response on an unchanged target recalls history", async () => {
    const h = harness()
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    ;(await h.request(0)).response.resolve(Response.json({ data: [] }))
    await withdrawing
    expect(h.empty()).toBe(1)
    const failed = h.queue.withdraw("ses_test", h.target)
    const rejected = failed.catch((error) => error)
    ;(await h.request(1)).response.reject(new Error("offline"))
    expect(await rejected).toBeInstanceOf(Error)
    expect(h.empty()).toBe(1)
    const changed = h.queue.withdraw("ses_test", h.target)
    h.invalidate()
    ;(await h.request(2)).response.resolve(Response.json({ data: [] }))
    await changed
    expect(h.empty()).toBe(1)
  })

  test.each(["empty", "nonempty"])(
    "replays a lost %s response without waiting for or withdrawing new admissions",
    async (batch) => {
      const h = harness()
      const receipt = batch === "empty" ? [] : [queued("original batch")]
      const first = h.queue.withdraw("ses_test", h.target).catch((error) => error)
      const lost = await h.request(0)
      const originalID = (await lost.request.json()).requestID
      expect(originalID.length).toBeGreaterThan(0)
      expect(originalID.length).toBeLessThanOrEqual(128)
      // The server committed this receipt, but its response never reached the client.
      lost.response.reject(new TypeError("response lost after commit"))
      expect(await first).toBeInstanceOf(Error)

      const admission = Promise.withResolvers<void>()
      h.queue.admit("ses_test", admission.promise)
      const replay = h.queue.withdraw("ses_test", h.target)
      const retry = await h.request(1)
      expect((await retry.request.json()).requestID).toBe(originalID)
      retry.response.resolve(Response.json({ data: receipt }))
      await replay
      expect(h.restored.map((draft) => draft.text)).toEqual(batch === "empty" ? [] : ["original batch"])
      expect(h.empty()).toBe(0)

      const next = h.queue.withdraw("ses_test", h.target)
      await Bun.sleep(5)
      expect(h.requests).toHaveLength(2)
      admission.resolve()
      const fresh = await h.request(2)
      const freshID = (await fresh.request.json()).requestID
      expect(freshID).not.toBe(originalID)
      fresh.response.resolve(Response.json({ data: [queued("new batch")] }))
      await next
      expect(h.restored.at(-1)?.text).toBe("new batch")

      const empty = h.queue.withdraw("ses_test", h.target)
      const emptyRequest = await h.request(3)
      const emptyID = (await emptyRequest.request.json()).requestID
      expect(emptyID).not.toBe(freshID)
      emptyRequest.response.resolve(Response.json({ data: [] }))
      await empty
      const afterEmpty = h.queue.withdraw("ses_test", h.target)
      const afterEmptyRequest = await h.request(4)
      expect((await afterEmptyRequest.request.json()).requestID).not.toBe(emptyID)
      afterEmptyRequest.response.resolve(Response.json({ data: [] }))
      await afterEmpty
      expect(h.empty()).toBe(2)
    },
  )

  test("retry identities and in-flight guards are independent for each session", async () => {
    const h = harness()
    const first = h.queue.withdraw("ses_test", h.target).catch((error) => error)
    const second = h.queue.withdraw("ses_other", h.target).catch((error) => error)
    const firstRequest = await h.request(0)
    const secondRequest = await h.request(1)
    const firstID = (await firstRequest.request.json()).requestID
    const secondID = (await secondRequest.request.json()).requestID
    expect(secondID).not.toBe(firstID)
    firstRequest.response.reject(new TypeError("first lost response"))
    secondRequest.response.reject(new TypeError("second lost response"))
    await Promise.all([first, second])

    const retrySecond = h.queue.withdraw("ses_other", h.target)
    const retryFirst = h.queue.withdraw("ses_test", h.target)
    const secondReplay = await h.request(2)
    const firstReplay = await h.request(3)
    expect(new URL(secondReplay.request.url).pathname).toBe("/api/session/ses_other/inbox/withdraw")
    expect((await secondReplay.request.json()).requestID).toBe(secondID)
    expect(new URL(firstReplay.request.url).pathname).toBe("/api/session/ses_test/inbox/withdraw")
    expect((await firstReplay.request.json()).requestID).toBe(firstID)
    secondReplay.response.resolve(Response.json({ data: [{ ...queued("second session"), sessionID: "ses_other" }] }))
    await retrySecond
    expect(h.queue.busy("ses_other")).toBe(false)
    expect(h.queue.busy("ses_test")).toBe(true)
    firstReplay.response.resolve(Response.json({ data: [queued("first session")] }))
    await retryFirst
    expect(h.restored.map((draft) => draft.text)).toEqual(["second session", "first session"])
  })

  test("one coordinator is shared per client connection", () => {
    const h = harness()
    const client = { api: h.api }
    const other = { api: h.api }
    expect(promptQueue(client)).toBe(promptQueue(client))
    expect(promptQueue(other)).not.toBe(promptQueue(client))
  })
})
