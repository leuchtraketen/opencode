import { describe, expect, test } from "bun:test"
import { createOpencodeClient, type SessionPromptPromptInput } from "@opencode-ai/sdk/v2"
import { combineQueuedPrompts, createPromptQueue, promptParts, trimPromptTail } from "../../src/prompt/queue"
import { displaySlice, promptOffsetWidth } from "../../src/prompt/display"
import type { PromptInfo } from "../../src/prompt/history"

const prompt = (text: string): SessionPromptPromptInput => ({ sessionID: "ses_test", parts: [{ type: "text", text }] })

function harness() {
  const requests: { request: Request; response: ReturnType<typeof Promise.withResolvers<Response>> }[] = []
  const abort = new AbortController()
  const client = createOpencodeClient({
    baseUrl: "http://localhost:4096",
    signal: abort.signal,
    fetch: (async (request: Request) => {
      const response = Promise.withResolvers<Response>()
      requests.push({ request, response })
      return response.promise
    }) as typeof fetch,
  })
  const queue = createPromptQueue(client)
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
    queue,
    requests,
    abort,
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
  test("combines admission order, keeps duplicate attachments and rebases wide/multiline offsets", () => {
    const first = prompt("你好\n[Image 1] first")
    const second = prompt("second [Image 1] @build")
    first.parts.push({
      id: "prt_old",
      type: "file",
      mime: "image/png",
      url: "data:image/png;base64,AAA",
      source: { type: "file", path: "", text: { value: "[Image 1]", start: 5, end: 14 } },
    })
    second.parts.push(
      {
        ...first.parts[1],
        type: "file",
        mime: "image/png",
        url: "data:image/png;base64,AAA",
        source: { type: "file", path: "", text: { value: "[Image 1]", start: 7, end: 16 } },
      },
      { type: "agent", name: "build", source: { value: "@build", start: 17, end: 23 } },
    )
    const before = structuredClone([first, second])
    const draft = combineQueuedPrompts([first, second])
    expect(draft.input).toBe("你好\n[Image 1] first\n\nsecond [Image 1] @build")
    expect(draft.parts.map((part) => part.type)).toEqual(["file", "file", "agent"])
    expect(draft.parts.every((part) => !("id" in part))).toBe(true)
    const sources = draft.parts.map((part) => (part.type === "agent" ? part.source! : part.source!.text))
    expect(sources.map((source) => source.start)).toEqual([5, 29, 39])
    sources.forEach((source) => expect(displaySlice(draft.input, source.start, source.end)).toBe(source.value))
    expect([first, second]).toEqual(before)
  })

  test("expanded pastes rebase attachments before serialization and round-trip withdrawal", () => {
    const marker = "[Paste]"
    const text = "你好\nlong\npaste"
    const draft: PromptInfo = {
      input: `${marker} [Image 1] ${marker} @build`,
      parts: [
        { type: "text", text, source: { text: { value: marker, start: 0, end: 7 } } },
        {
          type: "file",
          mime: "image/png",
          url: "data:image/png;base64,AAA",
          source: { type: "file", path: "", text: { value: "[Image 1]", start: 8, end: 17 } },
        },
        { type: "text", text: "second\npaste", source: { text: { value: marker, start: 18, end: 25 } } },
        { type: "agent", name: "build", source: { value: "@build", start: 26, end: 32 } },
      ],
    }
    const original = structuredClone(draft)
    const parts = promptParts(draft)
    const restored = combineQueuedPrompts([{ sessionID: "ses_test", parts }])
    expect(restored.input).toBe(`${text} [Image 1] second\npaste @build`)
    expect(restored.parts).toHaveLength(2)
    for (const part of restored.parts) {
      const source = part.type === "agent" ? part.source! : part.source!.text
      expect(displaySlice(restored.input, source.start, source.end)).toBe(source.value)
    }
    expect(promptParts(restored)).toEqual(parts)
    expect(draft).toEqual(original)
  })

  test("attachment-only payloads acquire editable markers and hidden text metadata survives resubmit", () => {
    const hidden = {
      type: "text" as const,
      text: "editor context",
      synthetic: true,
      metadata: { kind: "editor_context" },
    }
    const draft = combineQueuedPrompts([
      {
        sessionID: "ses_test",
        parts: [
          hidden,
          { type: "file", mime: "image/png", filename: "pic.png", url: "data:image/png;base64,AAA" },
          { type: "agent", name: "build" },
        ],
      },
    ])
    expect(draft.input).toBe("[pic.png] @build")
    expect(draft.parts[0]).toEqual(hidden)
    expect(promptParts(draft).slice(1)).toEqual(draft.parts)
    expect(promptOffsetWidth(draft.input)).toBe(16)
  })

  test("submission drops the withdrawal's trailing padding without disturbing attachments", () => {
    const draft = combineQueuedPrompts([prompt("first"), prompt("second")])
    const padded: PromptInfo = { ...draft, input: `${draft.input}\n\n` }
    expect(trimPromptTail(padded).input).toBe("first\n\nsecond")
    // An untouched draft must be returned as-is, not needlessly copied.
    expect(trimPromptTail(draft)).toBe(draft)
    // Deliberate trailing blank lines the user typed are padding too: the wire
    // prompt should never carry them.
    expect(trimPromptTail({ ...draft, input: "text\n   \n\t" }).input).toBe("text")
    expect(trimPromptTail({ input: "   ", parts: [], mode: "normal" }).input).toBe("")
  })

  test("trailing whitespace inside an attachment marker is never trimmed", () => {
    const draft = combineQueuedPrompts([
      {
        sessionID: "ses_test",
        parts: [
          { type: "text", text: "look " },
          { type: "file", mime: "image/png", filename: "pic.png", url: "data:image/png;base64,AAA" },
        ],
      },
    ])
    const marker = draft.parts.find((part) => part.type === "file")!.source!.text
    // Force the marker to cover the trailing whitespace so trimming would corrupt it.
    const risky: PromptInfo = { ...draft, input: `${draft.input}\n` }
    marker.end = promptOffsetWidth(risky.input)
    expect(trimPromptTail(risky)).toBe(risky)
  })
})

describe("queued prompt coordination", () => {
  test("one atomic withdrawal, duplicate Up and overlapping submission cannot duplicate a draft", async () => {
    const h = harness()
    const first = h.queue.withdraw("ses_test", h.target)
    const second = h.queue.withdraw("ses_test", h.target)
    expect(h.queue.busy("ses_test")).toBe(true)
    await expect(h.queue.submit(prompt("late"))).rejects.toThrow("being withdrawn")
    const request = await h.request(0)
    expect(request.request.method).toBe("POST")
    expect(new URL(request.request.url).pathname).toBe("/session/ses_test/queue/withdraw")
    expect(request.request.headers.get("content-type")).toBe("application/json")
    expect(await request.request.json()).toEqual({ requestID: expect.any(String) })
    request.response.resolve(Response.json([prompt("one"), prompt("two")]))
    await Promise.all([first, second])
    expect(h.requests).toHaveLength(1)
    expect(h.restored).toEqual([{ input: "one\n\ntwo", parts: [], mode: "normal" }])
    expect(h.saved).toEqual([])
    expect(h.empty()).toBe(0)
    expect(h.queue.busy("ses_test")).toBe(false)
  })

  test("waits for all in-flight admissions without waiting for the active turn", async () => {
    const h = harness()
    const send = h.queue.submit(prompt("one"))
    const submitted = await h.request(0)
    const sendSecond = h.queue.submit(prompt("two"))
    const submittedSecond = await h.request(1)
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    await Bun.sleep(5)
    expect(h.requests).toHaveLength(2)
    submittedSecond.response.resolve(new Response(null, { status: 204 }))
    await sendSecond
    await Bun.sleep(5)
    expect(h.requests).toHaveLength(2)
    submitted.response.resolve(new Response(null, { status: 204 }))
    await send
    const withdrawal = await h.request(2)
    withdrawal.response.resolve(Response.json([prompt("one")]))
    await withdrawing
    expect(h.restored[0].input).toBe("one")
  })

  test("an admission failure does not fall back to history or issue a withdrawal", async () => {
    const h = harness()
    const send = h.queue.submit(prompt("one"))
    const submitted = await h.request(0)
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    const failed = withdrawing.catch((error) => error)
    const sendFailed = send.catch((error) => error)
    submitted.response.reject(new Error("network failed"))
    expect(await failed).toBeInstanceOf(Error)
    expect(await sendFailed).toBeInstanceOf(Error)
    expect(h.requests).toHaveLength(1)
    expect(h.empty()).toBe(0)
    expect(h.queue.busy("ses_test")).toBe(false)
  })

  test("typing, navigation or unmount keeps withdrawn content in recovery instead of touching the new input", async () => {
    const h = harness()
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    const request = await h.request(0)
    h.invalidate()
    h.abort.abort()
    expect(request.request.signal.aborted).toBe(false)
    request.response.resolve(Response.json([prompt("never lose this")]))
    await withdrawing
    expect(h.restored).toEqual([])
    expect(h.saved[0].input).toBe("never lose this")
  })

  test("only an empty successful response on an unchanged target recalls history", async () => {
    const h = harness()
    const withdrawing = h.queue.withdraw("ses_test", h.target)
    ;(await h.request(0)).response.resolve(Response.json([]))
    await withdrawing
    expect(h.empty()).toBe(1)
    const failed = h.queue.withdraw("ses_test", h.target)
    const rejected = failed.catch((error) => error)
    ;(await h.request(1)).response.reject(new Error("offline"))
    expect(await rejected).toBeInstanceOf(Error)
    expect(h.empty()).toBe(1)
    const changed = h.queue.withdraw("ses_test", h.target)
    h.invalidate()
    ;(await h.request(2)).response.resolve(Response.json([]))
    await changed
    expect(h.empty()).toBe(1)
  })

  test.each(["empty", "nonempty"])(
    "replays a lost %s response without waiting for or withdrawing new admissions",
    async (batch) => {
      const h = harness()
      const receipt = batch === "empty" ? [] : [prompt("original batch")]
      const first = h.queue.withdraw("ses_test", h.target).catch((error) => error)
      const lost = await h.request(0)
      const originalID = (await lost.request.json()).requestID
      expect(originalID.length).toBeGreaterThan(0)
      expect(originalID.length).toBeLessThanOrEqual(128)
      // The server committed this receipt, but its response never reached the SDK.
      lost.response.reject(new TypeError("response lost after commit"))
      expect(await first).toBeInstanceOf(Error)

      const send = h.queue.submit(prompt("new batch"))
      const admission = await h.request(1)
      const replay = h.queue.withdraw("ses_test", h.target)
      const retry = await h.request(2)
      expect((await retry.request.json()).requestID).toBe(originalID)
      retry.response.resolve(Response.json(receipt))
      await replay
      expect(h.restored.map((draft) => draft.input)).toEqual(batch === "empty" ? [] : ["original batch"])
      expect(h.empty()).toBe(0)

      const next = h.queue.withdraw("ses_test", h.target)
      await Bun.sleep(5)
      expect(h.requests).toHaveLength(3)
      admission.response.resolve(new Response(null, { status: 204 }))
      await send
      const fresh = await h.request(3)
      const freshID = (await fresh.request.json()).requestID
      expect(freshID).not.toBe(originalID)
      fresh.response.resolve(Response.json([prompt("new batch")]))
      await next
      expect(h.restored.at(-1)?.input).toBe("new batch")

      const empty = h.queue.withdraw("ses_test", h.target)
      const emptyRequest = await h.request(4)
      const emptyID = (await emptyRequest.request.json()).requestID
      expect(emptyID).not.toBe(freshID)
      emptyRequest.response.resolve(Response.json([]))
      await empty
      const afterEmpty = h.queue.withdraw("ses_test", h.target)
      const afterEmptyRequest = await h.request(5)
      expect((await afterEmptyRequest.request.json()).requestID).not.toBe(emptyID)
      afterEmptyRequest.response.resolve(Response.json([]))
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
    expect(new URL(secondReplay.request.url).pathname).toBe("/session/ses_other/queue/withdraw")
    expect((await secondReplay.request.json()).requestID).toBe(secondID)
    expect(new URL(firstReplay.request.url).pathname).toBe("/session/ses_test/queue/withdraw")
    expect((await firstReplay.request.json()).requestID).toBe(firstID)
    secondReplay.response.resolve(Response.json([{ ...prompt("second session"), sessionID: "ses_other" }]))
    await retrySecond
    expect(h.queue.busy("ses_other")).toBe(false)
    expect(h.queue.busy("ses_test")).toBe(true)
    firstReplay.response.resolve(Response.json([prompt("first session")]))
    await retryFirst
    expect(h.restored.map((draft) => draft.input)).toEqual(["second session", "first session"])
  })
})
