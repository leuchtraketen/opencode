import { expect, test } from "bun:test"
import { TextareaRenderable } from "@opentui/core"
import type { SessionInboxUser } from "@opencode/client"
import type { Config } from "../../src/config"
import { displaySlice } from "../../src/prompt/display"
import { parsePromptHistory } from "../../src/prompt/history"
import { parsePromptStash } from "../../src/prompt/stash"
import { createAppFixture } from "../fixture/app"
import { directory, json } from "../fixture/tui-client"
import { tmpdir } from "../fixture/fixture"

const location = { directory, project: { id: "project", directory, canonical: directory } }
// Drafts are stashed per session ID across Prompt remounts in module state, so
// each test uses its own session to stay independent.
let sessions = 0

const image = { data: "AAA", mime: "image/png", source: { type: "inline" as const } }

function queuedItem(
  sessionID: string,
  id: string,
  text: string,
  payload: Partial<SessionInboxUser["payload"]> = {},
): SessionInboxUser {
  return { id, sessionID, time: { created: 1 }, type: "user", delivery: "queue", payload: { text, ...payload } }
}

function queued(sessionID: string) {
  return [
    queuedItem(sessionID, "msg_1", "first [Image 1]", {
      files: [{ ...image, mention: { start: 6, end: 15, text: "[Image 1]" } }],
    }),
    queuedItem(sessionID, "msg_2", "second @build", {
      agents: [{ name: "build", mention: { start: 7, end: 13, text: "@build" } }],
    }),
  ]
}

async function wait(check: () => boolean | Promise<boolean>) {
  for (let count = 0; count < 400; count++) {
    if (await check()) return
    await Bun.sleep(5)
  }
  throw new Error("Timed out waiting for TUI")
}

type Captured = {
  path: string
  body: any
  reply(response: Response): void
}

async function render(state: string, config: Config.Info = { animations: false, prompt: { queue_edit: true } }) {
  const sessionID = `ses_queue_edit_${sessions++}`
  const session = {
    id: sessionID,
    projectID: "project",
    title: "Queue edit fixture",
    agent: "build",
    model: { providerID: "demo", id: "model" },
    location: { directory },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 },
  }
  const requests: Captured[] = []
  const setup = await createAppFixture({
    state,
    args: { sessionID },
    config,
    fetch: async (url, request) => {
      if (url.pathname === `/api/session/${sessionID}`) return json({ data: session })
      if (url.pathname === `/api/session/${sessionID}/message`) return json({ data: [], cursor: {} })
      if (url.pathname === `/api/session/${sessionID}/inbox` && request.method === "GET") return json({ data: [] })
      if (url.pathname === `/api/session/${sessionID}/permission`) return json({ data: [] })
      if (url.pathname === "/api/agent")
        return json({ location, data: [{ id: "build", mode: "primary", hidden: false, permissions: [] }] })
      if (url.pathname === "/api/provider") return json({ location, data: [{ id: "demo", name: "Demo" }] })
      if (url.pathname === "/api/model")
        return json({ location, data: [{ id: "model", providerID: "demo", name: "Demo Model", variants: [] }] })
      if (
        request.method === "POST" &&
        (url.pathname === `/api/session/${sessionID}/prompt` ||
          url.pathname === `/api/session/${sessionID}/inbox/withdraw`)
      ) {
        const body = await request.json()
        return new Promise<Response>((reply) => requests.push({ path: url.pathname, body, reply }))
      }
      return undefined
    },
  })
  await setup.ready
  await setup.waitForFrame((frame) => frame.includes("Demo Model"))
  await setup.waitFor(() => setup.renderer.currentFocusedEditor instanceof TextareaRenderable)
  return {
    setup,
    sessionID,
    requests,
    queued: queued(sessionID),
    accepted(body: any) {
      return json({
        data: {
          id: body.id,
          sessionID,
          type: "user",
          time: { created: 10 },
          payload: { text: body.text, files: body.files, agents: body.agents, skills: body.skills },
          delivery: body.delivery ?? "steer",
        },
      })
    },
    async request(index: number) {
      await wait(() => requests.length > index)
      return requests[index]!
    },
    get input() {
      const input = setup.renderer.currentFocusedEditor
      if (!(input instanceof TextareaRenderable)) throw new Error("Expected prompt textarea")
      return input
    },
    async idle() {
      await Bun.sleep(30)
      await setup.renderOnce()
    },
    [Symbol.asyncDispose]: () => setup[Symbol.asyncDispose](),
  }
}

async function seedHistory(state: string, text: string) {
  await Bun.write(
    `${state}/prompt-history.jsonl`,
    JSON.stringify({ text, files: [], agents: [], skills: [], pasted: [] }) + "\n",
  )
}

test.each(["omitted", "empty", "false"])(
  "queue_edit %s keeps history recall and submission unchanged",
  async (setting) => {
    await using state = await tmpdir()
    await seedHistory(state.path, "old history")
    const config: Config.Info = {
      animations: false,
      ...(setting === "omitted" ? {} : { prompt: setting === "empty" ? {} : { queue_edit: false } }),
    }
    await using h = await render(state.path, config)
    h.setup.mockInput.pressArrow("up")
    await wait(() => h.input.plainText === "old history")
    expect(h.requests).toHaveLength(0)
    // Up left the cursor at the start; Down first moves it to the end, then recalls forward.
    h.setup.mockInput.pressArrow("down")
    await wait(() => h.input.cursorOffset === h.input.plainText.length)
    h.setup.mockInput.pressArrow("down")
    await wait(() => h.input.plainText === "")
    await h.setup.mockInput.typeText("pending prompt  ")
    h.setup.mockInput.pressEnter()
    const sent = await h.request(0)
    expect(sent.path).toBe(`/api/session/${h.sessionID}/prompt`)
    // Without the opt-in, submission does not trim the draft.
    expect(sent.body.text).toBe("pending prompt  ")
    sent.reply(h.accepted(sent.body))
    await wait(() => h.input.plainText === "")
    h.setup.mockInput.pressArrow("up")
    await wait(() => h.input.plainText === "pending prompt  ")
    expect(h.requests).toHaveLength(1)
  },
)

test("empty Up withdraws, opens a trailing line, and Enter sends one trimmed prompt with intact mentions", async () => {
  await using state = await tmpdir()
  await seedHistory(state.path, "old history")
  await using h = await render(state.path)
  h.setup.mockInput.pressArrow("up")
  const withdrawal = await h.request(0)
  expect(withdrawal.path).toBe(`/api/session/${h.sessionID}/inbox/withdraw`)
  expect(typeof withdrawal.body.requestID).toBe("string")
  // A second Up while the withdrawal is pending is a no-op.
  h.setup.mockInput.pressArrow("up")
  await h.idle()
  expect(h.requests).toHaveLength(1)
  withdrawal.reply(json({ data: h.queued }))
  await wait(() => h.input.plainText === "first [Image 1]\n\nsecond @build\n\n")
  await wait(() => h.input.cursorOffset === h.input.plainText.length)
  // Prompts keep their blank-line separator and a blank line opens below, so
  // the two prompts sit on rows 0 and 2 and the cursor waits on row 4.
  expect(h.input.visualCursor.logicalRow).toBe(4)
  expect(h.input.visualCursor.logicalCol).toBe(0)

  h.input.cursorOffset = 0
  await h.setup.mockInput.typeText("edit ")
  await wait(() => h.input.plainText.startsWith("edit "))
  h.setup.mockInput.pressEnter()
  const sent = await h.request(1)
  expect(sent.path).toBe(`/api/session/${h.sessionID}/prompt`)
  // The editing affordance must not leak trailing whitespace into the wire prompt.
  expect(sent.body.text).toBe("edit first [Image 1]\n\nsecond @build")
  expect(sent.body.files).toHaveLength(1)
  expect(sent.body.agents).toHaveLength(1)
  const file = sent.body.files[0]
  const agent = sent.body.agents[0]
  expect(file.uri).toBe("data:image/png;base64,AAA")
  expect(displaySlice(sent.body.text, file.mention.start, file.mention.end)).toBe("[Image 1]")
  expect(displaySlice(sent.body.text, agent.mention.start, agent.mention.end)).toBe("@build")
  sent.reply(h.accepted(sent.body))
  await wait(() => h.input.plainText === "")
  await h.idle()
  expect(h.requests).toHaveLength(2)
  // The padding must not survive into recallable history either.
  await wait(async () => {
    const history = parsePromptHistory(await Bun.file(`${state.path}/prompt-history.jsonl`).text())
    return history.at(-1)?.text === "edit first [Image 1]\n\nsecond @build"
  })
})

test("an empty withdrawal falls back to history and a nonempty draft never withdraws", async () => {
  await using state = await tmpdir()
  await seedHistory(state.path, "old history")
  await using h = await render(state.path)
  await h.setup.mockInput.typeText("keep this draft")
  h.input.cursorOffset = 0
  h.setup.mockInput.pressArrow("up")
  await h.idle()
  expect(h.input.plainText).toBe("keep this draft")
  expect(h.requests).toHaveLength(0)
  h.input.clear()
  await wait(() => h.input.plainText === "")

  h.setup.mockInput.pressArrow("up")
  const withdrawal = await h.request(0)
  expect(h.input.plainText).toBe("")
  withdrawal.reply(json({ data: [] }))
  await wait(() => h.input.plainText === "old history")
  expect(h.requests).toHaveLength(1)
})

test.each([400, 404, 500])(
  "withdrawal error %s keeps history and the input; the retry replays the request",
  async (status) => {
    await using state = await tmpdir()
    await seedHistory(state.path, "old history")
    await using h = await render(state.path)
    h.setup.mockInput.pressArrow("up")
    const failed = await h.request(0)
    failed.reply(json({ message: "withdraw failed" }, { status }))
    await h.setup.waitForFrame((frame) => frame.includes("Failed to withdraw queued prompts"))
    expect(h.input.plainText).toBe("")
    expect(await Bun.file(`${state.path}/prompt-stash.jsonl`).exists()).toBe(false)

    h.setup.mockInput.pressArrow("up")
    const replay = await h.request(1)
    expect(replay.body.requestID).toBe(failed.body.requestID)
    replay.reply(json({ data: [] }))
    await h.idle()
    // A replayed empty receipt says nothing about the current queue.
    expect(h.input.plainText).toBe("")

    h.setup.mockInput.pressArrow("up")
    const fresh = await h.request(2)
    expect(fresh.body.requestID).not.toBe(replay.body.requestID)
    fresh.reply(json({ data: [] }))
    await wait(() => h.input.plainText === "old history")
  },
)

test.each(["typing", "typing-erased", "shutdown"])(
  "a pending withdrawal saves its draft to the stash after %s",
  async (change) => {
    await using state = await tmpdir()
    await using h = await render(state.path)
    h.setup.mockInput.pressArrow("up")
    const withdrawal = await h.request(0)
    if (change === "typing" || change === "typing-erased") {
      await h.setup.mockInput.typeText("new draft")
      await wait(() => h.input.plainText === "new draft")
      // Enter is blocked while the withdrawal is pending.
      h.setup.mockInput.pressEnter()
      await h.idle()
      expect(h.requests).toHaveLength(1)
      if (change === "typing-erased") {
        h.input.clear()
        await wait(() => h.input.plainText === "")
      }
    }
    if (change === "shutdown") h.setup.renderer.destroy()
    withdrawal.reply(json({ data: h.queued }))
    const stash = () => Bun.file(`${state.path}/prompt-stash.jsonl`)
    await wait(async () => (await stash().exists()) && parsePromptStash(await stash().text()).length === 1)
    const [entry] = parsePromptStash(await stash().text())
    expect(entry!.prompt.text).toBe("first [Image 1]\n\nsecond @build")
    expect(entry!.prompt.files).toHaveLength(1)
    expect(entry!.prompt.agents).toHaveLength(1)
    expect(h.requests).toHaveLength(1)
    if (change === "typing") {
      expect(h.input.plainText).toBe("new draft")
      await h.setup.waitForFrame((frame) => frame.includes("saved in the prompt stash"))
    }
  },
)

test("an earlier deferred Enter cannot resubmit the withdrawn draft", async () => {
  await using state = await tmpdir()
  await using h = await render(state.path)
  h.setup.mockInput.pressEnter()
  h.setup.mockInput.pressArrow("up")
  const withdrawal = await h.request(0)
  withdrawal.reply(json({ data: h.queued }))
  await wait(() => h.input.plainText === "first [Image 1]\n\nsecond @build\n\n")
  await h.idle()
  expect(h.requests).toHaveLength(1)
})

test("Up immediately after Enter waits for the admission, then withdraws the prompt just sent", async () => {
  await using state = await tmpdir()
  await using h = await render(state.path)
  await h.setup.mockInput.typeText("queued locally")
  h.setup.mockInput.pressEnter()
  const sent = await h.request(0)
  expect(sent.path).toBe(`/api/session/${h.sessionID}/prompt`)
  await wait(() => h.input.plainText === "")
  h.setup.mockInput.pressArrow("up")
  await h.idle()
  expect(h.requests).toHaveLength(1)
  sent.reply(h.accepted(sent.body))
  const withdrawal = await h.request(1)
  expect(withdrawal.path).toBe(`/api/session/${h.sessionID}/inbox/withdraw`)
  withdrawal.reply(json({ data: [queuedItem(h.sessionID, sent.body.id, sent.body.text)] }))
  await wait(() => h.input.plainText === "queued locally\n\n")
})
