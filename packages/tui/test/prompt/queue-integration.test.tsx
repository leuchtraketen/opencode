/** @jsxImportSource @opentui/solid */
import { TextareaRenderable } from "@opentui/core"
import { createDefaultOpenTuiKeymap } from "@opentui/keymap/opentui"
import { testRender, useRenderer } from "@opentui/solid"
import { expect, test } from "bun:test"
import { createSignal, onCleanup, Show, type ParentProps } from "solid-js"
import type { JSX } from "@opentui/solid"
import type { SessionPromptPromptInput } from "@opencode-ai/sdk/v2"
import { Prompt, type PromptRef } from "../../src/component/prompt"
import { ArgsProvider } from "../../src/context/args"
import { DataProvider } from "../../src/context/data"
import { EditorContextProvider } from "../../src/context/editor"
import { ExitProvider } from "../../src/context/exit"
import { KVProvider } from "../../src/context/kv"
import { LocalProvider } from "../../src/context/local"
import { LocationProvider } from "../../src/context/location"
import { PermissionProvider } from "../../src/context/permission"
import { ProjectProvider } from "../../src/context/project"
import { RouteProvider } from "../../src/context/route"
import { SDKProvider, useSDK } from "../../src/context/sdk"
import { SyncProvider, useSync } from "../../src/context/sync"
import { ThemeProvider } from "../../src/context/theme"
import { TuiConfigProvider, type Info } from "../../src/config"
import { OpencodeKeymapProvider, registerOpencodeKeymap } from "../../src/keymap"
import { FrecencyProvider } from "../../src/prompt/frecency"
import { PromptHistoryProvider, usePromptHistory, type PromptInfo } from "../../src/prompt/history"
import { PromptStashProvider, usePromptStash } from "../../src/prompt/stash"
import { combineQueuedPrompts } from "../../src/prompt/queue"
import { DialogProvider } from "../../src/ui/dialog"
import { ToastProvider, useToast } from "../../src/ui/toast"
import { displaySlice } from "../../src/prompt/display"
import { tmpdir } from "../fixture/fixture"
import { TestTuiContexts } from "../fixture/tui-environment"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"

async function wait(check: () => boolean | Promise<boolean>) {
  for (let count = 0; count < 200; count++) {
    if (await check()) return
    await Bun.sleep(5)
  }
  throw new Error("Timed out waiting for TUI")
}

async function mount(root: string, options: { config?: Info; recovered?: PromptInfo } = {}) {
  await Bun.write(`${root}/kv.json`, "{}")
  let ref: PromptRef | undefined
  let sdk: ReturnType<typeof useSDK>
  let stash: ReturnType<typeof usePromptStash>
  let history: ReturnType<typeof usePromptHistory>
  let toast: ReturnType<typeof useToast>
  const [session, setSession] = createSignal("ses_test")
  const [visible, setVisible] = createSignal(true)
  const requests: {
    path: string
    body?: SessionPromptPromptInput
    requestID?: string
    reply(value: Response): void
    fail(error: Error): void
  }[] = []
  const provider = {
    id: "test",
    name: "Test",
    models: { test: { id: "test", name: "Test", limit: { context: 1000 } } },
  }
  const responses: Record<string, unknown> = {
    "/config": {},
    "/config/providers": { providers: [provider], default: { test: "test" } },
    "/provider": { all: [provider], connected: ["test"], default: { test: "test" } },
    "/agent": [{ name: "build", mode: "primary", permission: [], options: {} }],
    "/path": { directory: root, worktree: root, home: root, state: root, config: root },
    "/project/current": { id: "project", worktree: root },
    "/session/status": { ses_test: { type: "busy" } },
    "/mcp": {},
    "/experimental/resource": {},
  }
  const requestFetch = (async (request: Request) => {
    const pathname = new URL(request.url).pathname
    if (request.method === "POST" && pathname.startsWith("/session/")) {
      const withdrawal = pathname.endsWith("/queue/withdraw")
      const requestID = withdrawal ? (await request.json()).requestID : undefined
      if (withdrawal && (typeof requestID !== "string" || !requestID.length || requestID.length > 128))
        return Response.json({ message: "requestID is required" }, { status: 400 })
      const body =
        pathname.endsWith("/prompt_async") || pathname.endsWith("/message") ? await request.json() : undefined
      return new Promise<Response>((reply, fail) => requests.push({ path: pathname, body, requestID, reply, fail }))
    }
    if (pathname === "/api/location") return Response.json({ directory: root })
    if (pathname.startsWith("/api/")) return Response.json({ data: [], location: { directory: root } })
    return Response.json(responses[pathname] ?? [])
  }) as typeof fetch

  function Content() {
    sdk = useSDK()
    stash = usePromptStash()
    if (options.recovered) stash.push(options.recovered)
    history = usePromptHistory()
    toast = useToast()
    const sync = useSync()
    sync.set("session_status", "ses_test", { type: "busy" })
    return (
      <Show when={visible()}>
        <Prompt sessionID={session()} ref={(value) => (ref = value)} />
      </Show>
    )
  }

  function Harness() {
    const renderer = useRenderer()
    const keymap = createDefaultOpenTuiKeymap(renderer)
    const config = createTuiResolvedConfig(options.config ?? { prompt: { queue_edit: true } })
    onCleanup(registerOpencodeKeymap(keymap, renderer, config))
    const providers: ((props: ParentProps) => JSX.Element)[] = [
      (props) => (
        <TestTuiContexts directory={root} paths={{ home: root, state: root, worktree: root }}>
          {props.children}
        </TestTuiContexts>
      ),
      (props) => (
        <ExitProvider
          exit={(error) => {
            throw error
          }}
        >
          {props.children}
        </ExitProvider>
      ),
      (props) => <OpencodeKeymapProvider keymap={keymap}>{props.children}</OpencodeKeymapProvider>,
      ArgsProvider,
      KVProvider,
      ToastProvider,
      RouteProvider,
      (props) => <TuiConfigProvider config={config}>{props.children}</TuiConfigProvider>,
      (props) => (
        <SDKProvider
          url="http://localhost:4096"
          directory={root}
          fetch={requestFetch}
          events={{ subscribe: async () => () => {} }}
        >
          {props.children}
        </SDKProvider>
      ),
      PermissionProvider,
      ProjectProvider,
      SyncProvider,
      DataProvider,
      (props) => <ThemeProvider mode="dark">{props.children}</ThemeProvider>,
      LocalProvider,
      PromptStashProvider,
      DialogProvider,
      FrecencyProvider,
      PromptHistoryProvider,
      EditorContextProvider,
      LocationProvider,
    ]
    const Tree = providers.reduceRight(
      (Child, Provider) => () => (
        <Provider>
          <Child />
        </Provider>
      ),
      Content,
    )
    return <Tree />
  }
  const app = await testRender(() => <Harness />, { kittyKeyboard: true })
  await wait(() => !!ref?.focused)
  await Bun.sleep(20)
  return {
    app,
    requests,
    setSession,
    setVisible,
    get ref() {
      return ref!
    },
    get stash() {
      return stash
    },
    get history() {
      return history
    },
    get queue() {
      return sdk.prompts
    },
    get toast() {
      return toast.currentToast
    },
    get input() {
      const input = app.renderer.currentFocusedEditor
      if (!(input instanceof TextareaRenderable)) throw new Error("Expected prompt textarea")
      return input
    },
    close() {
      ref?.reset()
      app.renderer.destroy()
    },
  }
}

const queued: SessionPromptPromptInput[] = [
  {
    sessionID: "ses_test",
    parts: [
      { type: "text", text: "first [Image 1]" },
      {
        type: "file",
        mime: "image/png",
        url: "data:image/png;base64,AAA",
        source: { type: "file", path: "", text: { value: "[Image 1]", start: 6, end: 15 } },
      },
    ],
  },
  { sessionID: "ses_test", parts: [{ type: "text", text: "second" }] },
]

test.each(["omitted", "empty", "false"])("queue_edit %s preserves history and normal submission", async (setting) => {
  await using tmp = await tmpdir()
  const config: Info = setting === "omitted" ? {} : { prompt: setting === "empty" ? {} : { queue_edit: false } }
  const h = await mount(tmp.path, { config })
  try {
    h.history.append({ input: "old history", parts: [] })
    h.app.mockInput.pressArrow("up")
    await wait(() => h.ref.current.input === "old history")
    expect(h.requests).toHaveLength(0)
    h.ref.reset()
    h.input.insertText("pending prompt")
    h.ref.submit()
    await wait(() => h.requests.length === 1)
    expect(h.requests[0].path).toBe("/session/ses_test/message")
    h.app.mockInput.pressArrow("up")
    await wait(() => h.ref.current.input === "pending prompt")
    expect(h.requests).toHaveLength(1)
    h.requests[0].reply(Response.json({}))
  } finally {
    h.close()
  }
})

test.each([400, 404, 500])(
  "enabled withdrawal error %s does not recall history or replace the input",
  async (status) => {
    await using tmp = await tmpdir()
    const h = await mount(tmp.path)
    try {
      h.history.append({ input: "old history", parts: [] })
      h.app.mockInput.pressArrow("up")
      await wait(() => h.requests.length === 1)
      h.requests[0].reply(Response.json({ message: "withdraw failed" }, { status }))
      await wait(() => h.toast?.title === "Failed to withdraw queued prompts")
      expect(h.ref.current.input).toBe("")
      expect(h.stash.list()).toHaveLength(0)
      expect(h.queue.busy("ses_test")).toBe(false)
      h.app.mockInput.pressArrow("up")
      await wait(() => h.requests.length === 2)
      expect(h.requests[1].requestID).toBe(h.requests[0].requestID)
      h.requests[1].reply(Response.json([]))
      await wait(() => !h.queue.busy("ses_test"))
      expect(h.ref.current.input).toBe("")
      h.app.mockInput.pressArrow("up")
      await wait(() => h.requests.length === 3)
      expect(h.requests[2].requestID).not.toBe(h.requests[1].requestID)
      h.requests[2].reply(Response.json([]))
      await wait(() => h.ref.current.input === "old history")
    } finally {
      h.close()
    }
  },
)

test("enabled withdrawal follows the configured previous-history binding and ignores nonempty drafts", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path, {
    config: { prompt: { queue_edit: true }, keybinds: { history_previous: "ctrl+y" } },
  })
  try {
    h.app.mockInput.pressArrow("up")
    await Bun.sleep(20)
    expect(h.requests).toHaveLength(0)
    h.input.insertText("keep this draft")
    h.input.cursorOffset = 0
    h.app.mockInput.pressKey("y", { ctrl: true })
    await Bun.sleep(20)
    expect(h.ref.current.input).toBe("keep this draft")
    expect(h.requests).toHaveLength(0)
    h.ref.reset()
    await Bun.sleep(20)
    h.app.mockInput.pressKey("y", { ctrl: true })
    await wait(() => h.requests.length === 1)
    h.requests[0].reply(Response.json(queued))
    await wait(() => h.ref.current.input === "first [Image 1]\n\nsecond")
  } finally {
    h.close()
  }
})

test("empty Up withdraws, real extmarks survive editing, Enter submits once without interrupting", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    h.history.append({ input: "old history", parts: [] })
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 1)
    h.app.mockInput.pressArrow("up")
    expect(h.requests[0].path).toBe("/session/ses_test/queue/withdraw")
    h.requests[0].reply(Response.json(queued))
    await wait(() => h.ref.current.input === "first [Image 1]\n\nsecond")
    expect(h.ref.current.parts).toHaveLength(1)
    h.input.cursorOffset = 0
    h.input.insertText("edit ")
    await wait(() => h.ref.current.input.startsWith("edit "))
    h.app.mockInput.pressEnter()
    h.app.mockInput.pressEnter()
    await wait(() => h.requests.length === 2)
    expect(h.requests[1].path).toBe("/session/ses_test/prompt_async")
    const body = h.requests[1].body!
    const text = body.parts.find((part) => part.type === "text")!
    const file = body.parts.find((part) => part.type === "file")!
    expect(text.text).toBe("edit first [Image 1]\n\nsecond")
    expect(displaySlice(text.text, file.source!.text.start, file.source!.text.end)).toBe("[Image 1]")
    expect(body.parts.filter((part) => part.type === "file")).toHaveLength(1)
    h.requests[1].reply(Response.json({}))
    await Bun.sleep(20)
    expect(h.requests).toHaveLength(2)
    expect(h.ref.current.input).toBe("")
  } finally {
    h.close()
  }
})

test("history fallback and autocomplete retain their keymap precedence", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    h.history.append({ input: "old history", parts: [] })
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 1)
    expect(h.ref.current.input).toBe("")
    h.requests[0].reply(Response.json([]))
    await wait(() => h.ref.current.input === "old history")
    h.ref.reset()
    h.input.insertText("@")
    await Bun.sleep(30)
    h.app.mockInput.pressArrow("up")
    await Bun.sleep(20)
    expect(h.ref.current.input).toBe("@")
    expect(h.requests).toHaveLength(1)
  } finally {
    h.close()
  }
})

test("duplicate attachment markers and hidden context survive real extmark synchronization in part order", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    const hidden = { type: "text" as const, text: "editor context", synthetic: true }
    const payload = structuredClone(queued)
    payload[0].parts.unshift(hidden)
    payload[1].parts = [
      { type: "text", text: "[Image 1] @build" },
      { type: "agent", name: "build", source: { value: "@build", start: 10, end: 16 } },
      {
        type: "file",
        mime: "image/png",
        url: "data:image/png;base64,AAA",
        source: { type: "file", path: "", text: { value: "[Image 1]", start: 0, end: 9 } },
      },
    ]
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 1)
    h.requests[0].reply(Response.json(payload))
    await wait(() => h.ref.current.parts.length === 4)
    h.input.cursorOffset = 0
    h.input.insertText("prefix ")
    h.ref.submit()
    await wait(() => h.requests.length === 2)
    const parts = h.requests[1].body!.parts
    expect(parts.map((part) => part.type)).toEqual(["text", "text", "file", "agent", "file"])
    expect(parts[1]).toEqual(hidden)
    const text = parts[0]
    if (text.type !== "text") throw new Error("Expected editable text")
    for (const part of parts) {
      if (part.type !== "agent" && part.type !== "file") continue
      const source = part.type === "agent" ? part.source! : part.source!.text
      expect(displaySlice(text.text, source.start, source.end)).toBe(source.value)
    }
    h.requests[1].reply(new Response(null, { status: 204 }))
  } finally {
    h.close()
  }
})

test.each(["typing", "typing-erased", "session", "session-returned", "unmount", "shutdown"])(
  "pending withdrawal safely saves data after %s",
  async (change) => {
    await using tmp = await tmpdir()
    const h = await mount(tmp.path)
    try {
      h.app.mockInput.pressArrow("up")
      await wait(() => h.requests.length === 1)
      if (change === "typing" || change === "typing-erased") {
        h.input.insertText("new draft")
        h.app.mockInput.pressEnter()
        if (change === "typing-erased") h.ref.reset()
      }
      if (change === "session" || change === "session-returned") h.setSession("ses_other")
      if (change === "session-returned") h.setSession("ses_test")
      if (change === "unmount") h.setVisible(false)
      if (change === "shutdown") h.app.renderer.destroy()
      h.requests[0].reply(Response.json(queued))
      await wait(() => h.stash.list().length === 1)
      expect(h.stash.list()[0].input).toBe("first [Image 1]\n\nsecond")
      expect(h.stash.list()[0].parts).toHaveLength(1)
      expect(h.requests).toHaveLength(1)
      if (change === "typing") expect(h.ref.current.input).toBe("new draft")
      if (change === "session") expect(h.ref.current.input).toBe("")
    } finally {
      h.close()
    }
  },
)

test("recovery during stash startup and overlapping writes retain the newest snapshot", async () => {
  await using tmp = await tmpdir()
  const file = Bun.file(`${tmp.path}/prompt-stash.jsonl`)
  await Bun.write(file, JSON.stringify({ input: "older draft", parts: [], timestamp: 1 }) + "\n")
  const h = await mount(tmp.path, { recovered: combineQueuedPrompts(queued) })
  try {
    expect(h.stash.list().map((entry) => entry.input)).toEqual(["older draft", "first [Image 1]\n\nsecond"])
    h.stash.push({ input: "temporary", parts: [] })
    h.stash.pop()
    h.stash.push({ input: "newest", parts: [] })
    await wait(async () => {
      const lines = (await file.text()).trim().split("\n")
      return lines.length === 3 && lines.at(-1)?.includes('"input":"newest"') === true
    })
    expect(h.stash.list().map((entry) => entry.input)).toEqual(["older draft", "first [Image 1]\n\nsecond", "newest"])
    expect(h.stash.list()[1].parts).toHaveLength(1)
  } finally {
    h.close()
  }
})

test("an earlier deferred Enter cannot automatically resubmit the withdrawn draft", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    h.app.mockInput.pressEnter()
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 1)
    h.requests[0].reply(Response.json(queued))
    await wait(() => h.ref.current.input === "first [Image 1]\n\nsecond")
    await Bun.sleep(20)
    expect(h.requests).toHaveLength(1)
  } finally {
    h.close()
  }
})

test("Up immediately after Enter waits for admission, but not the active turn", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    h.input.insertText("queued locally")
    h.ref.submit()
    await wait(() => h.requests.length === 1)
    h.app.mockInput.pressArrow("up")
    await Bun.sleep(20)
    expect(h.queue.busy("ses_test")).toBe(true)
    expect(h.requests).toHaveLength(1)
    h.requests[0].reply(new Response(null, { status: 204 }))
    await wait(() => h.requests.length === 2)
    h.requests[1].reply(Response.json([{ ...h.requests[0].body!, sessionID: "ses_test" }]))
    await wait(() => h.ref.current.input === "queued locally")
    expect(h.requests.map((item) => item.path)).toEqual([
      "/session/ses_test/prompt_async",
      "/session/ses_test/queue/withdraw",
    ])
  } finally {
    h.close()
  }
})

test("Up replays a committed lost response after remount and leaves a newer admission for the next action", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 1)
    const receipt = structuredClone(queued)
    const requestID = h.requests[0].requestID
    h.requests[0].fail(new TypeError("response lost after commit"))
    await wait(() => !h.queue.busy("ses_test"))
    expect(h.ref.current.input).toBe("")
    h.setVisible(false)
    h.setVisible(true)
    await wait(() => h.ref?.focused)

    h.input.insertText("newer admission")
    h.ref.submit()
    await wait(() => h.requests.length === 2)
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 3)
    expect(h.requests[2].requestID).toBe(requestID)
    h.requests[2].reply(Response.json(receipt))
    await wait(() => h.ref.current.input === "first [Image 1]\n\nsecond")
    expect(h.ref.current.parts).toHaveLength(1)

    h.ref.reset()
    await Bun.sleep(20)
    h.app.mockInput.pressArrow("up")
    await Bun.sleep(20)
    expect(h.requests).toHaveLength(3)
    h.requests[1].reply(new Response(null, { status: 204 }))
    await wait(() => h.requests.length === 4)
    expect(h.requests[3].requestID).not.toBe(requestID)
    h.requests[3].reply(Response.json([{ ...h.requests[1].body!, sessionID: "ses_test" }]))
    await wait(() => h.ref.current.input === "newer admission")
  } finally {
    h.close()
  }
})

test("switching sessions cannot replace another session's retry identity", async () => {
  await using tmp = await tmpdir()
  const h = await mount(tmp.path)
  try {
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 1)
    const firstID = h.requests[0].requestID
    h.requests[0].fail(new TypeError("first lost response"))
    await wait(() => !h.queue.busy("ses_test"))
    h.setSession("ses_other")
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 2)
    const otherID = h.requests[1].requestID
    expect(otherID).not.toBe(firstID)
    h.requests[1].fail(new TypeError("other lost response"))
    await wait(() => !h.queue.busy("ses_other"))

    h.setSession("ses_test")
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 3)
    expect(h.requests[2].path).toBe("/session/ses_test/queue/withdraw")
    expect(h.requests[2].requestID).toBe(firstID)
    h.requests[2].reply(Response.json(queued))
    await wait(() => h.ref.current.input === "first [Image 1]\n\nsecond")

    h.ref.reset()
    h.setSession("ses_other")
    await Bun.sleep(20)
    h.app.mockInput.pressArrow("up")
    await wait(() => h.requests.length === 4)
    expect(h.requests[3].path).toBe("/session/ses_other/queue/withdraw")
    expect(h.requests[3].requestID).toBe(otherID)
    h.requests[3].reply(Response.json([{ sessionID: "ses_other", parts: [{ type: "text", text: "other session" }] }]))
    await wait(() => h.ref.current.input === "other session")
  } finally {
    h.close()
  }
})
