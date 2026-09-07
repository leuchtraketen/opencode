import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { eq } from "drizzle-orm"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect, spyOn } from "bun:test"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer } from "effect"
import path from "path"
import { fileURLToPath } from "url"
import { NamedError } from "@opencode-ai/core/util/error"
import { Agent as AgentSvc } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Command } from "../../src/command"
import { Config } from "@/config/config"
import { LSP } from "@/lsp/lsp"
import { MCP } from "../../src/mcp"
import { Permission } from "../../src/permission"
import { Plugin } from "../../src/plugin"
import { Provider as ProviderSvc } from "@/provider/provider"
import { Env } from "../../src/env"
import { Git } from "../../src/git"
import { Image } from "../../src/image/image"

import { Question } from "../../src/question"
import { Todo } from "../../src/session/todo"
import { Session } from "@/session/session"
import { SessionMessageTable } from "@opencode-ai/core/session/sql"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { SessionCompaction } from "../../src/session/compaction"
import { SessionSummary } from "../../src/session/summary"
import { Instruction } from "../../src/session/instruction"
import { SessionProcessor } from "../../src/session/processor"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionRevert } from "../../src/session/revert"
import { SessionRunState } from "../../src/session/run-state"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { Skill } from "../../src/skill"
import { SystemPrompt } from "../../src/session/system"
import { Shell } from "@opencode-ai/core/shell"
import { Snapshot } from "../../src/snapshot"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Format } from "../../src/format"
import { reloadInstance, TestInstance } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { LocationServiceMap, locationServiceMapLayer } from "@opencode-ai/core/location-services"

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

function withSh<A, E, R>(fx: () => Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const prev = process.env.SHELL
      process.env.SHELL = "/bin/sh"
      Shell.preferred.reset()
      return prev
    }),
    () => fx(),
    (prev) =>
      Effect.sync(() => {
        if (prev === undefined) delete process.env.SHELL
        else process.env.SHELL = prev
        Shell.preferred.reset()
      }),
  )
}

function toolPart(parts: SessionV1.Part[]) {
  return parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
}

type CompletedToolPart = SessionV1.ToolPart & { state: SessionV1.ToolStateCompleted }
type ErrorToolPart = SessionV1.ToolPart & { state: SessionV1.ToolStateError }

function completedTool(parts: SessionV1.Part[]) {
  const part = toolPart(parts)
  expect(part?.state.status).toBe("completed")
  return part?.state.status === "completed" ? (part as CompletedToolPart) : undefined
}

function errorTool(parts: SessionV1.Part[]) {
  const part = toolPart(parts)
  expect(part?.state.status).toBe("error")
  return part?.state.status === "error" ? (part as ErrorToolPart) : undefined
}

function makeMcp(instructions: MCP.ServerInstructions[] = []) {
  return Layer.succeed(
    MCP.Service,
    MCP.Service.of({
      status: () => Effect.succeed({}),
      clients: () => Effect.succeed({}),
      instructions: () => Effect.succeed(instructions),
      tools: () => Effect.succeed({}),
      prompts: () => Effect.succeed({}),
      resources: () => Effect.succeed({}),
      resourceTemplates: () => Effect.succeed({}),
      add: () => Effect.succeed({ status: { status: "disabled" as const } }),
      connect: () => Effect.void,
      disconnect: () => Effect.void,
      getPrompt: () => Effect.succeed(undefined),
      readResource: () => Effect.succeed(undefined),
      startAuth: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      authenticate: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      finishAuth: () => Effect.die("unexpected MCP auth in prompt-effect tests"),
      removeAuth: () => Effect.void,
      supportsOAuth: () => Effect.succeed(false),
      hasStoredTokens: () => Effect.succeed(false),
      getAuthStatus: () => Effect.succeed("not_authenticated" as const),
    }),
  )
}

const lsp = Layer.succeed(
  LSP.Service,
  LSP.Service.of({
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  }),
)

const processorCreateStarted: Array<() => void> = []
const blockingProcessor = Layer.succeed(
  SessionProcessor.Service,
  SessionProcessor.Service.of({
    create: () => Effect.sync(() => processorCreateStarted.shift()?.()).pipe(Effect.andThen(Effect.never)),
  }),
)

const runtimeFlags = RuntimeFlags.layer({ experimentalEventSystem: true })

const testLLMServerNode = LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })

const promptRoot = LayerNode.group([
  SessionPrompt.node,
  Session.node,
  SessionProjector.node,
  MessageV2.node,
  Snapshot.node,
  LLM.node,
  Env.node,
  AgentSvc.node,
  Command.node,
  Permission.node,
  Plugin.node,
  Config.node,
  ProviderSvc.node,
  LSP.node,
  MCP.node,
  FSUtil.node,
  BackgroundJob.node,
  SessionStatus.node,
  SessionRunState.node,
  Database.node,
  EventV2Bridge.node,
  Question.node,
  Todo.node,
  ToolRegistry.node,
  Skill.node,
  Git.node,
  Ripgrep.node,
  Format.node,
  Truncate.node,
  SessionProcessor.node,
  Image.node,
  SessionCompaction.node,
  SessionRevert.node,
  Instruction.node,
  SystemPrompt.node,
  CrossSpawnSpawner.node,
  RuntimeFlags.node,
])

function makePrompt(input?: { mcpInstructions?: MCP.ServerInstructions[]; processor?: "blocking" }) {
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [RuntimeFlags.node, runtimeFlags],
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(promptRoot, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(promptRoot, replacements)
}

function makeHttp(input?: { mcpInstructions?: MCP.ServerInstructions[]; processor?: "blocking" }) {
  const root = LayerNode.group([promptRoot, testLLMServerNode])
  const replacements = [
    [SessionSummary.node, summary],
    [LSP.node, lsp],
    [MCP.node, makeMcp(input?.mcpInstructions)],
    [RuntimeFlags.node, runtimeFlags],
  ] as const
  if (input?.processor === "blocking") {
    return LayerNode.compile(root, [...replacements, [SessionProcessor.node, blockingProcessor]])
  }
  return LayerNode.compile(root, replacements)
}

function makeHttpNoLLMServer(input?: { mcpInstructions?: MCP.ServerInstructions[]; processor?: "blocking" }) {
  return makePrompt(input)
}

const it = testEffect(makeHttp())
const noLLMServer = testEffect(makeHttpNoLLMServer())
const raceNoLLMServer = testEffect(makeHttpNoLLMServer({ processor: "blocking" }))
const withMcpInstructions = testEffect(
  makeHttp({
    mcpInstructions: [
      {
        name: "guide-server",
        instructions: "Use lookup before mutate.",
        tools: ["guide-server_lookup"],
      },
    ],
  }),
)
const unix = process.platform !== "win32" ? it.instance : it.instance.skip
const unixNoLLMServer = process.platform !== "win32" ? noLLMServer.instance : noLLMServer.instance.skip

// Config that registers a custom "test" provider with a "test-model" model
// so provider model lookup succeeds inside the loop.
const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: {
        ...cfg.provider.test,
        options: {
          ...cfg.provider.test.options,
          baseURL: url,
        },
      },
    },
  }
}

const writeText = Effect.fn("test.writeText")(function* (file: string, text: string) {
  const fs = yield* FSUtil.Service
  yield* fs.writeWithDirs(file, text)
})

const writeConfig = Effect.fn("test.writeConfig")(function* (dir: string, config: Partial<ConfigV1.Info>) {
  yield* writeText(
    path.join(dir, "opencode.json"),
    JSON.stringify({ $schema: "https://opencode.ai/config.json", ...config }),
  )
})

const useServerConfig = Effect.fn("test.useServerConfig")(function* (config: (url: string) => Partial<ConfigV1.Info>) {
  const { directory: dir } = yield* TestInstance
  const llm = yield* TestLLMServer
  yield* writeConfig(dir, config(llm.url))
  return { dir, llm }
})

// Wait for a session's runner to enter a busy state. SessionStatus is flipped
// inside Runner.startShell's serialized transition, so cancel can't no-op once
// we observe it.
const waitForBusy = (sessionID: SessionID, duration: Duration.Input = "2 seconds") =>
  pollWithTimeout(
    Effect.gen(function* () {
      const status = yield* SessionStatus.Service
      const s = yield* status.get(sessionID)
      return s.type === "busy" ? (true as const) : undefined
    }),
    `session ${sessionID} never became busy`,
    duration,
  )

const enqueue = Effect.fnUntraced(function* (input: SessionPrompt.PromptInput) {
  const prompt = yield* SessionPrompt.Service
  const admitted = yield* Deferred.make<
    void,
    Image.Error | SessionPrompt.PromptConflictError | SessionPrompt.PromptAbandonedError
  >()
  const fiber = yield* prompt.prompt(input, { admitted }).pipe(Effect.forkChild)
  yield* awaitWithTimeout(Deferred.await(admitted), "prompt was not admitted")
  return fiber
})

const visible = (messageID: MessageID) =>
  pollWithTimeout(
    MessageV2.parts(messageID).pipe(Effect.map((parts) => (parts.length ? true : undefined))),
    "prepared prompt was not published",
  )

it.instance("withdraw returns every queued original payload once without aborting the provider turn", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const status = yield* SessionStatus.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const release = yield* Deferred.make<void>()
    yield* llm.push(reply().wait(deferredAsPromise(release)).text("active work completed").stop())
    const active = yield* prompt
      .prompt({
        sessionID: chat.id,
        model: ref,
        parts: [{ type: "text", text: "active work" }],
      })
      .pipe(Effect.forkChild)
    yield* llm.wait(1)

    const photon = yield* Effect.promise(() => import("@silvia-odwyer/photon-node"))
    const source = new photon.PhotonImage(new Uint8Array(9_000 * 4).fill(255), 9_000, 1)
    const image = {
      type: "file" as const,
      mime: "image/png",
      filename: "original.png",
      url: `data:image/png;base64,${Buffer.from(source.get_bytes()).toString("base64")}`,
    }
    source.free()
    const ids = [MessageID.ascending(), MessageID.ascending(), MessageID.ascending()].reverse()
    const inputs: SessionPrompt.PromptInput[] = [
      {
        sessionID: chat.id,
        messageID: ids[0],
        model: ref,
        agent: "build",
        parts: [
          { type: "text", text: "first @notes.txt @explore" },
          {
            id: PartID.ascending(),
            type: "file",
            mime: "text/plain",
            filename: "notes.txt",
            url: "data:text/plain;base64,b3JpZ2luYWwgZmlsZQ==",
            source: { type: "file", path: "/original/notes.txt", text: { value: "@notes.txt", start: 6, end: 16 } },
          },
          { type: "agent", name: "explore", source: { value: "@explore", start: 17, end: 25 } },
        ],
      },
      {
        sessionID: chat.id,
        messageID: ids[1],
        model: ref,
        parts: [
          { type: "text", text: "second queued prompt" },
          {
            type: "file",
            mime: "application/pdf",
            url: "data:application/pdf;base64,cGRm",
            filename: "attachment.pdf",
          },
        ],
      },
      {
        sessionID: chat.id,
        messageID: ids[2],
        model: ref,
        parts: [{ type: "text", text: "third queued prompt" }, image],
      },
    ]
    const queued = []
    for (const input of inputs) {
      queued.push(yield* enqueue(input))
      yield* visible(input.messageID!)
    }
    expect(
      (yield* sessions.messages({ sessionID: chat.id })).filter((message) => message.info.role === "user"),
    ).toHaveLength(4)
    const results = yield* Effect.all(
      [prompt.withdraw(chat.id, "client-one"), prompt.withdraw(chat.id, "client-two")],
      {
        concurrency: "unbounded",
      },
    )
    expect(results.flat()).toEqual(inputs)
    expect(results.filter((result) => result.length === 0)).toHaveLength(1)
    expect(yield* status.get(chat.id)).toEqual({ type: "busy" })
    expect(yield* llm.calls).toBe(1)
    expect(
      (yield* sessions.messages({ sessionID: chat.id })).filter((message) => message.info.role === "user"),
    ).toHaveLength(1)
    expect(yield* prompt.withdraw(chat.id, "empty")).toEqual([])

    yield* Deferred.succeed(release, undefined)
    const result = yield* awaitWithTimeout(Fiber.join(active), "withdraw interrupted active work", "10 seconds")
    yield* Effect.forEach(queued, Fiber.join)
    expect(result.parts.some((part) => part.type === "text" && part.text === "active work completed")).toBe(true)
    expect(yield* llm.calls).toBe(1)
    expect(JSON.stringify(yield* llm.inputs)).not.toContain("queued prompt")
    const retry = yield* prompt.prompt(inputs[0]).pipe(Effect.exit)
    expect(Exit.isFailure(retry)).toBe(true)
    if (Exit.isFailure(retry)) expect(Cause.squash(retry.cause)).toBeInstanceOf(SessionPrompt.PromptConflictError)
    expect(yield* prompt.withdraw(chat.id, "empty")).toEqual([])
  }),
)

it.instance("withdraw can remove the first prepared prompt before the runner takes its snapshot", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2Bridge.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const input: SessionPrompt.PromptInput = {
      sessionID: chat.id,
      messageID: MessageID.ascending(),
      model: ref,
      parts: [{ type: "text", text: "withdraw before snapshot" }],
    }
    let withdrawn: SessionPrompt.PromptInput[] = []
    const off = yield* events.listen((event) => {
      if (event.type !== SessionStatus.Event.Status.type) return Effect.void
      const data = event.data as typeof SessionStatus.Event.Status.data.Type
      if (data.sessionID !== chat.id || data.status.type !== "busy") return Effect.void
      return prompt.withdraw(chat.id, "before-snapshot").pipe(
        Effect.tap((inputs) =>
          Effect.sync(() => {
            withdrawn = inputs
          }),
        ),
        Effect.asVoid,
      )
    })
    yield* Effect.addFinalizer(() => off)
    yield* prompt.prompt(input)
    expect(withdrawn).toEqual([input])
    expect(yield* llm.calls).toBe(0)
    expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
  }),
)

it.instance("withdraw excludes followups already selected by the next provider turn", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const first = yield* Deferred.make<void>()
    const second = yield* Deferred.make<void>()
    yield* llm.push(
      reply().wait(deferredAsPromise(first)).text("first response").stop(),
      reply().wait(deferredAsPromise(second)).text("second response").stop(),
    )
    const active = yield* prompt
      .prompt({ sessionID: chat.id, model: ref, parts: [{ type: "text", text: "first" }] })
      .pipe(Effect.forkChild)
    yield* llm.wait(1)
    const input = {
      sessionID: chat.id,
      messageID: MessageID.ascending(),
      model: ref,
      parts: [{ type: "text" as const, text: "next-turn followup" }],
    }
    const followup = yield* enqueue(input)
    yield* Deferred.succeed(first, undefined)
    yield* llm.wait(2)
    expect(JSON.stringify((yield* llm.inputs)[1])).toContain("next-turn followup")
    expect(yield* prompt.withdraw(chat.id, "claimed")).toEqual([])
    yield* Deferred.succeed(second, undefined)
    yield* Fiber.join(active)
    yield* Fiber.join(followup)
    expect(yield* prompt.withdraw(chat.id, "after")).toEqual([])
    expect(
      (yield* sessions.messages({ sessionID: chat.id })).some((message) => message.info.id === input.messageID),
    ).toBe(true)
  }),
)

for (const fail of [false, true])
  it.instance(`withdraw includes in-flight file resolution without late publication${fail ? " or errors" : ""}`, () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const registry = yield* ToolRegistry.Service
      const events = yield* EventV2Bridge.Service
      const errors: unknown[] = []
      yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type === Session.Event.Error.type) errors.push(event.data)
        }),
      )
      const { read } = yield* registry.named()
      const original = read.execute
      const resolving = yield* Deferred.make<void>()
      const finish = yield* Deferred.make<void>()
      read.execute = (args, ctx) =>
        Deferred.succeed(resolving, undefined).pipe(
          Effect.andThen(Deferred.await(finish)),
          Effect.andThen(fail ? Effect.die(new Error("late read failure")) : original(args, ctx)),
        )
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          read.execute = original
        }),
      )
      const chat = yield* sessions.create({ title: "Pinned" })
      const release = yield* Deferred.make<void>()
      yield* llm.push(reply().wait(deferredAsPromise(release)).text("done").stop())
      const active = yield* prompt
        .prompt({ sessionID: chat.id, model: ref, parts: [{ type: "text", text: "active" }] })
        .pipe(Effect.forkChild)
      yield* llm.wait(1)
      yield* writeText(path.join(dir, "slow.txt"), "file content")
      const slow: SessionPrompt.PromptInput = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [
          { type: "text", text: "slow first" },
          { type: "file", mime: "text/plain", url: `file://${path.join(dir, "slow.txt")}`, filename: "slow.txt" },
        ],
      }
      const slowFiber = yield* prompt.prompt(slow).pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(resolving), "file resolution never started", "10 seconds")
      const fast: SessionPrompt.PromptInput = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [{ type: "text", text: "fast second" }],
      }
      const fastFiber = yield* enqueue(fast)
      expect(yield* prompt.withdraw(chat.id, "both")).toEqual([slow, fast])
      yield* Deferred.succeed(finish, undefined)
      yield* awaitWithTimeout(Fiber.join(slowFiber), "withdrawn resolution did not finish", "10 seconds")
      expect(
        (yield* sessions.messages({ sessionID: chat.id })).some(
          (message) => message.info.id === slow.messageID || message.info.id === fast.messageID,
        ),
      ).toBe(false)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(active)
      yield* Fiber.join(fastFiber)
      expect(yield* llm.calls).toBe(1)
      expect(errors).toEqual([])
    }),
  )

noLLMServer.instance(
  "withdraw preserves raw input while a chat.message plugin is still resolving",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const plugin = yield* Plugin.Service
      const hooks = yield* plugin.list()
      const started = defer<void>()
      const release = defer<void>()
      const hook = {
        "chat.message": async (_input: unknown, output: { parts: Array<{ type: string; text?: string }> }) => {
          const text = output.parts.find((part) => part.type === "text")
          if (text) text.text = "plugin transformed text"
          started.resolve()
          await release.promise
        },
      }
      hooks.push(hook)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          hooks.splice(hooks.indexOf(hook), 1)
          release.resolve()
        }),
      )
      const chat = yield* sessions.create({ title: "Plugin race" })
      const events = yield* EventV2Bridge.Service
      let removals = 0
      yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type === SessionV1.Event.MessageRemoved.type) removals++
        }),
      )
      const input: SessionPrompt.PromptInput = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [{ type: "text", text: "original text" }],
      }
      const fiber = yield* prompt.prompt(input).pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Effect.promise(() => started.promise),
        "plugin did not start",
        "10 seconds",
      )
      const receipt = yield* prompt.withdraw(chat.id, "retry")
      expect(receipt).toEqual([input])
      Object.assign(receipt[0], { agent: "mutated receipt" })
      const later = { ...input, messageID: MessageID.ascending() }
      const laterFiber = yield* enqueue(later)
      expect(yield* prompt.withdraw(chat.id, "retry")).toEqual([input])
      expect(yield* prompt.withdraw(chat.id, "later")).toEqual([later])
      release.resolve()
      yield* Fiber.join(fiber)
      yield* Fiber.join(laterFiber)
      expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
      expect(yield* prompt.withdraw(chat.id, "empty")).toEqual([])
      expect(removals).toBe(0)
    }),
  { config: cfg },
)

noLLMServer.instance(
  "withdraw is session isolated and preserves noReply and synthetic messages",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const original = read.execute
      const finish = yield* Deferred.make<void>()
      read.execute = (args, ctx) => Deferred.await(finish).pipe(Effect.andThen(original(args, ctx)))
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          read.execute = original
        }),
      )
      const chats = [yield* sessions.create({ title: "One" }), yield* sessions.create({ title: "Two" })]
      yield* writeText(path.join(dir, "hold.txt"), "hold")
      const inputs = chats.map((chat) => ({
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [
          {
            type: "file" as const,
            mime: "text/plain",
            url: `file://${path.join(dir, "hold.txt")}`,
            filename: "hold.txt",
          },
        ],
      }))
      const fibers = yield* Effect.forEach(inputs, enqueue)
      const admitted = yield* Deferred.make<
        void,
        Image.Error | SessionPrompt.PromptConflictError | SessionPrompt.PromptAbandonedError
      >()
      const task = yield* prompt
        .prompt({ ...inputs[0], messageID: MessageID.ascending() }, { admitted, task: true })
        .pipe(Effect.forkChild)
      yield* Deferred.await(admitted)
      const noReply = yield* prompt.prompt({
        sessionID: chats[0].id,
        model: ref,
        noReply: true,
        parts: [{ type: "text", text: "keep noReply" }],
      })
      const synthetic = yield* user(chats[0].id, "synthetic history")
      const part = (yield* MessageV2.get({ sessionID: chats[0].id, messageID: synthetic.id })).parts[0]
      if (part.type === "text") yield* sessions.updatePart({ ...part, synthetic: true })
      const compaction = yield* SessionCompaction.Service
      yield* compaction.create({ sessionID: chats[0].id, agent: "build", model: ref, auto: true })
      const kept = yield* sessions.messages({ sessionID: chats[0].id })
      expect(yield* prompt.withdraw(chats[0].id, "one")).toEqual([inputs[0]])
      expect(yield* prompt.withdraw(chats[1].id, "two")).toEqual([inputs[1]])
      yield* Fiber.interrupt(task)
      yield* Deferred.succeed(finish, undefined)
      yield* Effect.forEach(fibers, Fiber.join)
      expect(yield* sessions.messages({ sessionID: chats[0].id })).toEqual(kept)
      expect(kept.some((message) => message.info.id === noReply.info.id)).toBe(true)
    }),
  { config: cfg },
)

const hasBash = Effect.sync(() => Bun.which("bash") !== null)

it.instance("claim applies original tool permissions before preparing the provider request", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const plugin = yield* Plugin.Service
    const hooks = yield* plugin.list()
    const hook = {
      "chat.message": async (_input: unknown, output: { message: { tools?: Record<string, boolean> } }) => {
        output.message.tools = { read: true }
      },
    }
    hooks.push(hook)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        hooks.splice(hooks.indexOf(hook), 1)
      }),
    )
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.text("done")
    yield* prompt.prompt({
      sessionID: chat.id,
      model: ref,
      tools: { read: false },
      parts: [{ type: "text", text: "keep read disabled" }],
    })
    expect((yield* sessions.get(chat.id)).permission).toEqual([{ permission: "read", action: "deny", pattern: "*" }])
    expect(JSON.stringify((yield* llm.inputs)[0].tools)).not.toContain('"name":"read"')
    expect(JSON.stringify((yield* llm.inputs)[0].tools)).toContain('"name":"bash"')
  }),
)

noLLMServer.instance(
  "cancel during preparation leaves the input withdrawable without starting a model",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const plugin = yield* Plugin.Service
      const hooks = yield* plugin.list()
      const started = defer<void>()
      const release = defer<void>()
      const hook = {
        "chat.message": async () => {
          started.resolve()
          await release.promise
        },
      }
      hooks.push(hook)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          release.resolve()
          hooks.splice(hooks.indexOf(hook), 1)
        }),
      )
      const chat = yield* sessions.create({ title: "Pinned" })
      const input = {
        sessionID: chat.id,
        model: ref,
        parts: [{ type: "text" as const, text: "cancelled preparation" }],
      }
      const fiber = yield* enqueue(input)
      yield* awaitWithTimeout(
        Effect.promise(() => started.promise),
        "preparation did not start",
      )
      yield* prompt.cancel(chat.id)
      release.resolve()
      yield* awaitWithTimeout(Fiber.join(fiber), "cancelled preparation started a model")
      expect(yield* prompt.withdraw(chat.id, "cancelled")).toEqual([input])
      expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
    }),
  { config: cfg },
)

noLLMServer.instance(
  "instance disposal discards admissions and receipts and prevents late publication",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const plugin = yield* Plugin.Service
      const hooks = yield* plugin.list()
      const release = defer<void>()
      const hook = {
        "chat.message": async () => {
          await release.promise
        },
      }
      hooks.push(hook)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          release.resolve()
          hooks.splice(hooks.indexOf(hook), 1)
        }),
      )
      const chat = yield* sessions.create({ title: "Pinned" })
      const input = { sessionID: chat.id, model: ref, parts: [{ type: "text" as const, text: "discarded" }] }
      const first = yield* enqueue(input)
      expect(yield* prompt.withdraw(chat.id, "receipt")).toEqual([input])
      const second = yield* enqueue(input)
      yield* reloadInstance({ directory: test.directory })
      expect(yield* prompt.withdraw(chat.id, "receipt")).toEqual([])
      release.resolve()
      yield* Fiber.join(first)
      yield* Fiber.join(second)
      expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
    }),
  { config: cfg },
)

it.instance("admission order survives slow preparation and nonmonotonic prompt and attachment IDs", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const plugin = yield* Plugin.Service
    const hooks = yield* plugin.list()
    const started = defer<void>()
    const release = defer<void>()
    const hook = {
      "chat.message": async (input: { messageID?: string }) => {
        if (input.messageID !== "msg_z_first") return
        started.resolve()
        await release.promise
      },
    }
    hooks.push(hook)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        release.resolve()
        hooks.splice(hooks.indexOf(hook), 1)
      }),
    )
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.text("done")
    const first = yield* prompt
      .prompt({
        sessionID: chat.id,
        messageID: MessageID.make("msg_z_first"),
        model: ref,
        parts: [
          { id: PartID.make("prt_z_first"), type: "text", text: "first text" },
          {
            id: PartID.make("prt_a_second"),
            type: "file",
            mime: "text/plain",
            filename: "note.txt",
            url: "data:text/plain,attachment content",
          },
          { id: PartID.make("prt_m_third"), type: "text", text: "last text" },
        ],
      })
      .pipe(Effect.forkChild)
    yield* awaitWithTimeout(
      Effect.promise(() => started.promise),
      "first preparation did not start",
    )
    const second = yield* enqueue({
      sessionID: chat.id,
      messageID: MessageID.make("msg_a_second"),
      model: ref,
      parts: [{ type: "text", text: "second prompt" }],
    })
    expect(yield* llm.calls).toBe(0)
    release.resolve()
    yield* Fiber.join(first)
    const result = yield* Fiber.join(second)
    expect(yield* llm.calls).toBe(1)
    expect(result.info.role === "assistant" && result.info.parentID).toBe(MessageID.make("msg_a_second"))
    const input = JSON.stringify((yield* llm.inputs)[0])
    expect(input.indexOf("first text")).toBeLessThan(input.indexOf("attachment content"))
    expect(input.indexOf("attachment content")).toBeLessThan(input.indexOf("last text"))
    expect(input.indexOf("last text")).toBeLessThan(input.indexOf("second prompt"))
    expect(yield* prompt.withdraw(chat.id, "claimed")).toEqual([])
  }),
)

for (const withdrawLater of [false, true])
  it.instance(
    `saved model follows ${withdrawLater ? "the active prompt, not a withdrawn followup" : "admission order despite delayed preparation"}`,
    () =>
      Effect.gen(function* () {
        const { llm } = yield* useServerConfig((url) => {
          const config = providerCfg(url)
          return {
            ...config,
            provider: {
              test: {
                ...config.provider.test,
                models: {
                  ...config.provider.test.models,
                  "test-model-b": { ...config.provider.test.models["test-model"], id: "test-model-b" },
                },
              },
            },
          }
        })
        const prompt = yield* SessionPrompt.Service
        const sessions = yield* Session.Service
        const plugin = yield* Plugin.Service
        const hooks = yield* plugin.list()
        const firstID = MessageID.ascending()
        const started = defer<void>()
        const prepare = defer<void>()
        const response = yield* Deferred.make<void>()
        const hook = {
          "chat.message": async (input: { messageID?: string }) => {
            if (input.messageID !== firstID) return
            started.resolve()
            await prepare.promise
          },
        }
        hooks.push(hook)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            prepare.resolve()
            hooks.splice(hooks.indexOf(hook), 1)
          }),
        )
        const chat = yield* sessions.create({ title: "Pinned" })
        const modelB = { providerID: ref.providerID, modelID: ModelV2.ID.make("test-model-b") }
        yield* llm.push(
          reply().wait(deferredAsPromise(response)).text("first response").stop(),
          reply().text("next response").stop(),
          reply().text("followup response").stop(),
        )
        const first = yield* enqueue({
          sessionID: chat.id,
          messageID: firstID,
          model: ref,
          parts: [{ type: "text", text: "slow model A" }],
        })
        yield* awaitWithTimeout(
          Effect.promise(() => started.promise),
          "model A preparation did not start",
          "10 seconds",
        )
        if (withdrawLater) {
          prepare.resolve()
          yield* llm.wait(1)
        }
        const input = {
          sessionID: chat.id,
          messageID: MessageID.ascending(),
          model: modelB,
          parts: [{ type: "text" as const, text: "fast model B" }],
        }
        const second = yield* enqueue(input)
        yield* visible(input.messageID)
        if (withdrawLater) {
          expect(yield* prompt.withdraw(chat.id, "withdraw-model-b")).toEqual([input])
          expect((yield* sessions.get(chat.id)).model?.id).toBe(ref.modelID)
        }
        prepare.resolve()
        yield* llm.wait(1)
        yield* Deferred.succeed(response, undefined)
        yield* Fiber.join(first)
        yield* Fiber.join(second)
        const expected = withdrawLater ? ref.modelID : modelB.modelID
        expect((yield* sessions.get(chat.id)).model?.id).toBe(expected)
        const followup = yield* prompt.prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [{ type: "text", text: "inherit the saved model" }],
        })
        expect(followup.info.role === "assistant" && followup.info.modelID).toBe(expected)
        expect((yield* llm.inputs).at(-1)?.model).toBe(expected)
        if (withdrawLater) expect(JSON.stringify(yield* llm.inputs)).not.toContain("fast model B")
      }),
  )

noLLMServer.instance(
  "failed publication removes its partial message",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const id = MessageID.ascending()
      const observed: string[] = []
      yield* events.listen((event) =>
        Effect.sync(() => {
          observed.push(event.type)
        }),
      )
      yield* events.project(SessionV1.Event.PartUpdated, (event) =>
        event.data.part.type === "text" && event.data.part.text === "fail publication"
          ? Effect.die("publication failed")
          : Effect.void,
      )
      const result = yield* prompt
        .prompt({
          sessionID: chat.id,
          messageID: id,
          model: ref,
          parts: [
            { type: "text", text: "first part" },
            { type: "text", text: "fail publication" },
          ],
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(result)).toBe(true)
      expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
      expect(observed).toContain(SessionV1.Event.MessageRemoved.type)
      expect(yield* prompt.withdraw(chat.id, "failed")).toEqual([])
    }),
  { config: cfg },
)

noLLMServer.instance(
  "concurrent admissions reserve message identity",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chats = [yield* sessions.create({ title: "One" }), yield* sessions.create({ title: "Two" })]
      const messageID = MessageID.ascending()
      const partID = PartID.ascending()
      const results = yield* Effect.all(
        chats.map((chat) =>
          prompt
            .prompt({
              sessionID: chat.id,
              messageID,
              model: ref,
              noReply: true,
              parts: [{ id: partID, type: "text", text: "original" }],
            })
            .pipe(Effect.exit),
        ),
        { concurrency: "unbounded" },
      )
      expect(results.filter(Exit.isSuccess)).toHaveLength(1)
      const conflict = results.find(Exit.isFailure)!
      expect(Cause.squash(conflict.cause)).toBeInstanceOf(SessionPrompt.PromptConflictError)
      const messages = (yield* Effect.forEach(chats, (chat) => sessions.messages({ sessionID: chat.id }))).flat()
      expect(messages).toHaveLength(1)
      expect(messages[0].parts).toMatchObject([{ id: partID, type: "text", text: "original" }])
    }),
  { config: cfg },
)

raceNoLLMServer.instance(
  "snapshot ownership precedes processor preparation",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const started = defer<void>()
      processorCreateStarted.push(started.resolve)
      const chat = yield* sessions.create({ title: "Pinned" })
      const activeID = MessageID.ascending()
      const active = yield* prompt
        .prompt({
          sessionID: chat.id,
          messageID: activeID,
          model: ref,
          parts: [{ type: "text", text: "active preparation" }],
        })
        .pipe(Effect.forkChild)
      yield* awaitWithTimeout(
        Effect.promise(() => started.promise),
        "processor preparation did not start",
      )
      const input = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [{ type: "text" as const, text: "queued during preparation" }],
      }
      const queued = yield* enqueue(input)
      expect(yield* prompt.withdraw(chat.id, "preparation")).toEqual([input])
      yield* prompt.cancel(chat.id)
      yield* Fiber.join(active)
      yield* Fiber.join(queued)
      expect((yield* sessions.messages({ sessionID: chat.id })).some((message) => message.info.id === activeID)).toBe(
        true,
      )
    }),
  { config: cfg },
)

it.instance("interrupted preparation is cleaned up and does not block a later admitted prompt", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const registry = yield* ToolRegistry.Service
    const { read } = yield* registry.named()
    const original = read.execute
    const started = yield* Deferred.make<void>()
    read.execute = () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        read.execute = original
      }),
    )
    const chat = yield* sessions.create({ title: "Pinned" })
    const id = MessageID.ascending()
    const first = yield* prompt
      .prompt({
        sessionID: chat.id,
        messageID: id,
        model: ref,
        parts: [{ type: "file", mime: "text/plain", url: "file:///pending.txt" }],
      })
      .pipe(Effect.forkChild)
    yield* awaitWithTimeout(Deferred.await(started), "file resolution did not start")
    yield* llm.text("done")
    const next = MessageID.ascending()
    const second = yield* enqueue({
      sessionID: chat.id,
      messageID: next,
      model: ref,
      parts: [{ type: "text", text: "surviving prompt" }],
    })
    yield* Fiber.interrupt(first)
    yield* awaitWithTimeout(Fiber.join(second), "failed resolution blocked the queue")
    expect(yield* llm.calls).toBe(1)
    expect((yield* sessions.messages({ sessionID: chat.id })).some((message) => message.info.id === id)).toBe(false)
    expect(yield* prompt.withdraw(chat.id, "after-interruption")).toEqual([])
  }),
)

it.instance("compaction claims its snapshot and retains its own parent when a followup is newer", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const compaction = yield* SessionCompaction.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const first = yield* Deferred.make<void>()
    const compact = yield* Deferred.make<void>()
    yield* llm.push(
      reply().wait(deferredAsPromise(first)).text("first response").stop(),
      reply().wait(deferredAsPromise(compact)).text("compacted history").stop(),
      reply().text("done").stop(),
    )
    const active = yield* prompt
      .prompt({ sessionID: chat.id, model: ref, parts: [{ type: "text", text: "initial" }] })
      .pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* compaction.create({ sessionID: chat.id, agent: "build", model: ref, auto: true })
    const marker = (yield* sessions.messages({ sessionID: chat.id })).find((message) =>
      message.parts.some((part) => part.type === "compaction"),
    )!
    const followupID = MessageID.ascending()
    yield* prompt.prompt({
      sessionID: chat.id,
      messageID: followupID,
      model: ref,
      noReply: true,
      parts: [{ type: "text", text: "included in compaction" }],
    })
    yield* Deferred.succeed(first, undefined)
    yield* llm.wait(2)
    expect(JSON.stringify((yield* llm.inputs)[1])).toContain("included in compaction")
    const queued = {
      sessionID: chat.id,
      messageID: MessageID.ascending(),
      model: ref,
      parts: [{ type: "text" as const, text: "withdraw while compacting" }],
    }
    const queuedFiber = yield* enqueue(queued)
    expect(yield* prompt.withdraw(chat.id, "compacting")).toEqual([queued])
    yield* Deferred.succeed(compact, undefined)
    yield* Fiber.join(active)
    yield* Fiber.join(queuedFiber)
    const summary = (yield* sessions.messages({ sessionID: chat.id })).find(
      (message) => message.info.role === "assistant" && message.info.summary,
    )
    expect(summary?.info.role === "assistant" && summary.info.parentID).toBe(marker.info.id)
    expect(JSON.stringify(yield* llm.inputs)).not.toContain("withdraw while compacting")
  }),
)

it.instance("visible pending prompts survive compaction and subsequent provider turns in admission order", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const compaction = yield* SessionCompaction.Service
    const plugin = yield* Plugin.Service
    const hooks = yield* plugin.list()
    const started = defer<void>()
    const release = defer<void>()
    const id = MessageID.ascending()
    const hook = {
      "chat.message": async (input: { messageID?: string }) => {
        if (input.messageID !== id) return
        started.resolve()
        await release.promise
      },
    }
    hooks.push(hook)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        release.resolve()
        hooks.splice(hooks.indexOf(hook), 1)
      }),
    )
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.text("initial response")
    yield* prompt.prompt({ sessionID: chat.id, model: ref, parts: [{ type: "text", text: "initial question" }] })
    const pending = yield* prompt
      .prompt({
        sessionID: chat.id,
        messageID: id,
        model: ref,
        parts: [{ type: "text", text: "slow prompt must survive" }],
      })
      .pipe(Effect.forkChild)
    yield* awaitWithTimeout(
      Effect.promise(() => started.promise),
      "slow prompt did not start",
    )
    const secondID = MessageID.ascending()
    const second = yield* enqueue({
      sessionID: chat.id,
      messageID: secondID,
      model: ref,
      parts: [{ type: "text", text: "visible pending prompt must survive" }],
    })
    yield* visible(secondID)
    yield* compaction.create({ sessionID: chat.id, agent: "build", model: ref, auto: false })
    yield* llm.text("compacted history")
    yield* prompt.loop({ sessionID: chat.id })
    expect(JSON.stringify(yield* llm.inputs)).not.toContain("slow prompt must survive")
    expect(JSON.stringify(yield* llm.inputs)).not.toContain("visible pending prompt must survive")
    yield* llm.push(reply().text("continuing"), reply().text("answered both prompts").stop())
    release.resolve()
    const result = yield* awaitWithTimeout(Fiber.join(pending), "compaction hid the pending prompt")
    yield* Fiber.join(second)
    expect(result.info).toMatchObject({ role: "assistant", parentID: secondID })
    expect(yield* llm.calls).toBe(4)
    for (const input of (yield* llm.inputs).slice(2).map((input) => JSON.stringify(input))) {
      expect(input).toContain("slow prompt must survive")
      expect(input).toContain("visible pending prompt must survive")
      expect(input.indexOf("compacted history")).toBeLessThan(input.indexOf("slow prompt must survive"))
      expect(input.indexOf("slow prompt must survive")).toBeLessThan(
        input.indexOf("visible pending prompt must survive"),
      )
    }
    expect(yield* prompt.withdraw(chat.id, "after-compaction")).toEqual([])
  }),
)

it.instance("message-update observers can withdraw reentrantly before publication finishes", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2Bridge.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const input = {
      sessionID: chat.id,
      messageID: MessageID.ascending(),
      model: ref,
      parts: [{ type: "text" as const, text: "withdraw from observer" }],
    }
    const observed: string[] = []
    let withdrawn: SessionPrompt.PromptInput[] = []
    yield* events.listen((event) =>
      Effect.gen(function* () {
        observed.push(event.type)
        if (event.type !== SessionV1.Event.MessageUpdated.type) return
        withdrawn = yield* prompt.withdraw(chat.id, "observer")
      }),
    )
    yield* awaitWithTimeout(prompt.prompt(input), "withdrawal deadlocked in an event listener")
    expect(withdrawn).toEqual([input])
    expect(observed).toContain(SessionV1.Event.MessageRemoved.type)
    expect(observed).not.toContain(SessionV1.Event.PartUpdated.type)
    expect(
      (yield* sessions.messages({ sessionID: chat.id })).some((message) => message.info.id === input.messageID),
    ).toBe(false)
    expect(yield* llm.calls).toBe(0)
  }),
)

for (const noReply of [false, true])
  it.instance(`partial ${noReply ? "noReply" : "user"} publication cannot enter a runner snapshot`, () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const previous = yield* seed(chat.id, { finish: "stop" })
      const id = MessageID.ascending()
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      yield* events.listen((event) => {
        if (event.type !== SessionV1.Event.MessageUpdated.type) return Effect.void
        const data = event.data as typeof SessionV1.Event.MessageUpdated.data.Type
        return data.info.id === id
          ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void
      })
      const fiber = yield* enqueue({
        sessionID: chat.id,
        messageID: id,
        model: ref,
        noReply,
        parts: [{ type: "text", text: "fully prepared input" }],
      })
      yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
      yield* awaitWithTimeout(Deferred.await(started), "publication did not start")
      expect((yield* MessageV2.get({ sessionID: chat.id, messageID: id })).parts).toEqual([])
      expect((yield* prompt.loop({ sessionID: chat.id })).info.id).toBe(previous.assistant.id)
      expect(yield* llm.calls).toBe(0)
      yield* llm.text("done")
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(fiber)
      if (noReply) yield* prompt.loop({ sessionID: chat.id })
      expect(yield* llm.calls).toBe(1)
      expect(JSON.stringify(yield* llm.inputs)).toContain("fully prepared input")
    }),
  )

for (const remove of [false, true])
  it.instance(
    `snapshot reads exclude late admissions${remove ? " and withdrawn copies" : " until the next turn"}`,
    () =>
      Effect.gen(function* () {
        const { llm } = yield* useServerConfig(providerCfg)
        const prompt = yield* SessionPrompt.Service
        const sessions = yield* Session.Service
        const chat = yield* sessions.create({ title: "Pinned" })
        const previous = yield* seed(chat.id, { finish: "stop" })
        const before = yield* Deferred.make<void>()
        const read = yield* Deferred.make<void>()
        const after = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const original = MessageV2.stream
        let held = false
        // Gate the real read, rather than replacing history or snapshot selection.
        const spy = spyOn(MessageV2, "stream").mockImplementation((sessionID) =>
          Effect.gen(function* () {
            if (held || sessionID !== chat.id) return yield* original(sessionID)
            held = true
            yield* Deferred.succeed(before, undefined)
            yield* Deferred.await(read)
            const messages = yield* original(sessionID)
            yield* Deferred.succeed(after, undefined)
            yield* Deferred.await(release)
            return messages
          }),
        )
        const first = {
          sessionID: chat.id,
          messageID: MessageID.ascending(),
          model: ref,
          tools: { read: true },
          parts: [{ type: "text" as const, text: "withdraw selected input" }],
        }
        const a = yield* enqueue(first)
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            spy.mockRestore()
            yield* Deferred.succeed(read, undefined)
            yield* Deferred.succeed(release, undefined)
          }),
        )
        yield* awaitWithTimeout(Deferred.await(before), "snapshot read did not start")
        const second = {
          ...first,
          messageID: MessageID.ascending(),
          tools: { read: false },
          parts: [{ type: "text" as const, text: "late admission" }],
        }
        const b = yield* enqueue(second)
        yield* visible(second.messageID)
        yield* Deferred.succeed(read, undefined)
        yield* awaitWithTimeout(Deferred.await(after), "snapshot read did not finish")
        if (remove) expect(yield* prompt.withdraw(chat.id, "during-read")).toEqual([first, second])
        if (!remove) yield* llm.push(reply().text("first response").stop(), reply().text("second response").stop())
        yield* Deferred.succeed(release, undefined)
        const result = yield* Fiber.join(a)
        yield* Fiber.join(b)
        if (remove) {
          expect(result.info.id).toBe(previous.assistant.id)
          expect(yield* llm.calls).toBe(0)
          expect(
            (yield* sessions.messages({ sessionID: chat.id })).some(
              (message) => message.info.id === first.messageID || message.info.id === second.messageID,
            ),
          ).toBe(false)
        }
        if (!remove) {
          expect(yield* llm.calls).toBe(2)
          expect(JSON.stringify((yield* llm.inputs)[0])).not.toContain("late admission")
          expect(JSON.stringify((yield* llm.inputs)[1])).toContain("late admission")
          expect(JSON.stringify((yield* llm.inputs)[0].tools)).toContain('"name":"read"')
          expect(JSON.stringify((yield* llm.inputs)[1].tools)).not.toContain('"name":"read"')
        }
      }),
  )

noLLMServer.instance(
  "failed unpersisted preparation can retry the same message ID",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const input = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [{ type: "text" as const, text: "retry after correcting the agent" }],
      }
      expect(Exit.isFailure(yield* prompt.prompt({ ...input, agent: "missing-agent" }).pipe(Effect.exit))).toBe(true)
      expect(yield* sessions.messages({ sessionID: chat.id })).toEqual([])
      expect((yield* prompt.prompt({ ...input, agent: "build", noReply: true })).info.id).toBe(input.messageID)
      expect(yield* prompt.withdraw(chat.id, "failed")).toEqual([])
    }),
  { config: cfg },
)

it.instance("withdrawal receipts survive partial removal failure without exposing the batch to the model", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2Bridge.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const release = yield* Deferred.make<void>()
    yield* llm.push(reply().wait(deferredAsPromise(release)).text("active result").stop())
    const active = yield* enqueue({ sessionID: chat.id, model: ref, parts: [{ type: "text", text: "active" }] })
    yield* llm.wait(1)
    const inputs = ["first pending", "second pending"].map((text) => ({
      sessionID: chat.id,
      messageID: MessageID.ascending(),
      model: ref,
      parts: [{ type: "text" as const, text }],
    }))
    const pending = []
    for (const input of inputs) {
      pending.push(yield* enqueue(input))
      yield* visible(input.messageID)
    }
    let fail = true
    yield* events.project(SessionV1.Event.MessageRemoved, (event) =>
      Effect.gen(function* () {
        if (event.data.messageID !== inputs[1].messageID || !fail) return
        fail = false
        return yield* Effect.die("simulated removal failure")
      }),
    )
    expect(Exit.isFailure(yield* prompt.withdraw(chat.id, "retry-removal").pipe(Effect.exit))).toBe(true)
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(active)
    yield* Effect.forEach(pending, Fiber.join)
    expect(yield* llm.calls).toBe(1)
    expect(yield* prompt.withdraw(chat.id, "retry-removal")).toEqual(inputs)
    expect(yield* prompt.withdraw(chat.id, "retry-removal")).toEqual(inputs)
    expect(
      (yield* sessions.messages({ sessionID: chat.id })).some((message) =>
        inputs.some((input) => input.messageID === message.info.id),
      ),
    ).toBe(false)
  }),
)

it.instance("cancelled hung preparation does not block a subsequent submission", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const plugin = yield* Plugin.Service
    const hooks = yield* plugin.list()
    const id = MessageID.ascending()
    const started = defer<void>()
    const release = defer<void>()
    const hook = {
      "chat.message": async (input: { messageID?: string }) => {
        if (input.messageID !== id) return
        started.resolve()
        await release.promise
      },
    }
    hooks.push(hook)
    const chat = yield* sessions.create({ title: "Pinned" })
    const input = {
      sessionID: chat.id,
      messageID: id,
      model: ref,
      parts: [{ type: "text" as const, text: "hung input" }],
    }
    const hung = yield* enqueue(input)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        release.resolve()
        hooks.splice(hooks.indexOf(hook), 1)
      }),
    )
    yield* awaitWithTimeout(
      Effect.promise(() => started.promise),
      "preparation did not start",
    )
    yield* prompt.cancel(chat.id)
    yield* llm.text("new request completed")
    const result = yield* awaitWithTimeout(
      prompt.prompt({ sessionID: chat.id, model: ref, parts: [{ type: "text", text: "new request" }] }),
      "cancelled preparation blocked the next request",
    )
    expect(result.info.role).toBe("assistant")
    expect(JSON.stringify(yield* llm.inputs)).not.toContain("hung input")
    expect(yield* prompt.withdraw(chat.id, "cancelled")).toEqual([input])
    release.resolve()
    yield* Fiber.join(hung)
    expect(yield* llm.calls).toBe(1)
  }),
)

noLLMServer.instance(
  "session deletion discards receipts and suppresses late preparation errors",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const plugin = yield* Plugin.Service
      const events = yield* EventV2Bridge.Service
      const hooks = yield* plugin.list()
      const chat = yield* sessions.create({ title: "Pinned" })
      const started = defer<void>()
      const release = defer<void>()
      const hook = {
        "chat.message": async (input: { sessionID: string }) => {
          if (input.sessionID !== chat.id) return
          started.resolve()
          await release.promise
          throw new Error("late plugin failure")
        },
      }
      hooks.push(hook)
      const input = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        model: ref,
        parts: [{ type: "text" as const, text: "deleted session input" }],
      }
      const fiber = yield* enqueue(input)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          release.resolve()
          hooks.splice(hooks.indexOf(hook), 1)
        }),
      )
      yield* awaitWithTimeout(
        Effect.promise(() => started.promise),
        "preparation did not start",
      )
      expect(yield* prompt.withdraw(chat.id, "receipt")).toEqual([input])
      yield* sessions.remove(chat.id)
      expect(yield* prompt.withdraw(chat.id, "receipt")).toEqual([])
      const errors: unknown[] = []
      yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type === Session.Event.Error.type) errors.push(event.data)
        }),
      )
      const next = yield* sessions.create({ title: "Next" })
      yield* prompt.prompt({ ...input, sessionID: next.id, noReply: true })
      release.resolve()
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionPrompt.PromptAbandonedError)
      expect(errors).toEqual([])
    }),
  { config: cfg },
)

noLLMServer.instance(
  "session deletion retains an in-flight publisher's ID until it settles",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const events = yield* EventV2Bridge.Service
      const chat = yield* sessions.create({ title: "Deleted" })
      const next = yield* sessions.create({ title: "Next" })
      const input = {
        messageID: MessageID.ascending(),
        model: ref,
        parts: [{ type: "text" as const, text: "old publication" }],
      }
      let conflict: unknown
      yield* events.listen((event) =>
        Effect.gen(function* () {
          if (event.type !== SessionV1.Event.MessageUpdated.type) return
          const data = event.data as typeof SessionV1.Event.MessageUpdated.data.Type
          if (data.info.sessionID !== chat.id) return
          yield* sessions.remove(chat.id).pipe(Effect.orDie)
          const exit = yield* prompt.prompt({ ...input, sessionID: next.id, noReply: true }).pipe(Effect.exit)
          conflict = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
        }),
      )
      yield* prompt.prompt({ ...input, sessionID: chat.id })
      expect(conflict).toBeInstanceOf(SessionPrompt.PromptConflictError)
      const result = yield* prompt.prompt({ ...input, sessionID: next.id, noReply: true })
      expect(result.info.sessionID).toBe(next.id)
      expect(yield* sessions.messages({ sessionID: next.id })).toHaveLength(1)
    }),
  { config: cfg },
)

const deferredAsPromise = <A>(deferred: Deferred.Deferred<A>): PromiseLike<A> => ({
  then: (onfulfilled, onrejected) => {
    Effect.runFork(
      Deferred.await(deferred).pipe(
        Effect.match({
          onFailure: (error) => {
            onrejected?.(error)
          },
          onSuccess: (value) => {
            onfulfilled?.(value)
          },
        }),
      ),
    )
    return deferredAsPromise(deferred) as PromiseLike<never>
  },
})

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const succeedVoid = (deferred: Deferred.Deferred<void>) => {
  Effect.runSync(Deferred.succeed(deferred, void 0).pipe(Effect.ignore))
}

const user = Effect.fn("test.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const seed = Effect.fn("test.seed")(function* (sessionID: SessionID, opts?: { finish?: string }) {
  const session = yield* Session.Service
  const msg = yield* user(sessionID, "hello")
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: msg.id,
    sessionID,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
    ...(opts?.finish ? { finish: opts.finish } : {}),
  }
  yield* session.updateMessage(assistant)
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: assistant.id,
    sessionID,
    type: "text",
    text: "hi there",
  })
  return { user: msg, assistant }
})

const addSubtask = (sessionID: SessionID, messageID: MessageID, model = ref) =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    yield* session.updatePart({
      id: PartID.ascending(),
      messageID,
      sessionID,
      type: "subtask",
      prompt: "look into the cache key path",
      description: "inspect bug",
      agent: "general",
      model,
    })
  })

const boot = Effect.fn("test.boot")(function* (input?: { title?: string }) {
  const config = yield* Config.Service
  const prompt = yield* SessionPrompt.Service
  const run = yield* SessionRunState.Service
  const sessions = yield* Session.Service
  yield* config.get()
  const chat = yield* sessions.create(input ?? { title: "Pinned" })
  return { prompt, run, sessions, chat }
})

// Loop semantics

noLLMServer.instance(
  "loop exits immediately when last assistant has stop finish",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* seed(chat.id, { finish: "stop" })

      const result = yield* prompt.loop({ sessionID: chat.id })
      expect(result.info.role).toBe("assistant")
      if (result.info.role === "assistant") expect(result.info.finish).toBe("stop")
    }),
  { config: cfg },
)

noLLMServer.instance(
  "loop exits for a completed parent turn with nonmonotonic message IDs",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      const userID = MessageID.make("msg_z_user")
      const assistantID = MessageID.make("msg_a_assistant")
      yield* sessions.updateMessage({
        id: userID,
        role: "user",
        sessionID: chat.id,
        agent: "build",
        model: ref,
        time: { created: 100 },
      })
      yield* sessions.updateMessage({
        id: assistantID,
        role: "assistant",
        parentID: userID,
        sessionID: chat.id,
        mode: "build",
        agent: "build",
        cost: 0,
        path: { cwd: "/tmp", root: "/tmp" },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ref.modelID,
        providerID: ref.providerID,
        time: { created: 200, completed: 201 },
        finish: "stop",
      })

      const result = yield* prompt.loop({ sessionID: chat.id })

      expect(result.info.id).toBe(assistantID)
    }),
  { config: cfg },
)

it.instance("loop exits without an LLM request for interrupted orphan tool calls", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const seeded = yield* seed(chat.id, { finish: "stop" })
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: seeded.assistant.id,
      sessionID: chat.id,
      type: "tool",
      callID: "interrupted-call",
      tool: "edit",
      state: {
        status: "error",
        input: {},
        error: "Tool execution aborted",
        metadata: { interrupted: true },
        time: { start: 1, end: 2 },
      },
    })

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.id).toBe(seeded.assistant.id)
    expect(yield* llm.hits).toHaveLength(0)
  }),
)

it.instance("loop calls LLM and returns assistant message", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.text("world")

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
    const parts = result.parts.filter((p) => p.type === "text")
    expect(parts.some((p) => p.type === "text" && p.text === "world")).toBe(true)
    expect(yield* llm.hits).toHaveLength(1)
  }),
)

withMcpInstructions.instance(
  "loop includes MCP instructions in model system context",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(llm.wait(1), "timed out waiting for MCP instruction request", "10 seconds")

      const hits = yield* llm.hits
      const body = JSON.stringify(hits[0]?.body)
      expect(body).toContain('<server name=\\"guide-server\\">')
      expect(body).toContain("Use lookup before mutate.")
      yield* Fiber.interrupt(fiber)
    }),
  15_000,
)

it.instance("legacy prompt emits message events without session.next events", () =>
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Pinned",
      agent: "plan",
      model: { providerID: ProviderV2.ID.make("old"), id: ModelV2.ID.make("old-model") },
    })
    const seen: string[] = []
    const off = yield* events.listen((event) => {
      seen.push(event.type)
      return Effect.void
    })

    const first = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      model: ref,
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    const second = yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "again" }],
    })
    yield* off

    expect(first.info.role).toBe("user")
    expect(second.info.role).toBe("user")
    if (first.info.role === "user" && second.info.role === "user") {
      expect(first.info.model).toEqual(ref)
      expect(second.info.model).toEqual(ref)
    }
    expect(yield* sessions.get(chat.id)).toMatchObject({
      agent: "build",
      model: { providerID: ref.providerID, id: ref.modelID },
    })
    expect(seen).toContain(Session.Event.Updated.type)
    expect(seen).toContain(MessageV2.Event.Updated.type)
    expect(seen).toContain(MessageV2.Event.PartUpdated.type)
    expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
  }),
)

it.instance("loop surfaces content-filter finishes as session errors", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const events = yield* EventV2Bridge.Service
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    const errors: NonNullable<SessionV1.Assistant["error"]>[] = []
    const expected = {
      name: "ContentFilterError",
      data: { message: "The response was blocked by the provider's content filter" },
    } satisfies NonNullable<SessionV1.Assistant["error"]>
    const off = yield* events.listen((event) => {
      if (event.type !== Session.Event.Error.type) return Effect.void
      const data = event.data as typeof Session.Event.Error.data.Type
      if (data.sessionID === chat.id && data.error) errors.push(data.error)
      return Effect.void
    })

    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply().text("partial response").contentFilter())

    const result = yield* prompt.loop({ sessionID: chat.id })
    const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: result.info.id })
    yield* off

    expect(yield* llm.hits).toHaveLength(1)
    expect(result.info.role).toBe("assistant")
    expect(stored.info.role).toBe("assistant")
    if (result.info.role === "assistant" && stored.info.role === "assistant") {
      expect(result.info.finish).toBe("content-filter")
      expect(result.info.error).toEqual(expected)
      expect(stored.info.error).toEqual(result.info.error)
      expect(errors).toContainEqual(expected)
    }
    expect(result.parts).toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "text", text: "partial response" })]),
    )
  }),
)

it.instance("loop stops provider overflow instead of auto-compacting when disabled", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      compaction: { auto: false },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.error(413, { error: { message: "request entity too large" } })
    yield* prompt.prompt({
      sessionID: chat.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })

    const result = yield* prompt.loop({ sessionID: chat.id })
    const messages = yield* sessions.messages({ sessionID: chat.id })

    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.info.error?.name).toBe("ContextOverflowError")
      expect(result.info.finish).toBe("error")
    }
    expect(messages.some((message) => message.parts.some((part) => part.type === "compaction"))).toBe(false)
  }),
)

noLLMServer.instance.skip(
  "prompt emits v2 prompted and synthetic events (v2 projector disabled)",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [
          { type: "text", text: "hello v2" },
          {
            type: "file",
            mime: "text/plain",
            filename: "note.txt",
            url: "data:text/plain;base64,bm90ZSBjb250ZW50",
          },
        ],
      })

      const messages = yield* SessionV2.Service.use((session) => session.messages({ sessionID: chat.id })).pipe(
        Effect.provide(
          LayerNode.compile(SessionV2.node, [
            [SessionExecution.node, SessionExecution.noopLayer],
            [LocationServiceMap.node, locationServiceMapLayer],
          ]),
        ),
      )
      const { db } = yield* Database.Service
      const row = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, chat.id))
        .get()
        .pipe(Effect.orDie)
      expect(messages.find((message) => message.type === "user")).toMatchObject({ type: "user", text: "hello v2" })
      expect(typeof row?.data.time.created).toBe("number")
      expect(messages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ type: "synthetic", text: expect.stringContaining("Called the Read tool") }),
          expect.objectContaining({ type: "synthetic", text: "note content" }),
        ]),
      )
    }),
  { config: cfg },
)

it.instance("static loop returns assistant text through local provider", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Prompt provider",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })

    yield* llm.text("world")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(result.info.role).toBe("assistant")
    expect(result.parts.some((part) => part.type === "text" && part.text === "world")).toBe(true)
    expect(yield* llm.hits).toHaveLength(1)
    expect(yield* llm.pending).toBe(0)
  }),
)

it.instance("static loop consumes queued replies across turns", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Prompt provider turns",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello one" }],
    })

    yield* llm.text("world one")

    const first = yield* prompt.loop({ sessionID: session.id })
    expect(first.info.role).toBe("assistant")
    expect(first.parts.some((part) => part.type === "text" && part.text === "world one")).toBe(true)

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello two" }],
    })

    yield* llm.text("world two")

    const second = yield* prompt.loop({ sessionID: session.id })
    expect(second.info.role).toBe("assistant")
    expect(second.parts.some((part) => part.type === "text" && part.text === "world two")).toBe(true)

    expect(yield* llm.hits).toHaveLength(2)
    expect(yield* llm.pending).toBe(0)
  }),
)

it.instance("loop continues when finish is tool-calls", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.tool("first", { value: "first" })
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("loop continues when finish is unknown", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply())
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("glob tool keeps instance context during prompt runs", () =>
  Effect.gen(function* () {
    const { dir, llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Glob context",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const file = path.join(dir, "probe.txt")
    yield* writeText(file, "probe")

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "find text files" }],
    })
    yield* llm.tool("glob", { pattern: "**/*.txt" })
    yield* llm.text("done")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(result.info.role).toBe("assistant")

    const msgs = yield* MessageV2.filterCompactedEffect(session.id)
    const tool = msgs
      .flatMap((msg) => msg.parts)
      .find(
        (part): part is CompletedToolPart =>
          part.type === "tool" && part.tool === "glob" && part.state.status === "completed",
      )
    if (!tool) return

    expect(tool.state.output).toContain(file)
    expect(tool.state.output).not.toContain("No context found for instance")
    expect(result.parts.some((part) => part.type === "text" && part.text === "done")).toBe(true)
  }),
)

it.instance("loop continues when finish is stop but assistant has tool parts", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "Pinned",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      parts: [{ type: "text", text: "hello" }],
    })
    yield* llm.push(reply().tool("first", { value: "first" }).stop())
    yield* llm.text("second")

    const result = yield* prompt.loop({ sessionID: session.id })
    expect(yield* llm.calls).toBe(2)
    expect(result.info.role).toBe("assistant")
    if (result.info.role === "assistant") {
      expect(result.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)
      expect(result.info.finish).toBe("stop")
    }
  }),
)

it.instance("failed subtask preserves metadata on error tool state", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig((url) => ({
      ...providerCfg(url),
      agent: {
        general: {
          model: "test/missing-model",
        },
      },
    }))
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.tool("task", {
      description: "inspect bug",
      prompt: "look into the cache key path",
      subagent_type: "general",
    })
    yield* llm.text("done")
    const msg = yield* user(chat.id, "hello")
    yield* addSubtask(chat.id, msg.id)

    const result = yield* prompt.loop({ sessionID: chat.id })
    expect(result.info.role).toBe("assistant")
    expect(yield* llm.calls).toBe(2)

    const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
    const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
    expect(taskMsg?.info.role).toBe("assistant")
    if (!taskMsg || taskMsg.info.role !== "assistant") return

    const tool = errorTool(taskMsg.parts)
    if (!tool) return

    expect(tool.state.error).toContain("Tool execution failed")
    expect(tool.state.metadata).toBeDefined()
    expect(tool.state.metadata?.sessionId).toBeDefined()
    expect(tool.state.metadata?.model).toEqual({
      providerID: ProviderV2.ID.make("test"),
      modelID: ModelV2.ID.make("missing-model"),
    })
  }),
)

it.instance("subtask child inherits parent session external_directory allow", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({
      title: "Parent",
      permission: [{ permission: "external_directory", pattern: "/tmp/allowed/*", action: "allow" }],
    })
    yield* llm.text("done")
    const msg = yield* user(chat.id, "hello")
    yield* addSubtask(chat.id, msg.id)

    yield* prompt.loop({ sessionID: chat.id })

    const kids = yield* sessions.children(chat.id)
    expect(kids).toHaveLength(1)
    const child = kids[0]!
    const rules = child.permission ?? []
    expect(rules).toEqual(
      expect.arrayContaining([{ permission: "external_directory", pattern: "/tmp/allowed/*", action: "allow" }]),
    )
    expect(Permission.evaluate("external_directory", "/tmp/allowed/file", rules).action).toBe("allow")
    expect(Permission.evaluate("task", "anything", rules).action).toBe("deny")
  }),
)

noLLMServer.instance("prompt tools replace previous prompt tool rules", () =>
  Effect.gen(function* () {
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt tools" })

    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      tools: { bash: false },
      parts: [{ type: "text", text: "first" }],
    })
    yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      noReply: true,
      tools: { read: true },
      parts: [{ type: "text", text: "second" }],
    })

    const reloaded = yield* sessions.get(session.id)
    expect(reloaded.permission).toEqual([{ permission: "read", pattern: "*", action: "allow" }])
    expect(Permission.evaluate("bash", "anything", reloaded.permission ?? []).action).toBe("ask")
  }),
)

it.instance(
  "running subtask preserves metadata after tool-call transition",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

      const tool = yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
          const tool = taskMsg?.parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
          if (tool?.state.status === "running" && tool.state.metadata?.sessionId) return tool
        }),
        "timed out waiting for running subtask metadata",
      )

      if (tool.state.status !== "running") return
      expect(typeof tool.state.metadata?.sessionId).toBe("string")
      expect(tool.state.title).toBeDefined()
      expect(tool.state.metadata?.model).toBeDefined()

      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
    }),
  5_000,
)

it.instance(
  "running task tool preserves metadata after tool-call transition",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.tool("task", {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)

      const tool = yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const assistant = msgs.findLast((item) => item.info.role === "assistant" && item.info.agent === "build")
          const tool = assistant?.parts.find(
            (part): part is SessionV1.ToolPart => part.type === "tool" && part.tool === "task",
          )
          if (tool?.state.status === "running" && tool.state.metadata?.sessionId) return tool
        }),
        "timed out waiting for running task metadata",
      )

      if (tool.state.status !== "running") return
      expect(typeof tool.state.metadata?.sessionId).toBe("string")
      expect(tool.state.title).toBe("inspect bug")
      expect(tool.state.metadata?.model).toBeDefined()

      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
    }),
  10_000,
)

it.instance(
  "loop sets status to busy then idle",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const status = yield* SessionStatus.Service

      yield* llm.hang

      const chat = yield* sessions.create({})
      yield* user(chat.id, "hi")

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      expect((yield* status.get(chat.id)).type).toBe("busy")
      yield* prompt.cancel(chat.id)
      yield* Fiber.await(fiber)
      expect((yield* status.get(chat.id)).type).toBe("idle")
    }),
  3_000,
)

// Cancel semantics

it.instance("cancel interrupts loop and resolves with an assistant message", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* seed(chat.id)

    yield* llm.hang

    yield* user(chat.id, "more")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)
    yield* prompt.cancel(chat.id)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      expect(exit.value.info.role).toBe("assistant")
    }
  }),
)

it.instance("cancel records MessageAbortedError on interrupted process", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.hang
    yield* user(chat.id, "hello")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)
    yield* prompt.cancel(chat.id)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      const info = exit.value.info
      if (info.role === "assistant") {
        expect(info.error?.name).toBe("MessageAbortedError")
      }
    }
  }),
)

raceNoLLMServer.instance(
  "finalizes assistant when cancelled before processor creation completes",
  () =>
    Effect.gen(function* () {
      processorCreateStarted.length = 0
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          processorCreateStarted.length = 0
        }),
      )

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Processor creation race" })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "first" }],
      })

      const firstCreate = defer<void>()
      processorCreateStarted.push(firstCreate.resolve)
      const first = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.promise(() => firstCreate.promise)

      yield* prompt.cancel(chat.id)
      const firstExit = yield* Fiber.await(first)
      expect(Exit.isSuccess(firstExit)).toBe(true)

      let messages = yield* sessions.messages({ sessionID: chat.id })
      const firstInterrupted = messages.at(-1)
      expect(firstInterrupted?.info.role).toBe("assistant")
      expect(firstInterrupted?.parts).toHaveLength(0)
      if (firstInterrupted?.info.role === "assistant") {
        expect(firstInterrupted.info.finish).toBeUndefined()
        expect(firstInterrupted.info.time.completed).toBeNumber()
        expect(firstInterrupted.info.error?.name).toBe("MessageAbortedError")
      }

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "second" }],
      })

      const secondCreate = defer<void>()
      processorCreateStarted.push(secondCreate.resolve)
      const second = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.promise(() => secondCreate.promise)

      yield* prompt.cancel(chat.id)
      const secondExit = yield* Fiber.await(second)
      expect(Exit.isSuccess(secondExit)).toBe(true)

      messages = yield* sessions.messages({ sessionID: chat.id })
      const poisonMessages = messages.filter(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.length === 0 &&
          !message.info.finish &&
          !message.info.time.completed &&
          !message.info.error,
      )
      expect(poisonMessages).toHaveLength(0)

      const interruptedMessages = messages.filter(
        (message) =>
          message.info.role === "assistant" &&
          message.parts.length === 0 &&
          message.info.time.completed &&
          message.info.error?.name === "MessageAbortedError",
      )
      expect(interruptedMessages).toHaveLength(2)

      const lastUser = messages.at(-2)
      const lastAssistant = messages.at(-1)
      expect(lastUser?.info.role).toBe("user")
      expect(lastAssistant?.info.role).toBe("assistant")
      if (lastUser?.info.role === "user" && lastAssistant?.info.role === "assistant") {
        expect(lastAssistant.info.parentID).toBe(lastUser?.info.id)
      }
    }),
  { config: cfg },
  3_000,
)

noLLMServer.instance(
  "cancel finalizes subtask tool state",
  () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>()
      const aborted = yield* Deferred.make<void>()
      const registry = yield* ToolRegistry.Service
      const { task } = yield* registry.named()
      const original = task.execute
      task.execute = (_args, ctx) =>
        Effect.callback<never>((_resume) => {
          ctx.abort.addEventListener("abort", () => succeedVoid(aborted), { once: true })
          if (ctx.abort.aborted) succeedVoid(aborted)
          succeedVoid(ready)
          return Effect.sync(() => succeedVoid(aborted))
        })
      yield* Effect.addFinalizer(() => Effect.sync(() => void (task.execute = original)))

      const { prompt, chat } = yield* boot()
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for task tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      yield* awaitWithTimeout(Deferred.await(aborted), "timed out waiting for task tool abort", "10 seconds")

      const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
      const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
      expect(taskMsg?.info.role).toBe("assistant")
      if (!taskMsg || taskMsg.info.role !== "assistant") return

      const tool = toolPart(taskMsg.parts)
      expect(tool?.type).toBe("tool")
      if (!tool) return

      expect(tool.state.status).not.toBe("running")
      expect(taskMsg.info.time.completed).toBeDefined()
      expect(taskMsg.info.finish).toBeDefined()
    }),
  { config: cfg },
  30_000,
)

it.instance(
  "cancel propagates from slash command subtask to child session",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const status = yield* SessionStatus.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      const msg = yield* user(chat.id, "hello")
      yield* addSubtask(chat.id, msg.id)

      const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)

      const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
      const taskMsg = msgs.find((item) => item.info.role === "assistant" && item.info.agent === "general")
      const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
      const sessionID = tool?.state.status === "running" ? tool.state.metadata?.sessionId : undefined
      expect(typeof sessionID).toBe("string")
      if (typeof sessionID !== "string") throw new Error("missing child session id")
      const childID = SessionID.make(sessionID)
      expect((yield* status.get(childID)).type).toBe("busy")

      yield* prompt.cancel(chat.id)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)

      expect((yield* status.get(chat.id)).type).toBe("idle")
      expect((yield* status.get(childID)).type).toBe("idle")
    }),
  10_000,
)

it.instance(
  "cancel with queued callers resolves all cleanly",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Pinned" })
      yield* llm.hang
      yield* user(chat.id, "hello")

      const a = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      const b = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      yield* prompt.cancel(chat.id)
      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      if (Exit.isSuccess(exitA) && Exit.isSuccess(exitB)) {
        expect(exitA.value.info.id).toBe(exitB.value.info.id)
      }
    }),
  { git: true },
  10_000,
)

// Queue semantics

noLLMServer.instance("concurrent loop callers get same result", () =>
  Effect.gen(function* () {
    const { prompt, run, chat } = yield* boot()
    yield* seed(chat.id, { finish: "stop" })

    const [a, b] = yield* Effect.all([prompt.loop({ sessionID: chat.id }), prompt.loop({ sessionID: chat.id })], {
      concurrency: "unbounded",
    })

    expect(a.info.id).toBe(b.info.id)
    expect(a.info.role).toBe("assistant")
    yield* run.assertNotBusy(chat.id)
  }),
)

it.instance("concurrent loop callers all receive same error result", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.fail("boom")
    yield* user(chat.id, "hello")

    const [a, b] = yield* Effect.all([prompt.loop({ sessionID: chat.id }), prompt.loop({ sessionID: chat.id })], {
      concurrency: "unbounded",
    })
    expect(a.info.id).toBe(b.info.id)
    expect(a.info.role).toBe("assistant")
  }),
)

it.instance("prompt submitted during an active run is included in the next LLM input", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const gate = yield* Deferred.make<void>()
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })

    yield* llm.hold("first", deferredAsPromise(gate))
    yield* llm.text("second")

    const a = yield* prompt
      .prompt({
        sessionID: chat.id,
        agent: "build",
        model: ref,
        parts: [{ type: "text", text: "first" }],
      })
      .pipe(Effect.forkChild)

    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const id = MessageID.ascending()
    const b = yield* enqueue({
      sessionID: chat.id,
      messageID: id,
      agent: "build",
      model: ref,
      parts: [{ type: "text", text: "second" }],
    })
    yield* visible(id)
    expect((yield* sessions.messages({ sessionID: chat.id })).some((message) => message.info.id === id)).toBe(true)

    yield* Deferred.succeed(gate, void 0)

    const [ea, eb] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
    expect(Exit.isSuccess(ea)).toBe(true)
    expect(Exit.isSuccess(eb)).toBe(true)
    expect(yield* llm.calls).toBe(2)

    const msgs = yield* sessions.messages({ sessionID: chat.id })
    const assistants = msgs.filter((msg) => msg.info.role === "assistant")
    expect(assistants).toHaveLength(2)
    const last = assistants.at(-1)
    if (!last || last.info.role !== "assistant") throw new Error("expected second assistant")
    expect(last.info.parentID).toBe(id)
    expect(last.parts.some((part) => part.type === "text" && part.text === "second")).toBe(true)

    const inputs = yield* llm.inputs
    expect(inputs).toHaveLength(2)
    const messages = inputs.at(-1)?.messages
    if (!Array.isArray(messages)) throw new Error("expected LLM messages")
    expect(messages.at(-1)).toEqual({ role: "user", content: "second" })
  }),
)

it.instance("assertNotBusy fails with BusyError when loop running", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const run = yield* SessionRunState.Service
    const sessions = yield* Session.Service
    yield* llm.hang

    const chat = yield* sessions.create({})
    yield* user(chat.id, "hi")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const exit = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "SessionBusyError", sessionID: chat.id })
    }

    yield* prompt.cancel(chat.id)
    yield* Fiber.await(fiber)
  }),
)

noLLMServer.instance("assertNotBusy succeeds when idle", () =>
  Effect.gen(function* () {
    const run = yield* SessionRunState.Service
    const sessions = yield* Session.Service

    const chat = yield* sessions.create({})
    const exit = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
    expect(Exit.isSuccess(exit)).toBe(true)
  }),
)

// Shell semantics

it.instance("shell rejects with BusyError when loop running", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "Pinned" })
    yield* llm.hang
    yield* user(chat.id, "hi")

    const fiber = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
    yield* llm.wait(1)
    yield* waitForBusy(chat.id)

    const exit = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "echo hi" }).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
      expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "SessionBusyError", sessionID: chat.id })
    }

    yield* prompt.cancel(chat.id)
    yield* Fiber.await(fiber)
  }),
)

unixNoLLMServer(
  "shell captures stdout and stderr in completed tool output",
  () =>
    Effect.gen(function* () {
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "printf out && printf err >&2",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.output).toContain("out")
      expect(tool.state.output).toContain("err")
      expect(tool.state.metadata.output).toContain("out")
      expect(tool.state.metadata.output).toContain("err")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell completes a fast command on the preferred shell",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "pwd",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.input.command).toBe("pwd")
      expect(tool.state.output).toContain(dir)
      expect(tool.state.metadata.output).toContain(dir)
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell uses configured shell over env shell",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        if (!(yield* hasBash)) return

        const { prompt, chat } = yield* boot()
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "[[ 1 -eq 1 ]] && printf configured",
        })

        const tool = completedTool(result.parts)
        if (!tool) return
        expect(tool.state.output).toContain("configured")
      }),
    ),
  { config: { ...cfg, shell: "bash" } },
  30_000,
)

unixNoLLMServer(
  "shell commands can change directory after startup",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { directory: dir } = yield* TestInstance
        const { prompt, run, chat } = yield* boot()
        const parent = path.dirname(dir)
        const result = yield* prompt.shell({
          sessionID: chat.id,
          agent: "build",
          command: "cd .. && pwd",
        })

        expect(result.info.role).toBe("assistant")
        const tool = completedTool(result.parts)
        if (!tool) return

        expect(tool.state.output).toContain(parent)
        expect(tool.state.metadata.output).toContain(parent)
        yield* run.assertNotBusy(chat.id)
      }),
    ),
  { config: cfg },
)

unixNoLLMServer(
  "shell lists files from the project directory",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const { prompt, run, chat } = yield* boot()
      yield* writeText(path.join(dir, "README.md"), "# e2e\n")

      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "command ls",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.input.command).toBe("command ls")
      expect(tool.state.output).toContain("README.md")
      expect(tool.state.metadata.output).toContain("README.md")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell captures stderr from a failing command",
  () =>
    Effect.gen(function* () {
      const { prompt, run, chat } = yield* boot()
      const result = yield* prompt.shell({
        sessionID: chat.id,
        agent: "build",
        command: "command -v __nonexistent_cmd_e2e__ || echo 'not found' >&2; exit 1",
      })

      expect(result.info.role).toBe("assistant")
      const tool = completedTool(result.parts)
      if (!tool) return

      expect(tool.state.output).toContain("not found")
      expect(tool.state.metadata.output).toContain("not found")
      yield* run.assertNotBusy(chat.id)
    }),
  { config: cfg },
)

unixNoLLMServer(
  "shell updates running metadata before process exit",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()

        const fiber = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "printf first && sleep 0.2 && printf second" })
          .pipe(Effect.forkChild)

        yield* pollWithTimeout(
          Effect.gen(function* () {
            const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
            const taskMsg = msgs.find((item) => item.info.role === "assistant")
            const tool = taskMsg ? toolPart(taskMsg.parts) : undefined
            if (tool?.state.status === "running" && tool.state.metadata?.output.includes("first")) return true
          }),
          "timed out waiting for running shell metadata",
        )

        const exit = yield* Fiber.await(fiber)
        expect(Exit.isSuccess(exit)).toBe(true)
      }),
    ),
  { config: cfg },
  30_000,
)

it.instance(
  "loop waits while shell runs and starts after shell exits",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.text("after-shell")

      const sh = yield* prompt
        .shell({ sessionID: chat.id, agent: "build", command: "sleep 0.2" })
        .pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const loop = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      expect(yield* llm.calls).toBe(0)

      yield* Fiber.await(sh)
      const exit = yield* Fiber.await(loop)

      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) {
        expect(exit.value.info.role).toBe("assistant")
        expect(exit.value.parts.some((part) => part.type === "text" && part.text === "after-shell")).toBe(true)
      }
      expect(yield* llm.calls).toBe(1)
    }),
  { git: true },
  10_000,
)

it.instance(
  "shell completion resumes queued loop callers",
  () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Pinned",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* llm.text("done")

      const sh = yield* prompt
        .shell({ sessionID: chat.id, agent: "build", command: "sleep 0.2" })
        .pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const a = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      const b = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      expect(yield* llm.calls).toBe(0)

      yield* Fiber.await(sh)
      const [ea, eb] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])

      expect(Exit.isSuccess(ea)).toBe(true)
      expect(Exit.isSuccess(eb)).toBe(true)
      if (Exit.isSuccess(ea) && Exit.isSuccess(eb)) {
        expect(ea.value.info.id).toBe(eb.value.info.id)
        expect(ea.value.info.role).toBe("assistant")
      }
      expect(yield* llm.calls).toBe(1)
    }),
  { git: true },
  10_000,
)

unix(
  "command ! expansion uses configured shell over env shell",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        if (!(yield* hasBash)) return
        const { llm } = yield* useServerConfig((url) => ({
          ...providerCfg(url),
          shell: "bash",
          command: {
            probe: {
              template: "Probe: !`[[ 1 -eq 1 ]] && printf configured`",
            },
          },
        }))

        const { prompt, chat } = yield* boot()
        yield* llm.text("done")

        const result = yield* prompt.command({
          sessionID: chat.id,
          command: "probe",
          arguments: "",
        })

        expect(result.info.role).toBe("assistant")
        const inputs = yield* llm.inputs
        expect(JSON.stringify(inputs.at(-1)?.messages)).toContain("configured")
      }),
    ),
  30_000,
)

unixNoLLMServer(
  "cancel interrupts shell and resolves cleanly",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, run, chat } = yield* boot()
        const { directory: dir } = yield* TestInstance
        const afs = yield* FSUtil.Service
        const ready = path.join(dir, ".shell-ready")

        const sh = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: ": > '.shell-ready'; sleep 30" })
          .pipe(Effect.forkChild)
        yield* pollWithTimeout(
          afs.existsSafe(ready).pipe(Effect.map((exists) => (exists ? (true as const) : undefined))),
          "shell never created readiness marker",
        )

        yield* prompt.cancel(chat.id)

        const status = yield* SessionStatus.Service
        expect((yield* status.get(chat.id)).type).toBe("idle")
        const busy = yield* run.assertNotBusy(chat.id).pipe(Effect.exit)
        expect(Exit.isSuccess(busy)).toBe(true)

        const exit = yield* Fiber.await(sh)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (Exit.isSuccess(exit)) {
          expect(exit.value.info.role).toBe("assistant")
          const tool = completedTool(exit.value.parts)
          if (tool) {
            expect(tool.state.output).toContain("User aborted the command")
          }
        }
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unixNoLLMServer(
  "cancel persists aborted shell result when shell ignores TERM",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()
        const { directory: dir } = yield* TestInstance
        const afs = yield* FSUtil.Service
        const ready = path.join(dir, ".trap-ready")

        const sh = yield* prompt
          .shell({
            sessionID: chat.id,
            agent: "build",
            // Touch marker AFTER trap installs so the test waits for the actual
            // ignore-TERM state before cancelling; otherwise SIGTERM can arrive
            // before `trap` runs and the escalation path is never exercised.
            command: `trap '' TERM; touch "${ready}"; sleep 30`,
          })
          .pipe(Effect.forkChild)

        yield* Effect.gen(function* () {
          while (!(yield* afs.existsSafe(ready))) {
            yield* Effect.sleep(Duration.millis(10))
          }
        }).pipe(Effect.timeout(Duration.seconds(5)))

        yield* prompt.cancel(chat.id)

        const exit = yield* Fiber.await(sh)
        expect(Exit.isSuccess(exit)).toBe(true)
        if (Exit.isSuccess(exit)) {
          expect(exit.value.info.role).toBe("assistant")
          const tool = completedTool(exit.value.parts)
          if (tool) {
            expect(tool.state.output).toContain("User aborted the command")
          }
        }
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

unix(
  "cancel finalizes interrupted bash tool output through normal truncation",
  () =>
    Effect.gen(function* () {
      const { dir, llm } = yield* useServerConfig(providerCfg)
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({
        title: "Interrupted bash truncation",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })

      yield* prompt.prompt({
        sessionID: chat.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "run bash" }],
      })

      yield* llm.tool("bash", {
        command:
          'i=0; while [ "$i" -lt 4000 ]; do printf "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx %05d\\n" "$i"; i=$((i + 1)); done; printf truncation-ready; sleep 30',
        timeout: 30_000,
        workdir: path.resolve(dir),
      })

      const run = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* llm.wait(1)
      yield* pollWithTimeout(
        Effect.gen(function* () {
          const msgs = yield* MessageV2.filterCompactedEffect(chat.id)
          const assistant = msgs.findLast((item) => item.info.role === "assistant")
          const tool = assistant ? toolPart(assistant.parts) : undefined
          if (tool?.state.status === "running" && tool.state.metadata?.output.includes("truncation-ready")) return true
        }),
        "timed out waiting for truncated shell output",
      )
      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(run)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isFailure(exit)) return

      const tool = completedTool(exit.value.parts)
      if (!tool) return

      expect(tool.state.metadata.truncated).toBe(true)
      expect(typeof tool.state.metadata.outputPath).toBe("string")
      expect(tool.state.output).toMatch(/\.\.\.output truncated\.\.\./)
      expect(tool.state.output).toMatch(/Full output saved to:\s+\S+/)
      expect(tool.state.output).not.toContain("Tool execution aborted")
    }),
  { git: true },
  30_000,
)

unixNoLLMServer(
  "cancel interrupts loop queued behind shell",
  () =>
    Effect.gen(function* () {
      const { prompt, chat } = yield* boot()

      const sh = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "sleep 30" }).pipe(Effect.forkChild)
      yield* waitForBusy(chat.id)

      const loop = yield* prompt.loop({ sessionID: chat.id }).pipe(Effect.forkChild)
      yield* Effect.sleep(50)

      yield* prompt.cancel(chat.id)

      const exit = yield* Fiber.await(loop)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) {
        const tool = completedTool(exit.value.parts)
        expect(tool?.state.output).toContain("User aborted the command")
      }

      yield* Fiber.await(sh)
    }),
  { git: true, config: cfg },
  30_000,
)

unixNoLLMServer(
  "shell rejects when another shell is already running",
  () =>
    withSh(() =>
      Effect.gen(function* () {
        const { prompt, chat } = yield* boot()

        const a = yield* prompt
          .shell({ sessionID: chat.id, agent: "build", command: "sleep 30" })
          .pipe(Effect.forkChild)
        yield* waitForBusy(chat.id)

        const exit = yield* prompt.shell({ sessionID: chat.id, agent: "build", command: "echo hi" }).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.BusyError)
        }

        yield* prompt.cancel(chat.id)
        yield* Fiber.await(a)
      }),
    ),
  { git: true, config: cfg },
  30_000,
)

// Abort signal propagation tests for inline tool execution

function hangUntilAborted(tool: { execute: (...args: any[]) => any }) {
  return Effect.gen(function* () {
    const ready = yield* Deferred.make<void>()
    const aborted = yield* Deferred.make<void>()
    const original = tool.execute
    tool.execute = (_args: any, ctx: any) => {
      ctx.abort.addEventListener("abort", () => succeedVoid(aborted), { once: true })
      if (ctx.abort.aborted) succeedVoid(aborted)
      succeedVoid(ready)
      return Effect.callback<never>(() => Effect.sync(() => succeedVoid(aborted)))
    }
    const restore = Effect.addFinalizer(() => Effect.sync(() => void (tool.execute = original)))
    return { ready, aborted, restore }
  })
}

noLLMServer.instance(
  "interrupt propagates abort signal to read tool via file part (text/plain)",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const { ready, restore } = yield* hangUntilAborted(read)
      yield* restore

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Abort Test" })

      const testFile = path.join(dir, "test.txt")
      yield* writeText(testFile, "hello world")

      const fiber = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [
            { type: "text", text: "read this" },
            { type: "file", url: `file://${testFile}`, filename: "test.txt", mime: "text/plain" },
          ],
        })
        .pipe(Effect.forkChild)

      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for read tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  { config: cfg },
  30_000,
)

noLLMServer.instance(
  "interrupt propagates abort signal to read tool via file part (directory)",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const registry = yield* ToolRegistry.Service
      const { read } = yield* registry.named()
      const { ready, restore } = yield* hangUntilAborted(read)
      yield* restore

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const chat = yield* sessions.create({ title: "Abort Test" })

      const fiber = yield* prompt
        .prompt({
          sessionID: chat.id,
          agent: "build",
          parts: [
            { type: "text", text: "read this" },
            { type: "file", url: `file://${dir}`, filename: "dir", mime: "application/x-directory" },
          ],
        })
        .pipe(Effect.forkChild)

      yield* awaitWithTimeout(Deferred.await(ready), "timed out waiting for read tool to start", "10 seconds")
      yield* prompt.cancel(chat.id)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  { config: cfg },
  30_000,
)

// Missing file handling

noLLMServer.instance(
  "does not fail the prompt when a file part is missing",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const missing = path.join(dir, "does-not-exist.ts")
      const msg = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [
          { type: "text", text: "please review @does-not-exist.ts" },
          {
            type: "file",
            mime: "text/plain",
            url: `file://${missing}`,
            filename: "does-not-exist.ts",
          },
        ],
      })

      if (msg.info.role !== "user") throw new Error("expected user message")
      const hasFailure = msg.parts.some(
        (part) => part.type === "text" && part.synthetic && part.text.includes("Read tool failed to read"),
      )
      expect(hasFailure).toBe(true)

      yield* sessions.remove(session.id)
    }),
  { config: cfg },
)

noLLMServer.instance(
  "keeps stored part order stable when file resolution is async",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const missing = path.join(dir, "still-missing.ts")
      const msg = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [
          {
            type: "file",
            mime: "text/plain",
            url: `file://${missing}`,
            filename: "still-missing.ts",
          },
          { type: "text", text: "after-file" },
        ],
      })

      if (msg.info.role !== "user") throw new Error("expected user message")

      const stored = yield* MessageV2.get({
        sessionID: session.id,
        messageID: msg.info.id,
      })
      const text = stored.parts.filter((part) => part.type === "text").map((part) => part.text)

      expect(text[0]?.startsWith("Called the Read tool with the following input:")).toBe(true)
      expect(text[1]?.includes("Read tool failed to read")).toBe(true)
      expect(text[2]).toBe("after-file")

      yield* sessions.remove(session.id)
    }),
  { config: cfg },
)

// Special characters in filenames

noLLMServer.instance(
  "handles filenames with # character",
  () =>
    Effect.gen(function* () {
      const { directory: dir } = yield* TestInstance
      yield* writeText(path.join(dir, "file#name.txt"), "special content\n")

      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const parts = yield* prompt.resolvePromptParts("Read @file#name.txt")
      const fileParts = parts.filter((part) => part.type === "file")

      expect(fileParts.length).toBe(1)
      expect(fileParts[0].filename).toBe("file#name.txt")
      expect(fileParts[0].url).toContain("%23")

      const decodedPath = fileURLToPath(fileParts[0].url)
      expect(decodedPath).toBe(path.join(dir, "file#name.txt"))

      const message = yield* prompt.prompt({
        sessionID: session.id,
        parts,
        noReply: true,
      })
      const stored = yield* MessageV2.get({ sessionID: session.id, messageID: message.info.id })
      const textParts = stored.parts.filter((part) => part.type === "text")
      const hasContent = textParts.some((part) => part.text.includes("special content"))
      expect(hasContent).toBe(true)

      yield* sessions.remove(session.id)
    }),
  { git: true, config: cfg },
)

// Regression: empty assistant turn loop

it.instance("does not loop empty assistant turns for a simple reply", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt regression" })

    yield* llm.text("packages/opencode/src/session/processor.ts")

    const result = yield* prompt.prompt({
      sessionID: session.id,
      agent: "build",
      parts: [{ type: "text", text: "Where is SessionProcessor?" }],
    })

    expect(result.info.role).toBe("assistant")
    expect(result.parts.some((part) => part.type === "text" && part.text.includes("processor.ts"))).toBe(true)

    const msgs = yield* sessions.messages({ sessionID: session.id })
    expect(msgs.filter((msg) => msg.info.role === "assistant")).toHaveLength(1)
    expect(yield* llm.calls).toBe(1)
  }),
)

it.instance("records aborted errors when prompt is cancelled mid-stream", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig(providerCfg)
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const session = yield* sessions.create({ title: "Prompt cancel regression" })

    yield* llm.hang

    const fiber = yield* prompt
      .prompt({
        sessionID: session.id,
        agent: "build",
        parts: [{ type: "text", text: "Cancel me" }],
      })
      .pipe(Effect.forkChild)

    yield* llm.wait(1)
    yield* waitForBusy(session.id)
    yield* prompt.cancel(session.id)

    const exit = yield* Fiber.await(fiber)
    expect(Exit.isSuccess(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      expect(exit.value.info.role).toBe("assistant")
      if (exit.value.info.role === "assistant") {
        expect(exit.value.info.error?.name).toBe("MessageAbortedError")
      }
    }

    const msgs = yield* sessions.messages({ sessionID: session.id })
    const last = msgs.findLast((msg) => msg.info.role === "assistant")
    expect(last?.info.role).toBe("assistant")
    if (last?.info.role === "assistant") {
      expect(last.info.error?.name).toBe("MessageAbortedError")
    }
  }),
)

// Agent variant

noLLMServer.instance(
  "applies agent variant only when using agent model",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})

      const other = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        model: { providerID: ProviderV2.ID.make("opencode"), modelID: ModelV2.ID.make("kimi-k2.5-free") },
        noReply: true,
        parts: [{ type: "text", text: "hello" }],
      })
      if (other.info.role !== "user") throw new Error("expected user message")
      expect(other.info.model.variant).toBeUndefined()

      const match = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        parts: [{ type: "text", text: "hello again" }],
      })
      if (match.info.role !== "user") throw new Error("expected user message")
      expect(match.info.model).toEqual({
        providerID: ProviderV2.ID.make("test"),
        modelID: ModelV2.ID.make("test-model"),
        variant: "xhigh",
      })
      expect(match.info.model.variant).toBe("xhigh")

      const override = yield* prompt.prompt({
        sessionID: session.id,
        agent: "build",
        noReply: true,
        variant: "high",
        parts: [{ type: "text", text: "hello third" }],
      })
      if (override.info.role !== "user") throw new Error("expected user message")
      expect(override.info.model.variant).toBe("high")

      yield* sessions.remove(session.id)
    }),
  {
    config: {
      ...cfg,
      provider: {
        ...cfg.provider,
        test: {
          ...cfg.provider.test,
          models: {
            "test-model": {
              ...cfg.provider.test.models["test-model"],
              variants: { xhigh: {}, high: {} },
            },
          },
        },
      },
      agent: {
        build: {
          model: "test/test-model",
          variant: "xhigh",
        },
      },
    },
  },
)

// Agent / command resolution errors

noLLMServer.instance(
  "unknown agent throws typed error",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "nonexistent-agent-xyz",
          noReply: true,
          parts: [{ type: "text", text: "hello" }],
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(err).not.toBeInstanceOf(TypeError)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain('Agent not found: "nonexistent-agent-xyz"')
        }
      }
    }),
  30_000,
)

noLLMServer.instance(
  "unknown agent error includes available agent names",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .prompt({
          sessionID: session.id,
          agent: "nonexistent-agent-xyz",
          noReply: true,
          parts: [{ type: "text", text: "hello" }],
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain("build")
        }
      }
    }),
  30_000,
)

noLLMServer.instance(
  "unknown command throws typed error with available names",
  () =>
    Effect.gen(function* () {
      const prompt = yield* SessionPrompt.Service
      const sessions = yield* Session.Service
      const session = yield* sessions.create({})
      const exit = yield* prompt
        .command({
          sessionID: session.id,
          command: "nonexistent-command-xyz",
          arguments: "",
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const err = Cause.squash(exit.cause)
        expect(err).not.toBeInstanceOf(TypeError)
        expect(NamedError.Unknown.isInstance(err)).toBe(true)
        if (NamedError.Unknown.isInstance(err)) {
          expect(err.data.message).toContain('Command not found: "nonexistent-command-xyz"')
          expect(err.data.message).toContain("init")
        }
      }
    }),
  30_000,
)
