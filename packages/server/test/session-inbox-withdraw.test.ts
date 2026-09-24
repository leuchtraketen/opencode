import { expect, setDefaultTimeout } from "bun:test"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionMessage } from "@opencode/core/session/message"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Effect, Layer } from "effect"
import { tmpdir } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

setDefaultTimeout(30_000)

type InboxItem = { id: string; type: string; delivery: string; payload: { text: string } }

it.live("withdraws queued prompts through POST /api/session/:sessionID/inbox/withdraw", () =>
  Effect.gen(function* () {
    const tmp = yield* Effect.acquireDisposable(Effect.promise(() => tmpdir("opencode-inbox-withdraw-")))
    // Execution never runs, so admitted prompts stay pending in the inbox.
    const execution = Layer.succeed(
      SessionExecution.Service,
      SessionExecution.Service.of({
        active: Effect.succeed(new Set()),
        isActive: () => Effect.succeed(false),
        resume: () => Effect.void,
        wake: () => Effect.void,
        interrupt: () => Effect.succeed(false),
        awaitIdle: () => Effect.void,
      }),
    )
    const handler = yield* ServerFetch.make(
      {
        app: { version: "test-version" },
        database: { path: ":memory:" },
        fs: { filewatcher: false },
        models: { fetch: false },
      },
      {
        overrides: [
          SessionExecution.node.replace(
            makeGlobalNode({ service: SessionExecution.Service, layer: execution, deps: [] }),
          ),
        ],
      },
    )
    const request = (path: string, body?: unknown) =>
      Effect.promise(async () => {
        const response = await handler(
          new Request(`http://opencode.local${path}`, {
            method: body === undefined ? "GET" : "POST",
            headers: body === undefined ? undefined : { "content-type": "application/json" },
            body: body === undefined ? undefined : JSON.stringify(body),
          }),
        )
        return { status: response.status, body: (await response.json()) as Record<string, unknown> }
      })
    const created = yield* request("/api/session", { location: { directory: tmp.path } })
    const sessionID = Session.ID.make((created.body.data as { id: string }).id)
    const prompt = (id: SessionMessage.ID, text: string, delivery: "queue" | "steer") =>
      request(`/api/session/${sessionID}/prompt`, { id, text, delivery, resume: false })
    const withdraw = (requestID: unknown) => request(`/api/session/${sessionID}/inbox/withdraw`, { requestID })
    const inbox = () =>
      request(`/api/session/${sessionID}/inbox`).pipe(
        Effect.map((response) => (response.body.data as InboxItem[]).map((item) => item.id)),
      )
    const ids = {
      steered: SessionMessage.ID.create(),
      first: SessionMessage.ID.create(),
      second: SessionMessage.ID.create(),
      later: SessionMessage.ID.create(),
    }

    expect((yield* prompt(ids.steered, "steered", "steer")).status).toBe(200)
    expect((yield* prompt(ids.first, "first", "queue")).status).toBe(200)
    expect((yield* prompt(ids.second, "second", "queue")).status).toBe(200)
    expect(yield* inbox()).toEqual([ids.steered, ids.first, ids.second])

    const withdrawn = yield* withdraw("req_1")
    expect(withdrawn.status).toBe(200)
    expect(withdrawn.body.data).toMatchObject([
      { id: ids.first, type: "user", delivery: "queue", payload: { text: "first" } },
      { id: ids.second, type: "user", delivery: "queue", payload: { text: "second" } },
    ])
    expect(yield* inbox()).toEqual([ids.steered])

    // Replaying the request returns the original receipt without touching newer prompts.
    expect((yield* prompt(ids.later, "later", "queue")).status).toBe(200)
    expect(yield* withdraw("req_1")).toEqual(withdrawn)
    expect(yield* inbox()).toEqual([ids.steered, ids.later])

    const other = yield* withdraw("req_2")
    expect(other.status).toBe(200)
    expect(other.body.data).toMatchObject([{ id: ids.later, payload: { text: "later" } }])
    expect(yield* inbox()).toEqual([ids.steered])
    expect(yield* withdraw("req_3")).toEqual({ status: 200, body: { data: [] } })

    expect(yield* withdraw("")).toMatchObject({ status: 400 })
    expect(yield* withdraw("x".repeat(129))).toMatchObject({ status: 400 })
    expect(yield* request(`/api/session/${sessionID}/inbox/withdraw`, {})).toMatchObject({ status: 400 })
    expect(yield* request(`/api/session/${Session.ID.create()}/inbox/withdraw`, { requestID: "req_1" })).toMatchObject({
      status: 404,
      body: { _tag: "SessionNotFoundError" },
    })
  }),
)
