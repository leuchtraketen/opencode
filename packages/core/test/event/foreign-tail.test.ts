import { afterAll, describe, expect, test } from "bun:test"
import { Database as SqliteDatabase } from "bun:sqlite"
import { Deferred, Effect, Fiber, Layer, Stream } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { ForeignTail } from "@opencode-ai/core/event/foreign-tail"
import { Database } from "@opencode-ai/core/database/database"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { Session } from "@opencode-ai/schema/session"
import { SessionV1 } from "@opencode-ai/schema/session-v1"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { eq } from "drizzle-orm"
import path from "path"
import { tmpdir } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

const previousInterval = process.env[ForeignTail.EnvVar]
process.env[ForeignTail.EnvVar] = "50"

const tmp = await tmpdir()
const filename = path.join(tmp.path, "foreign-tail.sqlite")

afterAll(async () => {
  if (previousInterval === undefined) delete process.env[ForeignTail.EnvVar]
  if (previousInterval !== undefined) process.env[ForeignTail.EnvVar] = previousInterval
  await tmp[Symbol.asyncDispose]()
})

// A fresh EventV2 (and connection) per test, all on the same file: rows left behind by an earlier test
// must be treated as history by the next test's tail.
const it = testEffect(EventV2.layerWith().pipe(Layer.provideMerge(Database.layerFromPath(filename))))

const messageUpdated = (sessionID: Session.ID, messageID: SessionV1.MessageID) => ({
  sessionID,
  info: {
    id: messageID,
    sessionID,
    role: "user" as const,
    time: { created: 1 },
    agent: "build",
    model: { providerID: ProviderV2.ID.make("openai"), modelID: ModelV2.ID.make("gpt-5") },
  },
})

const partUpdated = (sessionID: Session.ID, messageID: SessionV1.MessageID, partID: SessionV1.PartID) => ({
  sessionID,
  part: { id: partID, sessionID, messageID, type: "text" as const, text: "written by another process" },
  time: 2,
})

type ForeignRow = {
  readonly aggregateID: string
  readonly seq: number
  readonly type: string
  readonly data: Record<string, unknown>
  readonly entityID: string
}

/** Commits rows the way an external tool would: a second connection, one transaction, no EventV2 involved. */
function commitForeign(rows: ForeignRow[]) {
  const foreign = new SqliteDatabase(filename)
  foreign.run("PRAGMA busy_timeout = 5000")
  foreign.transaction(() => {
    for (const row of rows) {
      foreign.run(
        "insert into event_sequence (aggregate_id, seq, owner_id) values (?, ?, null) on conflict (aggregate_id) do update set seq = excluded.seq",
        [row.aggregateID, row.seq],
      )
      foreign.run("insert into event (id, aggregate_id, seq, type, data, entity_id) values (?, ?, ?, ?, ?, ?)", [
        EventV2.ID.create(),
        row.aggregateID,
        row.seq,
        row.type,
        JSON.stringify(row.data),
        row.entityID,
      ])
    }
  })()
  foreign.close()
}

describe("EventV2 foreign event tail", () => {
  test("interval is read from the environment and disabled by zero or garbage", () => {
    expect(ForeignTail.interval(undefined)).toBe(ForeignTail.DefaultIntervalMs)
    expect(ForeignTail.interval("")).toBe(ForeignTail.DefaultIntervalMs)
    expect(ForeignTail.interval("50")).toBe(50)
    expect(ForeignTail.interval("0")).toBeUndefined()
    expect(ForeignTail.interval("-5")).toBeUndefined()
    expect(ForeignTail.interval("soon")).toBeUndefined()
  })

  it.live("rebroadcasts rows committed by another connection without inserting anything", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const sessionID = Session.ID.create()
      const messageID = SessionV1.MessageID.ascending()
      const partID = SessionV1.PartID.ascending()
      const received = new Array<EventV2.Payload>()
      const both = yield* Deferred.make<void>()
      yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type !== SessionV1.Event.MessageUpdated.type && event.type !== SessionV1.Event.PartUpdated.type)
            return
          received.push(event)
          if (received.length === 2) Deferred.doneUnsafe(both, Effect.void)
        }),
      )
      const typed = yield* events
        .subscribe(SessionV1.Event.PartUpdated)
        .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped)
      const durable = yield* events
        .durable({ aggregateID: sessionID })
        .pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)
      yield* Effect.yieldNow

      commitForeign([
        {
          aggregateID: sessionID,
          seq: 0,
          type: EventV2.versionedType(SessionV1.Event.MessageUpdated.type, 1),
          data: messageUpdated(sessionID, messageID),
          entityID: messageID,
        },
        {
          aggregateID: sessionID,
          seq: 1,
          type: EventV2.versionedType(SessionV1.Event.PartUpdated.type, 1),
          data: partUpdated(sessionID, messageID, partID),
          entityID: partID,
        },
      ])
      yield* Deferred.await(both).pipe(Effect.timeout("1 second"))

      expect(received.map((event) => [event.type, event.durable?.seq, event.durable?.aggregateID])).toEqual([
        [SessionV1.Event.MessageUpdated.type, 0, sessionID],
        [SessionV1.Event.PartUpdated.type, 1, sessionID],
      ])
      expect(received[0]?.data).toEqual(messageUpdated(sessionID, messageID))
      expect(received[1]?.data).toEqual(partUpdated(sessionID, messageID, partID))
      expect(Array.from(yield* Fiber.join(typed)).map((event) => event.data)).toEqual([
        partUpdated(sessionID, messageID, partID),
      ])
      expect(Array.from(yield* Fiber.join(durable)).map((event) => event.durable?.seq)).toEqual([0, 1])

      const rows = yield* db
        .select({ seq: EventTable.seq, type: EventTable.type })
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, sessionID))
        .orderBy(EventTable.seq)
        .all()
        .pipe(Effect.orDie)
      const sequence = yield* db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      expect(rows).toEqual([
        { seq: 0, type: "message.updated.1" },
        { seq: 1, type: "message.part.updated.1" },
      ])
      expect(sequence).toEqual({ seq: 1 })
    }),
  )

  it.live("delivers a locally published durable event once and never replays history", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const local = Session.ID.create()
      const foreign = Session.ID.create()
      const received = new Array<EventV2.Payload>()
      const arrived = yield* Deferred.make<void>()
      yield* events.all().pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            received.push(event)
            if (event.durable?.aggregateID === foreign) Deferred.doneUnsafe(arrived, Effect.void)
          }),
        ),
        Effect.forkScoped,
      )
      yield* Effect.yieldNow

      yield* events.publish(SessionV1.Event.MessageUpdated, messageUpdated(local, SessionV1.MessageID.ascending()))
      const messageID = SessionV1.MessageID.ascending()
      // A foreign commit changes the data version, so the next tick compares every aggregate -- including
      // the one this process just wrote to.
      commitForeign([
        {
          aggregateID: foreign,
          seq: 0,
          type: EventV2.versionedType(SessionV1.Event.MessageUpdated.type, 1),
          data: messageUpdated(foreign, messageID),
          entityID: messageID,
        },
      ])
      yield* Deferred.await(arrived).pipe(Effect.timeout("1 second"))
      yield* Effect.sleep("200 millis")

      expect(received.filter((event) => event.durable?.aggregateID === local)).toHaveLength(1)
      expect(received.filter((event) => event.durable?.aggregateID === foreign)).toHaveLength(1)
      expect(received).toHaveLength(2)
    }),
  )

  it.live("sees an aggregate another process removed and recreated under the same id from seq 0 again", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const sessionID = Session.ID.create()
      const received = new Array<EventV2.Payload>()
      const recreated = yield* Deferred.make<void>()
      yield* events.all().pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.durable?.aggregateID !== sessionID) return
            received.push(event)
            if (received.length === 3) Deferred.doneUnsafe(recreated, Effect.void)
          }),
        ),
        Effect.forkScoped,
      )
      yield* Effect.yieldNow

      // This process writes seq 0 and 1 itself: its local watermark for the aggregate is 1.
      yield* events.publish(SessionV1.Event.MessageUpdated, messageUpdated(sessionID, SessionV1.MessageID.ascending()))
      yield* events.publish(SessionV1.Event.MessageUpdated, messageUpdated(sessionID, SessionV1.MessageID.ascending()))

      // Another process deletes the aggregate ...
      const foreign = new SqliteDatabase(filename)
      foreign.run("PRAGMA busy_timeout = 5000")
      foreign.run("delete from event where aggregate_id = ?", [sessionID])
      foreign.run("delete from event_sequence where aggregate_id = ?", [sessionID])
      foreign.close()
      yield* Effect.sleep("200 millis")

      // ... and recreates it: seq 0 again, at or below the old local watermark.
      const messageID = SessionV1.MessageID.ascending()
      commitForeign([
        {
          aggregateID: sessionID,
          seq: 0,
          type: EventV2.versionedType(SessionV1.Event.MessageUpdated.type, 1),
          data: messageUpdated(sessionID, messageID),
          entityID: messageID,
        },
      ])
      yield* Deferred.await(recreated).pipe(Effect.timeout("1 second"))

      expect(received.map((event) => event.durable?.seq)).toEqual([0, 1, 0])
      expect(received[2]?.data).toEqual(messageUpdated(sessionID, messageID))
    }),
  )
})
