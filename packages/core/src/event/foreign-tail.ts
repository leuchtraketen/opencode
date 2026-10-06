export * as ForeignTail from "./foreign-tail"

import { Cause, Duration, Effect, Schedule } from "effect"
import { and, asc, eq, gt } from "drizzle-orm"
import type { Database } from "../database/database"
import type { SerializedEvent } from "../event"
import { Flag } from "../flag/flag"
import { EventSequenceTable, EventTable } from "./sql"

/** Poll interval in milliseconds; unset uses the default, `0` or anything that is not a positive number disables the tail. */
export const EnvVar = "OPENCODE_FOREIGN_EVENT_TAIL_MS"

export const DefaultIntervalMs = 1500

export function interval(value: string | undefined) {
  if (value === undefined || value.trim() === "") return DefaultIntervalMs
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) return undefined
  return parsed
}

export interface Target {
  readonly db: Database.Interface["db"]
  /** Highest sequence this process committed for the aggregate, if any. */
  readonly localSeq: (aggregateID: string) => number | undefined
  /** Forget the local watermarks of aggregates no longer in `event_sequence` (removed by another process), so a recreated one is seen from seq 0. */
  readonly pruneLocal: (present: ReadonlySet<string>) => void
  readonly rebroadcast: (rows: SerializedEvent[]) => Effect.Effect<void>
}

/**
 * Watches the shared `event` table for rows committed by other processes (another opencode, an external
 * tool) and rebroadcasts them to this process's live listeners. `PRAGMA data_version` only changes when a
 * different connection commits, so an idle or self-writing process pays one pragma per tick. Positions are
 * seeded from `event_sequence` at start, so history is never replayed; the caller's own commits are
 * excluded through `localSeq`.
 */
export const start = Effect.fn("EventV2.ForeignTail.start")(function* (target: Target) {
  const ms = interval(Flag.OPENCODE_FOREIGN_EVENT_TAIL_MS)
  if (ms === undefined) return
  const remembered = yield* sequences(target.db).pipe(Effect.orDie)
  let version = yield* dataVersion(target.db).pipe(Effect.orDie)

  const tick = Effect.gen(function* () {
    const current = yield* dataVersion(target.db)
    if (current === version) return
    version = current
    const latest = yield* sequences(target.db)
    // Gone from `event_sequence`: another process removed the aggregate. Forget both watermarks — the tail's and
    // the process's own (an aggregate this process wrote never entered `remembered`, own commits do not change
    // the data version) — otherwise a session recreated under the same id would be ignored until it passed the
    // old high-water mark.
    for (const aggregateID of remembered.keys()) {
      if (!latest.has(aggregateID)) remembered.delete(aggregateID)
    }
    target.pruneLocal(new Set(latest.keys()))
    for (const [aggregateID, seq] of latest) {
      const after = Math.max(remembered.get(aggregateID) ?? -1, target.localSeq(aggregateID) ?? -1)
      if (seq > after) {
        const rows = yield* target.db
          .select()
          .from(EventTable)
          .where(and(eq(EventTable.aggregate_id, aggregateID), gt(EventTable.seq, after)))
          .orderBy(asc(EventTable.seq))
          .all()
        yield* target.rebroadcast(
          rows.map((row) => ({
            id: row.id,
            aggregateID: row.aggregate_id,
            seq: row.seq,
            type: row.type,
            data: row.data,
          })),
        )
        // The sequence scan and the row read are separate snapshots: a commit between them is already in
        // `rows`, so the position is what was actually delivered, never a value behind it.
        remembered.set(aggregateID, Math.max(seq, rows.at(-1)?.seq ?? seq))
        continue
      }
      remembered.set(aggregateID, seq)
    }
  })

  yield* tick.pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterrupts(cause),
      (cause) => Effect.logWarning("Foreign event tail tick failed", { cause }),
    ),
    Effect.repeat(Schedule.spaced(Duration.millis(ms))),
    Effect.forkScoped,
  )
})

const dataVersion = (db: Database.Interface["db"]) =>
  db.get<{ data_version: number }>("PRAGMA data_version").pipe(Effect.map((row) => row?.data_version ?? 0))

const sequences = (db: Database.Interface["db"]) =>
  db
    .select({ aggregateID: EventSequenceTable.aggregate_id, seq: EventSequenceTable.seq })
    .from(EventSequenceTable)
    .all()
    .pipe(Effect.map((rows) => new Map(rows.map((row) => [row.aggregateID, row.seq]))))
