import { sqliteTable, text, integer, index, uniqueIndex } from "drizzle-orm/sqlite-core"
import type { EventV2 } from "../event"

export const EventSequenceTable = sqliteTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: integer().notNull(),
  owner_id: text(),
})

export const EventTable = sqliteTable(
  "event",
  {
    id: text().$type<EventV2.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    type: text().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
    // Identity of the entity a snapshot-style event describes (`Event.DurableOptions.supersedes`), so the
    // previous snapshot of the same entity can be found through the index below instead of parsing `data`.
    // NULL for events without `supersedes` and for rows written before the column existed; those are never
    // collapsed (a backfill would rewrite every multi-megabyte row of an unbounded log, so it is deliberately
    // not done by the migration).
    entity_id: text(),
  },
  (table) => [
    uniqueIndex("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    index("event_aggregate_type_seq_idx").on(table.aggregate_id, table.type, table.seq),
    index("event_aggregate_type_entity_idx").on(table.aggregate_id, table.type, table.entity_id),
  ],
)
