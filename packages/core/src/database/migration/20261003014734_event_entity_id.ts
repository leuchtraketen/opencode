import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261003014734_event_entity_id",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`event\` ADD \`entity_id\` text;`)
      yield* tx.run(
        `CREATE INDEX \`event_aggregate_type_entity_idx\` ON \`event\` (\`aggregate_id\`,\`type\`,\`entity_id\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
