import path from "path"
import { onMount } from "solid-js"
import { createStore, produce, unwrap } from "solid-js/store"
import { createSimpleContext } from "../context/helper"
import { useTuiPaths } from "../context/runtime"
import { appendText, readText, writeText } from "../util/persistence"
import type { PromptInfo } from "./history"

export type StashEntry = {
  input: string
  parts: PromptInfo["parts"]
  timestamp: number
}

export const MAX_STASH_ENTRIES = 50

export function parsePromptStash(text: string) {
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line) as StashEntry
      } catch {
        return undefined
      }
    })
    .filter((line): line is StashEntry => line !== undefined)
    .slice(-MAX_STASH_ENTRIES)
}

export const { use: usePromptStash, provider: PromptStashProvider } = createSimpleContext({
  name: "PromptStash",
  init: () => {
    const paths = useTuiPaths()
    const stashPath = path.join(paths.state, "prompt-stash.jsonl")
    let loaded = false
    let writing = Promise.resolve()

    function persist(append?: StashEntry[]) {
      if (!loaded) return
      const entries = append ?? store.entries
      const text = entries.map((line) => JSON.stringify(line)).join("\n") + (entries.length ? "\n" : "")
      // Capture mutations now, then serialize them with ordinary appends.
      writing = writing.then(() => (append ? appendText(stashPath, text) : writeText(stashPath, text))).catch(() => {})
    }

    onMount(async () => {
      const text = await readText(stashPath).catch(() => "")
      const lines = parsePromptStash(text)
      const pending = store.entries.slice()
      // A withdrawal may finish while persisted entries are still loading.
      setStore("entries", (entries) => [...lines, ...entries].slice(-MAX_STASH_ENTRIES))
      loaded = true
      const retained = lines.map((line) => JSON.stringify(line)).join("\n") + (lines.length ? "\n" : "")
      if (lines.length + pending.length > MAX_STASH_ENTRIES || text !== retained) persist()
      else if (pending.length) persist(pending)
    })

    const [store, setStore] = createStore({ entries: [] as StashEntry[] })

    return {
      list() {
        return store.entries
      },
      push(entry: Omit<StashEntry, "timestamp">) {
        const stash = structuredClone(unwrap({ ...entry, timestamp: Date.now() }))
        const trimmed = store.entries.length >= MAX_STASH_ENTRIES
        setStore(
          produce((draft) => {
            draft.entries.push(stash)
            if (draft.entries.length > MAX_STASH_ENTRIES) {
              draft.entries = draft.entries.slice(-MAX_STASH_ENTRIES)
            }
          }),
        )

        persist(trimmed ? undefined : [stash])
      },
      pop() {
        if (store.entries.length === 0) return undefined
        const entry = store.entries[store.entries.length - 1]
        setStore(produce((draft) => void draft.entries.pop()))
        persist()
        return entry
      },
      remove(index: number) {
        if (index < 0 || index >= store.entries.length) return
        setStore(produce((draft) => void draft.entries.splice(index, 1)))
        persist()
      },
    }
  },
})
