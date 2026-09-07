/** @jsxImportSource @opentui/solid */
import { testRender } from "@opentui/solid"
import { expect, test } from "bun:test"
import { PromptStashProvider, parsePromptStash, usePromptStash } from "../../src/prompt/stash"
import { TestTuiContexts } from "../fixture/tui-environment"
import { tmpdir } from "../fixture/fixture"

async function wait(check: () => boolean | Promise<boolean>) {
  for (let count = 0; count < 200; count++) {
    if (await check()) return
    await Bun.sleep(5)
  }
  throw new Error("Timed out waiting for stash persistence")
}

test("two providers sharing a stash file preserve each other's ordinary appends", async () => {
  await using tmp = await tmpdir()
  const file = Bun.file(`${tmp.path}/prompt-stash.jsonl`)
  await Bun.write(file, JSON.stringify({ input: "existing", parts: [], timestamp: 1 }) + "\n")
  const providers: ReturnType<typeof usePromptStash>[] = []
  function Probe() {
    providers.push(usePromptStash())
    return <box />
  }
  const app = await testRender(() => (
    <TestTuiContexts paths={{ state: tmp.path }}>
      <PromptStashProvider>
        <Probe />
      </PromptStashProvider>
      <PromptStashProvider>
        <Probe />
      </PromptStashProvider>
    </TestTuiContexts>
  ))
  try {
    await wait(() => providers.length === 2 && providers.every((provider) => provider.list().length === 1))
    providers[0].push({ input: "first provider", parts: [] })
    await wait(async () => parsePromptStash(await file.text()).some((entry) => entry.input === "first provider"))
    // The second provider still has the snapshot loaded before the first push.
    expect(providers[1].list().map((entry) => entry.input)).toEqual(["existing"])
    providers[1].push({ input: "second provider", parts: [] })
    await wait(async () => parsePromptStash(await file.text()).some((entry) => entry.input === "second provider"))
    expect(parsePromptStash(await file.text()).map((entry) => entry.input)).toEqual([
      "existing",
      "first provider",
      "second provider",
    ])
    providers[0].push({ input: "first again", parts: [] })
    providers[1].push({ input: "second again", parts: [] })
    await wait(async () => parsePromptStash(await file.text()).length === 5)
    expect(
      parsePromptStash(await file.text())
        .map((entry) => entry.input)
        .sort(),
    ).toEqual(["existing", "first again", "first provider", "second again", "second provider"])
  } finally {
    app.renderer.destroy()
  }
})

test("queued mutation snapshots do not include and duplicate a later append", async () => {
  await using tmp = await tmpdir()
  const file = Bun.file(`${tmp.path}/prompt-stash.jsonl`)
  await Bun.write(file, JSON.stringify({ input: "existing", parts: [], timestamp: 1 }) + "\n")
  let stash!: ReturnType<typeof usePromptStash>
  function Probe() {
    stash = usePromptStash()
    stash.push({ input: "during load", parts: [] })
    return <box />
  }
  const app = await testRender(() => (
    <TestTuiContexts paths={{ state: tmp.path }}>
      <PromptStashProvider>
        <Probe />
      </PromptStashProvider>
    </TestTuiContexts>
  ))
  try {
    await wait(() => stash.list().length === 2)
    stash.push({ input: "temporary", parts: [] })
    stash.pop()
    stash.remove(0)
    stash.push({ input: "last", parts: [] })
    await wait(async () => parsePromptStash(await file.text()).at(-1)?.input === "last")
    expect(parsePromptStash(await file.text()).map((entry) => entry.input)).toEqual(["during load", "last"])
  } finally {
    app.renderer.destroy()
  }
})
