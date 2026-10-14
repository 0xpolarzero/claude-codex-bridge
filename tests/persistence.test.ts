import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, expect, test, vi } from "vitest";
import { AppServer } from "../src/app-server.ts";
import { Bridge } from "../src/bridge.ts";
import { ThreadStore } from "../src/store.ts";
import { wait } from "../src/wait.ts";

const command = [
  process.execPath,
  resolve(import.meta.dirname, "fixtures/app-server.ts"),
];
const cleanup: Bridge[] = [];
afterEach(async () => {
  await Promise.all(cleanup.splice(0).map((bridge) => bridge.close()));
  vi.restoreAllMocks();
});
function bridge(store: ThreadStore, mode?: string) {
  const fixtureState = join(store.home, "app-server-state.json");
  const created = new Bridge(
    new AppServer([...command, mode ?? "-", fixtureState], 2_000),
    async () => {},
    10,
    200,
    store,
  );
  cleanup.push(created);
  return created;
}
async function until(condition: () => boolean) {
  const deadline = Date.now() + 2_000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("Condition not met");
    await sleep(5);
  }
}
test("a restarted bridge lists saved threads and reattaches them on message", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const first = bridge(store);
  const session = await first.start({
    cwd: process.cwd(),
    prompt: "late",
    serviceTier: "fast",
    effort: "high",
  });
  expect(session.status).toBe("running");
  await first.close();
  expect(store.read(session.threadId)?.status).toBe("interrupted");
  expect(store.read(session.threadId)).toMatchObject({
    serviceTier: "fast",
    effort: "high",
  });

  const second = bridge(store);
  const listed = second.status().sessions;
  expect(listed).toEqual([
    expect.objectContaining({
      threadId: session.threadId,
      status: "interrupted",
      serviceTier: "fast",
      effort: "high",
      attached: false,
      ownerAlive: true,
    }),
  ]);
  expect(second.status(session.threadId).sessions).toHaveLength(1);
  expect(() => second.status("missing")).toThrow("Unknown thread");

  const resumed = await second.resume(session.threadId);
  expect(resumed.serviceTier).toBe("fast");
  expect(resumed.effort).toBe("high");
  const continued = await second.message(session.threadId, "effort");
  expect(continued.threadId).toBe(session.threadId);
  expect(continued.lastAnswer).toBe("effort=high");
  expect(second.status(session.threadId).sessions[0].attached).toBe(true);
  const fixtureState = JSON.parse(
    readFileSync(join(store.home, "app-server-state.json"), "utf8"),
  );
  expect(
    fixtureState.calls.find(
      (call: { method: string }) => call.method === "thread/resume",
    ).params.serviceTier,
  ).toBe("fast");
  expect(
    fixtureState.calls
      .filter((call: { method: string }) => call.method === "turn/start")
      .at(-1).params,
  ).toMatchObject({ effort: "high", serviceTier: "fast" });
});

test("concurrent messages share one reattachment and cannot replace live turn state", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const first = bridge(store);
  const session = await first.start({
    cwd: process.cwd(),
    prompt: "instant",
  });
  await first.close();

  const second = bridge(store, "slow-resume");
  const results = await Promise.allSettled([
    second.message(session.threadId, "effort"),
    second.message(session.threadId, "effort"),
  ]);
  expect(
    results.filter((result) => result.status === "fulfilled"),
  ).toHaveLength(1);
  expect(results.filter((result) => result.status === "rejected")).toHaveLength(
    1,
  );
  expect(second.status(session.threadId).sessions[0]).toMatchObject({
    status: "completed",
    lastAnswer: "effort=default",
  });
  const fixtureState = JSON.parse(
    readFileSync(join(store.home, "app-server-state.json"), "utf8"),
  );
  expect(
    fixtureState.calls.filter(
      (call: { method: string }) => call.method === "thread/resume",
    ),
  ).toHaveLength(1);
  expect(
    fixtureState.calls.filter(
      (call: { method: string; params: { input: { text: string }[] } }) =>
        call.method === "turn/start" && call.params.input[0].text === "effort",
    ),
  ).toHaveLength(1);
});

test("shutdown persists an active turn as interrupted and wait returns its diagnostic", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store);
  const session = await created.start({
    cwd: process.cwd(),
    prompt: "work",
  });
  await created.close();

  expect(store.read(session.threadId)).toMatchObject({
    status: "interrupted",
    error: expect.stringContaining("Bridge closed"),
  });
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  expect(await wait(["--home", store.home, session.threadId], 5)).toBe(0);
  expect(log).toHaveBeenCalledWith(
    expect.stringContaining('"status":"interrupted"'),
  );
  expect(log).toHaveBeenCalledWith(
    expect.stringContaining('"error":"Bridge closed'),
  );
});

test("shutdown marks a turn interrupted while its acceptance response is pending", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store);
  const starting = created.start({ cwd: process.cwd(), prompt: "pending" });
  const rejected = expect(starting).rejects.toThrow("Bridge closed");
  const fixtureStatePath = join(store.home, "app-server-state.json");
  await until(() => {
    try {
      const state = JSON.parse(readFileSync(fixtureStatePath, "utf8"));
      return state.calls.some(
        (call: { method: string }) => call.method === "turn/start",
      );
    } catch {
      return false;
    }
  });
  await created.close();
  await rejected;
  expect(store.list()[0]).toMatchObject({
    status: "interrupted",
    error: expect.stringContaining("Bridge closed"),
  });
});

test("RPC and process failures persist diagnostics for status and wait", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store);
  await expect(
    created.start({ cwd: process.cwd(), prompt: "fail" }),
  ).rejects.toThrow("turn rejected");
  const rpcFailure = store.list().find((record) => record.status === "failed");
  if (!rpcFailure) throw new Error("Missing persisted RPC failure");
  expect(rpcFailure?.error).toContain("turn rejected");
  expect(created.status(rpcFailure.threadId).sessions[0].error).toContain(
    "turn rejected",
  );
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  expect(await wait(["--home", store.home, rpcFailure.threadId], 5)).toBe(0);
  expect(log).toHaveBeenCalledWith(
    expect.stringContaining('"error":"turn rejected"'),
  );

  await expect(
    created.start({ cwd: process.cwd(), prompt: "crash" }),
  ).rejects.toThrow("exited");
  const processFailure = store
    .list()
    .find(
      (record) =>
        record.status === "unavailable" && record.error?.includes("exited"),
    );
  expect(processFailure?.error).toContain("exited");
  expect(
    await wait(["--home", store.home, processFailure?.threadId ?? ""], 5),
  ).toBe(0);
  expect(log).toHaveBeenCalledWith(
    expect.stringContaining('"error":"Codex app-server exited'),
  );
});

test("wait exits once saved threads finish and prints their answers", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  const error = vi.spyOn(console, "error").mockImplementation(() => {});
  const record = {
    threadId: "t1",
    cwd: "/",
    model: "m",
    status: "running",
    lastTurnId: "turn",
    lastAnswer: "",
    answerTruncated: false,
  };
  store.write(record);
  const pending = wait(["--home", store.home, "t1"], 5);
  await sleep(20);
  store.write({ ...record, status: "completed", lastAnswer: "done" });
  expect(await pending).toBe(0);
  expect(log).toHaveBeenCalledWith(
    expect.stringContaining('"lastAnswer":"done"'),
  );

  expect(await wait(["--home", store.home, "nope"], 5)).toBe(1);
  store.write({ ...record, threadId: "t2" });
  expect(await wait(["--home", store.home, "--timeout", "0", "t2"], 5)).toBe(2);
  store.write({ ...record, threadId: "t3" });
  const orphan = store.read("t3");
  if (!orphan) throw new Error("Missing fixture thread");
  writeFileSync(
    store.path("t3"),
    JSON.stringify({ ...orphan, ownerPid: 2147483647 }),
  );
  expect(await wait(["--home", store.home, "t3"], 5)).toBe(0);
  expect(store.read("t3")).toMatchObject({
    status: "unavailable",
    error: expect.stringContaining("owner process"),
    ownerPid: 2147483647,
  });
  expect(await wait(["--home", store.home], 5)).toBe(64);
  expect(error).toHaveBeenCalled();
});
