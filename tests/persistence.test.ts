import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
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

test("service tier status reports the server value and keeps the request separately", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const first = bridge(store, "null-tier");
  const session = await first.start({
    cwd: process.cwd(),
    prompt: "work",
    serviceTier: "fast",
  });
  expect(session).toMatchObject({
    serviceTier: null,
    requestedServiceTier: "fast",
  });
  let state = JSON.parse(
    readFileSync(join(store.home, "app-server-state.json"), "utf8"),
  );
  expect(
    state.calls.find((call: { method: string }) => call.method === "turn/start")
      .params,
  ).not.toHaveProperty("serviceTier");
  await first.close();

  const second = bridge(store, "null-tier");
  const resumed = await second.resume(session.threadId);
  expect(resumed).toMatchObject({
    serviceTier: null,
    requestedServiceTier: "fast",
  });
  await second.message(session.threadId, "work");
  state = JSON.parse(
    readFileSync(join(store.home, "app-server-state.json"), "utf8"),
  );
  expect(
    state.calls.find(
      (call: { method: string }) => call.method === "thread/resume",
    ).params.serviceTier,
  ).toBe("fast");
  expect(
    state.calls
      .filter((call: { method: string }) => call.method === "turn/start")
      .at(-1).params,
  ).not.toHaveProperty("serviceTier");
});

test("reported effort stays effective while the requested effort remains visible", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store, "reported-effort");
  const session = await created.start({
    cwd: process.cwd(),
    prompt: "work",
    effort: "high",
  });
  expect(session).toMatchObject({ effort: "medium", requestedEffort: "high" });
  await created.close();
  const resumed = await bridge(store, "reported-effort").resume(
    session.threadId,
  );
  expect(resumed).toMatchObject({ effort: "medium", requestedEffort: "high" });
});

test("resume drops a saved diagnostic when the returned latest turn is different", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const first = bridge(store);
  const session = await first.start({ cwd: process.cwd(), prompt: "late" });
  await first.close();
  const saved = store.read(session.threadId);
  if (!saved) throw new Error("Missing saved session");
  store.write({
    ...saved,
    lastTurnId: "older-turn",
    error: "old shutdown error",
  });

  const resumed = await bridge(store).resume(session.threadId);
  expect(resumed.status).toBe("interrupted");
  expect(resumed.error).toBeNull();

  const sameTurnStore = new ThreadStore(
    mkdtempSync(join(tmpdir(), "bridge-store-")),
  );
  const sameTurnSource = bridge(sameTurnStore, "resume-error");
  const sameTurnSession = await sameTurnSource.start({
    cwd: process.cwd(),
    prompt: "late",
  });
  await sameTurnSource.close();
  const sameTurnSaved = sameTurnStore.read(sameTurnSession.threadId);
  if (!sameTurnSaved) throw new Error("Missing same-turn record");
  sameTurnStore.write({ ...sameTurnSaved, error: "older diagnostic" });
  const sameTurn = await bridge(sameTurnStore, "resume-error").resume(
    sameTurnSession.threadId,
  );
  expect(sameTurn.error).toBe("latest returned turn error");
});

test("persisted diagnostics are capped and report truncation through wait", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store);
  await expect(
    created.start({ cwd: process.cwd(), prompt: "huge-error" }),
  ).rejects.toThrow("x".repeat(100));
  const failed = store.list().find((record) => record.status === "failed");
  if (!failed) throw new Error("Missing persisted error");
  expect(failed.error).toHaveLength(64_000);
  expect(failed.errorTruncated).toBe(true);

  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  expect(await wait(["--home", store.home, failed.threadId], 5)).toBe(0);
  expect(log.mock.calls[0][0]).toContain('"errorTruncated":true');
});

test("shutdown preserves completion received before acceptance returns", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store);
  const starting = created
    .start({ cwd: process.cwd(), prompt: "complete-pending" })
    .catch((error: Error) => error);
  await until(() => store.list()[0]?.status === "completed");
  await created.close();
  await starting;
  expect(store.list()[0]).toMatchObject({
    status: "completed",
    lastAnswer: "finished before acceptance",
    error: null,
  });
});

test("acceptance continuation cannot overwrite interrupted state after shutdown", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const created = bridge(store);
  type RpcCall = (method: string, params: object) => Promise<unknown>;
  const rpc = (created as unknown as { rpc: { call: RpcCall } }).rpc;
  const originalCall = rpc.call.bind(rpc);
  rpc.call = async (method, params) => {
    const result = await originalCall(method, params);
    if (method === "turn/start") await created.close();
    return result;
  };
  await created
    .start({ cwd: process.cwd(), prompt: "pending" })
    .catch(() => undefined);
  expect(store.list()[0]?.status).toBe("interrupted");
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
    JSON.stringify({
      ...orphan,
      ownerPid: 2147483647,
      ownerHost: hostname(),
      ownerStartId: "old process incarnation",
    }),
  );
  expect(await wait(["--home", store.home, "t3"], 5)).toBe(3);
  expect(store.read("t3")).toMatchObject({
    status: "running",
    ownerPid: 2147483647,
  });
  expect(await wait(["--home", store.home], 5)).toBe(64);
  expect(error).toHaveBeenCalled();
});

test("wait never overwrites a newer record when an owner disappears during its check", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const record = {
    threadId: "owner-race",
    cwd: "/",
    model: "m",
    status: "running",
    lastTurnId: "old-turn",
    lastAnswer: "old answer",
    answerTruncated: false,
  };
  store.write(record);
  const originalKill = process.kill.bind(process);
  let replaced = false;
  vi.spyOn(process, "kill").mockImplementation(((
    pid: number,
    signal?: NodeJS.Signals | number,
  ) => {
    if (pid === 2147483647 && !replaced) {
      replaced = true;
      store.write({
        ...record,
        serviceTier: "fast",
        effort: "high",
        status: "running",
        lastTurnId: "new-turn",
        lastAnswer: "new answer",
      });
      throw Object.assign(new Error("no such process"), { code: "ESRCH" });
    }
    return originalKill(pid, signal);
  }) as typeof process.kill);
  const old = store.read("owner-race");
  if (!old) throw new Error("Missing race record");
  writeFileSync(
    store.path("owner-race"),
    JSON.stringify({
      ...old,
      ownerPid: 2147483647,
      ownerHost: hostname(),
      ownerStartId: "gone process incarnation",
    }),
  );

  const error = vi.spyOn(console, "error").mockImplementation(() => {});
  expect(await wait(["--home", store.home, "owner-race"], 5)).toBe(3);
  expect(store.read("owner-race")).toMatchObject({
    serviceTier: "fast",
    effort: "high",
    status: "running",
    lastTurnId: "new-turn",
    lastAnswer: "new answer",
  });
  expect(error).toHaveBeenCalledWith(expect.stringContaining("owner gone"));
});

test("wait treats reused, foreign, and legacy owners as gone or unknown safely", async () => {
  const store = new ThreadStore(mkdtempSync(join(tmpdir(), "bridge-store-")));
  const base = {
    threadId: "owner",
    cwd: "/",
    model: "m",
    status: "running",
    lastTurnId: "turn",
    lastAnswer: "",
    answerTruncated: false,
  };
  const error = vi.spyOn(console, "error").mockImplementation(() => {});
  store.write({ ...base, threadId: "reused" });
  const reused = store.read("reused");
  if (!reused) throw new Error("Missing reused PID record");
  writeFileSync(
    store.path("reused"),
    JSON.stringify({
      ...reused,
      ownerPid: process.pid,
      ownerHost: hostname(),
      ownerStartId: "different process incarnation",
    }),
  );
  expect(
    await wait(["--home", store.home, "--timeout", "0", "reused"], 5),
  ).toBe(3);
  expect(store.read("reused")?.status).toBe("running");

  store.write({ ...base, threadId: "foreign" });
  const foreign = store.read("foreign");
  if (!foreign) throw new Error("Missing foreign owner record");
  writeFileSync(
    store.path("foreign"),
    JSON.stringify({
      ...foreign,
      ownerPid: 2147483647,
      ownerHost: "another-host",
    }),
  );
  expect(
    await wait(["--home", store.home, "--timeout", "0", "foreign"], 5),
  ).toBe(4);
  expect(store.read("foreign")?.status).toBe("running");

  store.write({ ...base, threadId: "legacy" });
  const legacy = store.read("legacy");
  if (!legacy) throw new Error("Missing legacy owner record");
  const { ownerHost: _host, ownerStartId: _startId, ...legacyRecord } = legacy;
  writeFileSync(store.path("legacy"), JSON.stringify(legacyRecord));
  expect(
    await wait(["--home", store.home, "--timeout", "0", "legacy"], 5),
  ).toBe(4);
  expect(store.read("legacy")?.status).toBe("running");
  expect(error).toHaveBeenCalledWith(
    expect.stringContaining("owner status unknown"),
  );
});
