import { mkdtempSync } from "node:fs";
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
function bridge(store: ThreadStore) {
  const created = new Bridge(
    new AppServer(command, 2_000),
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
    prompt: "instant",
    serviceTier: "fast",
    effort: "high",
  });
  await until(() => store.read(session.threadId)?.status === "completed");
  expect(store.read(session.threadId)).toMatchObject({
    lastAnswer: expect.any(String),
    serviceTier: "fast",
    effort: "high",
  });

  const second = bridge(store);
  const listed = second.status().sessions;
  expect(listed).toEqual([
    expect.objectContaining({
      threadId: session.threadId,
      status: "completed",
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
  const continued = await second.message(session.threadId, "again");
  expect(continued.threadId).toBe(session.threadId);
  expect(second.status(session.threadId).sessions[0].attached).toBe(true);
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
  expect(await wait(["--home", store.home], 5)).toBe(64);
  expect(error).toHaveBeenCalled();
});
