import { setTimeout as sleep } from "node:timers/promises";
import { processAlive, TERMINAL, ThreadStore } from "./store.ts";

/**
 * `wait [--home DIR] [--timeout SECONDS] THREAD_ID...`: exit once every thread's
 * saved status is terminal, printing each final status and answer. Run it in a
 * background shell so the shell's exit notifies the caller.
 */
export async function wait(args: string[], pollMs = 2_000) {
  let home: string | undefined;
  let timeoutSeconds = 3_600;
  const threadIds: string[] = [];
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === "--home") home = args[++index];
    else if (arg === "--timeout") timeoutSeconds = Number(args[++index]);
    else threadIds.push(arg);
  }
  if (!threadIds.length || !Number.isFinite(timeoutSeconds)) {
    console.error("usage: wait [--home DIR] [--timeout SECONDS] THREAD_ID...");
    return 64;
  }
  const store = new ThreadStore(home);
  const deadline = Date.now() + timeoutSeconds * 1_000;
  for (;;) {
    const records = threadIds.map((id) => store.read(id));
    const missing = threadIds.filter((_, index) => !records[index]);
    if (missing.length) {
      console.error(`unknown thread: ${missing.join(", ")}`);
      return 1;
    }
    for (const record of records) {
      if (
        record &&
        !TERMINAL.has(record.status) &&
        !processAlive(record.ownerPid)
      ) {
        store.write(
          {
            ...record,
            status: "unavailable",
            error:
              record.error ??
              `Bridge owner process ${record.ownerPid} is not running`,
          },
          record.ownerPid,
        );
      }
    }
    const refreshed = threadIds.map((id) => store.read(id));
    if (refreshed.every((record) => record && TERMINAL.has(record.status))) {
      for (const record of refreshed) {
        if (!record) continue;
        console.log(
          JSON.stringify({
            threadId: record.threadId,
            status: record.status,
            error: record.error ?? null,
            lastAnswer: record.lastAnswer,
            answerTruncated: record.answerTruncated,
          }),
        );
      }
      return 0;
    }
    if (Date.now() >= deadline) {
      console.error(`timed out after ${timeoutSeconds}s; still running`);
      return 2;
    }
    await sleep(pollMs);
  }
}
