import {
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export type ThreadRecord = {
  threadId: string;
  cwd: string;
  model: string;
  serviceTier?: string | null;
  effort?: string | null;
  status: string;
  error?: string | null;
  lastTurnId: string | null;
  lastAnswer: string;
  answerTruncated: boolean;
  ownerPid: number;
  updatedAt: string;
};

export const TERMINAL = new Set([
  "completed",
  "interrupted",
  "failed",
  "unavailable",
  "idle",
]);

export function bridgeHome() {
  return (
    process.env.CODEX_BRIDGE_HOME ??
    process.env.CLAUDE_PLUGIN_DATA ??
    join(homedir(), ".claude", "codex-bridge")
  );
}

/** Thread records on disk, so state survives a restarted Claude session. */
export class ThreadStore {
  readonly dir: string;

  constructor(readonly home = bridgeHome()) {
    this.dir = join(home, "threads");
    mkdirSync(this.dir, { recursive: true });
  }

  path(threadId: string) {
    return join(this.dir, `${threadId.replace(/[^A-Za-z0-9_-]/g, "_")}.json`);
  }

  write(
    record: Omit<ThreadRecord, "ownerPid" | "updatedAt">,
    ownerPid = process.pid,
  ) {
    const full: ThreadRecord = {
      ...record,
      ownerPid,
      updatedAt: new Date().toISOString(),
    };
    const target = this.path(record.threadId);
    const temporary = `${target}.${process.pid}.tmp`;
    writeFileSync(temporary, JSON.stringify(full));
    renameSync(temporary, target);
  }

  read(threadId: string): ThreadRecord | undefined {
    try {
      return JSON.parse(readFileSync(this.path(threadId), "utf8"));
    } catch {
      return undefined;
    }
  }

  list(): ThreadRecord[] {
    return readdirSync(this.dir)
      .filter((name) => name.endsWith(".json"))
      .flatMap((name) => {
        try {
          return [JSON.parse(readFileSync(join(this.dir, name), "utf8"))];
        } catch {
          return [];
        }
      });
  }
}

export function processAlive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}
