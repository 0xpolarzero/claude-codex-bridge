import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { homedir, hostname } from "node:os";
import { join } from "node:path";

export type ThreadRecord = {
  threadId: string;
  cwd: string;
  model: string;
  serviceTier?: string | null;
  requestedServiceTier?: string | null;
  effort?: string | null;
  requestedEffort?: string | null;
  status: string;
  error?: string | null;
  errorTruncated?: boolean;
  lastTurnId: string | null;
  lastAnswer: string;
  answerTruncated: boolean;
  ownerPid: number;
  ownerHost?: string;
  ownerStartId?: string;
  updatedAt: string;
};

export type OwnerStatus = "alive" | "gone" | "unknown";

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

function processStartId(pid: number) {
  try {
    const value = execFileSync("ps", ["-o", "lstart=", "-p", String(pid)], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    return value || undefined;
  } catch {
    return undefined;
  }
}

/** Only claim ownership when both host and process incarnation match. */
export function ownerStatus(record: ThreadRecord): OwnerStatus {
  if (
    record.ownerHost !== hostname() ||
    !record.ownerStartId ||
    !Number.isInteger(record.ownerPid)
  )
    return "unknown";
  const currentStartId = processStartId(record.ownerPid);
  if (currentStartId !== undefined)
    return currentStartId === record.ownerStartId ? "alive" : "gone";
  return processAlive(record.ownerPid) ? "unknown" : "gone";
}

/** Thread records on disk, so state survives a restarted Claude session. */
export class ThreadStore {
  readonly dir: string;
  private readonly ownerHost = hostname();
  private readonly ownerStartId = processStartId(process.pid);

  constructor(readonly home = bridgeHome()) {
    this.dir = join(home, "threads");
    mkdirSync(this.dir, { recursive: true });
  }

  path(threadId: string) {
    return join(this.dir, `${threadId.replace(/[^A-Za-z0-9_-]/g, "_")}.json`);
  }

  write(
    record: Omit<
      ThreadRecord,
      "ownerPid" | "ownerHost" | "ownerStartId" | "updatedAt"
    >,
  ) {
    const full: ThreadRecord = {
      ...record,
      ownerPid: process.pid,
      ownerHost: this.ownerHost,
      ownerStartId: this.ownerStartId,
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
