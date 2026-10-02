import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

process.env.CODEX_BRIDGE_HOME ??= mkdtempSync(
  join(tmpdir(), "codex-bridge-test-"),
);
