import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

export const versionPaths = [
  "package.json",
  "plugins/claude-codex-bridge/.claude-plugin/plugin.json",
  ".claude-plugin/marketplace.json",
  "package-lock.json",
];
const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const levels = ["patch", "minor", "major"];

export function releaseLevel(labels: unknown): string | undefined {
  if (
    !Array.isArray(labels) ||
    labels.some((label) => typeof label !== "string")
  )
    throw new Error("Release labels must be a JSON array of strings");
  const requested = labels.filter((label) => label.startsWith("release:"));
  if (requested.length === 0) return undefined;
  if (requested.length !== 1 || !levels.includes(requested[0].slice(8)))
    throw new Error(
      "Choose exactly one release:patch, release:minor, or release:major label",
    );
  return requested[0].slice(8);
}

export function nextVersion(current: string, level: string): string {
  if (!semver.test(current))
    throw new Error(`Invalid project version: ${current}`);
  const parts = current.split(".").map(Number);
  const index = { major: 0, minor: 1, patch: 2 }[level];
  if (index === undefined) throw new Error(`Invalid release level: ${level}`);
  parts[index]++;
  for (let i = index + 1; i < parts.length; i++) parts[i] = 0;
  if (parts.some((part) => !Number.isSafeInteger(part)))
    throw new Error("Version exceeds the safe integer range");
  return parts.join(".");
}

export function readVersions(root: string, checkoutRuntime = false) {
  const contents = versionPaths.map((path) =>
    readFileSync(resolve(root, path), "utf8"),
  );
  const [pkg, plugin, marketplace, lock] = contents.map((content) =>
    JSON.parse(content),
  );
  if (typeof pkg.version !== "string" || !semver.test(pkg.version))
    throw new Error("package.json must declare a stable X.Y.Z version");
  const entries = marketplace.plugins.filter(
    (entry: { name: string }) => entry.name === plugin.name,
  );
  if (entries.length !== 1)
    throw new Error("Expected exactly one marketplace entry for the plugin");
  const server = plugin.mcpServers["codex-bridge"];
  const expectedCommand = checkoutRuntime ? "node" : "npx";
  const expectedArgs = checkoutRuntime
    ? [resolve(root, "dist/server.js")]
    : ["--yes", `${pkg.name}@${pkg.version}`];
  if (
    lock.version !== pkg.version ||
    lock.packages[""].version !== pkg.version ||
    lock.name !== pkg.name ||
    lock.packages[""].name !== pkg.name ||
    plugin.version !== pkg.version ||
    entries[0].version !== pkg.version ||
    server.command !== expectedCommand ||
    JSON.stringify(server.args) !== JSON.stringify(expectedArgs)
  )
    throw new Error(
      "Project versions or the plugin's npm pin disagree; refusing a partial release",
    );
  return { contents, version: pkg.version, packageName: pkg.name };
}

export function bumpVersion(root: string, level: string): string {
  const { contents, version, packageName } = readVersions(root);
  const next = nextVersion(version, level);
  // Validate every replacement before writing any file, and preserve formatting.
  const replacements = contents.map((content, index) => {
    if (index === 3) {
      const lock = JSON.parse(content);
      lock.version = next;
      lock.packages[""].version = next;
      return `${JSON.stringify(lock, null, 2)}\n`;
    }
    const pattern = /("version"\s*:\s*")([^"\n]+)(")/g;
    const matches = [...content.matchAll(pattern)];
    if (matches.length !== 1 || matches[0][2] !== version)
      throw new Error(
        `${versionPaths[index]} must contain exactly one project version`,
      );
    let updated = content.replace(pattern, `$1${next}$3`);
    if (index === 1) {
      const pin = `${packageName}@${version}`;
      if (updated.split(pin).length !== 2)
        throw new Error("Expected exactly one npm version pin");
      updated = updated.replace(pin, `${packageName}@${next}`);
    }
    return updated;
  });
  for (let i = 0; i < versionPaths.length; i++)
    writeFileSync(resolve(root, versionPaths[i]), replacements[i]);
  return next;
}

export function run(args: string[], root: string): string {
  const [command, argument] = args;
  if (command === "level")
    return releaseLevel(JSON.parse(argument ?? "[]")) ?? "";
  if (command === "bump") return bumpVersion(root, argument);
  if (command === "verify") {
    const checkoutRuntime = args.includes("--checkout-runtime");
    const versions = args.filter((value) => value !== "--checkout-runtime");
    if (versions.length > 2)
      throw new Error("Usage: release.ts verify [X.Y.Z] [--checkout-runtime]");
    const expectedVersion = versions[1];
    const { version } = readVersions(root, checkoutRuntime);
    if (expectedVersion !== undefined && expectedVersion !== version)
      throw new Error(
        `Tag ${expectedVersion} does not match project version ${version}`,
      );
    return version;
  }
  throw new Error(
    "Usage: release.ts level '<labels JSON>' | bump patch|minor|major | verify [X.Y.Z] [--checkout-runtime]",
  );
}

if (import.meta.main) {
  const output = run(process.argv.slice(2), resolve(import.meta.dirname, ".."));
  if (output) console.log(output);
}
