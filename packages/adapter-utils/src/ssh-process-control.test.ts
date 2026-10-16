import { spawn, execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import { prepareSshProcessControl } from "./ssh-process-control.js";
import { runSshCommand } from "./ssh.js";
import { cleanupRestoredRunWorkspace } from "./workspace-restore-result.js";

vi.mock("./ssh.js", async (importOriginal) => ({
  ...await importOriginal<typeof import("./ssh.js")>(),
  runSshCommand: vi.fn(async (_spec, command, options) =>
    promisify(execFile)("/bin/sh", ["-c", command], { timeout: options.timeoutMs, maxBuffer: options.maxBuffer })),
}));
const runId = "11111111-1111-4111-8111-111111111111";
const roots: string[] = [];
const children: ReturnType<typeof spawn>[] = [];
const spec = (root: string) => ({
  host: "test", port: 22, username: "test", privateKey: null, knownHosts: null,
  strictHostKeyChecking: true, remoteWorkspacePath: root,
  remoteCwd: path.join(root, ".paperclip-runtime", "runs", runId, "workspace"),
});
async function fixture(source: string) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "paperclip-stop-proof-"));
  roots.push(root);
  const target = spec(root);
  await fs.mkdir(target.remoteCwd, { recursive: true });
  const control = prepareSshProcessControl({ runId, spec: target, command: process.execPath, args: ["-e", source], graceSec: 0.1 })!;
  const child = spawn(process.execPath, control.args, { cwd: target.remoteCwd, stdio: ["pipe", "pipe", "pipe"] });
  children.push(child);
  const closed = new Promise<{ code: number | null; signal: string | null }>(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  let stdout = "";
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += chunk; });
  child.stdout!.on("data", chunk => { stdout += chunk; });
  return { root, target, control, child, closed, stdout: () => stdout, stderr: () => stderr };
}
async function waitFor(check: () => Promise<boolean> | boolean) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error("Fixture did not become ready");
}
afterEach(async () => {
  vi.mocked(runSshCommand).mockClear();
  for (const child of children.splice(0)) child.kill("SIGKILL");
  await Promise.all(roots.splice(0).map(root => fs.rm(root, { recursive: true, force: true })));
});

describe("SSH owned process group stop proof", () => {
  it("preserves arguments, stdin, stdout, and nonzero provider exit while proving natural completion", async () => {
    const f = await fixture('process.stdin.on("data", data => process.stdout.write(data)); process.stdin.on("end", () => process.exit(7));');
    f.child.stdin!.end("input ' with spaces\n");
    expect(await f.closed).toEqual({ code: 7, signal: null });
    expect(f.stdout()).toBe("input ' with spaces\n");
    expect(await f.control.confirmStopped()).toBe(true);
  });

  it("stops an interrupt-aware provider and its interrupt-ignoring child after transport loss", async () => {
    const f = await fixture(`
      const { spawn } = require("node:child_process");
      const fs = require("node:fs");
      const child = spawn(process.execPath, ["-e", 'process.on("SIGINT", () => {}); process.on("SIGTERM", () => {}); setInterval(() => require("node:fs").appendFileSync("writes", "x"), 10);'], { stdio: "ignore" });
      process.on("SIGINT", () => { fs.writeFileSync("interrupted", "yes"); process.exit(130); });
      fs.writeFileSync("provider-ready", String(child.pid));
      setInterval(() => {}, 1000);
    `);
    await waitFor(() => fs.stat(path.join(f.target.remoteCwd, "writes")).then(() => true, () => false));
    // An SSH disconnect delivers HUP to the remote command. The supervisor
    // must stop its owned group and leave proof before exiting the transport.
    f.child.kill("SIGHUP");
    const outcome = await f.closed;
    expect(await f.control.confirmStopped(), JSON.stringify({ outcome, stderr: f.stderr() })).toBe(true);
    expect(await fs.readFile(path.join(f.target.remoteCwd, "interrupted"), "utf8")).toBe("yes");
    const before = await fs.readFile(path.join(f.target.remoteCwd, "writes"), "utf8");
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(await fs.readFile(path.join(f.target.remoteCwd, "writes"), "utf8")).toBe(before);
    const backup = path.join(f.root, "copied-back.txt");
    await fs.copyFile(path.join(f.target.remoteCwd, "writes"), backup);
    await cleanupRestoredRunWorkspace({
      cleanup: () => fs.rm(path.dirname(f.target.remoteCwd), { recursive: true, force: true }),
      result: { exitCode: 130, signal: "SIGINT", timedOut: false, errorCode: "cancelled" },
      providerStopped: await f.control.confirmStopped(), collectionSucceeded: true, workspaceRestored: true,
      onFailure: () => { throw new Error("delete failed"); },
    });
    expect(await fs.readFile(backup, "utf8")).toBe(before);
    await expect(fs.stat(f.target.remoteCwd)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("stops lingering child commands before confirming a normal provider exit", async () => {
    const f = await fixture(`
      const { spawn } = require("node:child_process");
      spawn(process.execPath, ["-e", 'process.on("SIGINT", () => {}); setInterval(() => require("node:fs").appendFileSync("writes", "x"), 5);'], { stdio: "ignore" });
      setTimeout(() => process.exit(0), 100);
    `);
    const outcome = await f.closed;
    expect(outcome.code, JSON.stringify({ outcome, stderr: f.stderr() })).toBe(0);
    expect(await f.control.confirmStopped()).toBe(true);
    const file = path.join(f.target.remoteCwd, "writes");
    const before = await fs.readFile(file, "utf8");
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(await fs.readFile(file, "utf8")).toBe(before);
  });

  it("can stop through the independent channel while the original SSH process is still attached", async () => {
    const f = await fixture('process.stdout.write("ready"); setInterval(() => {}, 1000);');
    await waitFor(() => f.stdout() === "ready");
    expect(await f.control.confirmStopped()).toBe(true);
    await f.closed;
  });

  it("retains when transport fails or receipt contents are forged", async () => {
    const f = await fixture('process.exit(0)');
    expect(await f.closed, f.stderr()).toEqual({ code: 0, signal: null });
    const directory = path.join(f.root, ".paperclip-runtime", "runs", runId, "ssh-processes");
    const receiptName = (await fs.readdir(directory)).find(name => name.endsWith(".receipt"))!;
    const receiptPath = path.join(directory, receiptName);
    const receipt = JSON.parse(await fs.readFile(receiptPath, "utf8"));
    // A correct body must not authorize deletion with an invalid signature.
    await fs.writeFile(receiptPath, JSON.stringify({ ...receipt, signature: "0".repeat(64) }));
    expect(await f.control.confirmStopped()).toBe(false);
    // Neither may a live control endpoint claim stop without valid proof.
    vi.mocked(runSshCommand).mockResolvedValueOnce({ stdout: JSON.stringify({ stopped: true, signature: "0".repeat(64) }), stderr: "" });
    expect(await f.control.confirmStopped()).toBe(false);
    vi.mocked(runSshCommand).mockRejectedValueOnce(new Error("offline"));
    expect(await f.control.confirmStopped()).toBe(false);
  });

  it("retains source files when copy-back fails after confirmed stop", async () => {
    const f = await fixture('require("node:fs").writeFileSync("unsaved", "keep this work"); process.exit(1);');
    await f.closed;
    expect(await f.control.confirmStopped()).toBe(true);
    await cleanupRestoredRunWorkspace({
      cleanup: () => fs.rm(path.dirname(f.target.remoteCwd), { recursive: true, force: true }),
      result: { exitCode: 1, signal: null, timedOut: false },
      providerStopped: true, collectionSucceeded: true, workspaceRestored: false, onFailure: () => {},
    });
    expect(await fs.readFile(path.join(f.target.remoteCwd, "unsaved"), "utf8")).toBe("keep this work");
  });

  it("retains when the group leader unexpectedly dies", async () => {
    const f = await fixture('process.kill(process.ppid, "SIGKILL"); process.exit(0)');
    expect((await f.closed).code).toBe(255);
    expect(await f.control.confirmStopped()).toBe(false);
  });

  it("does not supervise arbitrary paths or invalid run IDs", () => {
    const input = { runId, spec: spec("/srv/test"), command: "pi", args: [], graceSec: 1 };
    expect(prepareSshProcessControl({ ...input, runId: "../escape" })).toBeNull();
    expect(prepareSshProcessControl({ ...input, spec: { ...input.spec, remoteCwd: "/srv/test" } })).toBeNull();
  });
});
