import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import path from "node:path";
import { runSshCommand, shellQuote, type SshRemoteExecutionSpec } from "./ssh.js";

// These sources use only Node built-ins. SSH runners already require Node for
// the callback bridge. Control state lives beside, never inside, copied files.
export const SSH_GROUP_SENTINEL_SOURCE = String.raw`
const { spawn } = require("node:child_process");
const input = JSON.parse(process.argv[1]);
process.on("SIGINT", () => {});
process.on("SIGTERM", () => {});
const hold = setInterval(() => {}, 1000);
const child = spawn(input.command, input.args, { stdio: "inherit", env: process.env });
child.once("error", () => process.send?.({ code: 127, signal: null }));
child.once("exit", (code, signal) => process.send?.({
  code: code ?? (128 + (require("node:os").constants.signals[signal] || 0)), signal,
}));
`;

export const SSH_PROCESS_SUPERVISOR_SOURCE = String.raw`
const { spawn } = require("node:child_process");
const fs = require("node:fs/promises");
const net = require("node:net");
const crypto = require("node:crypto");
const input = JSON.parse(process.argv[1]);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const sign = text => crypto.createHmac("sha256", input.secret).update(text).digest("hex");
const body = JSON.stringify({ runId: input.runId, invocationId: input.invocationId, stopped: true });
let anchor;
let live = false;
let stopPromise;
let providerResult;
let shutdownRequested = false;
let finishing = false;
const unknown = reason => { console.error("[paperclip] SSH process stop unconfirmed: " + reason); return false; };
const stop = () => stopPromise ??= (async () => {
  // A retained live leader anchors the group identity. Never signal a group
  // after that leader exits. Following the final KILL, only inspect absence.
  if (!anchor || !live) return unknown("group leader unavailable");
  try { process.kill(-anchor.pid, "SIGINT"); } catch { return unknown("interrupt failed"); }
  await sleep(input.graceMs);
  if (!live) return unknown("group leader exited before escalation");
  try { process.kill(-anchor.pid, "SIGKILL"); } catch { return unknown("escalation failed"); }
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    try { process.kill(-anchor.pid, 0); }
    catch (error) {
      if (error.code === "ESRCH") {
        try {
          await fs.writeFile(input.controlPath + ".receipt", JSON.stringify({ body, signature: sign(body) }), { mode: 0o600 });
        } catch { return unknown("stop receipt write failed"); }
        return true;
      }
      // Darwin can report EPERM while a killed group is being reaped. It is
      // never proof: keep inspecting, and retain unless absence is observed.
      if (error.code !== "EPERM") return unknown("group inspection failed");
    }
    await sleep(25);
  }
  return unknown("group still present");
})();
const finish = async () => {
  const stopped = await stop();
  // Leave the signed receipt for the host. Missing proof always means retain.
  if (finishing) return;
  finishing = true;
  const code = providerResult?.code;
  const exitCode = stopped && Number.isInteger(code) ? code : 255;
  server.close(() => process.exit(exitCode));
  setTimeout(() => process.exit(exitCode), 1000).unref();
};
const server = net.createServer(socket => {
  socket.setTimeout(6000, () => socket.destroy());
  let buffer = "";
  socket.on("data", data => {
    buffer += data;
    if (buffer.length > 1024) return socket.destroy();
    if (!buffer.includes("\n")) return;
    socket.removeAllListeners("data");
    let request;
    try { request = JSON.parse(buffer); } catch { return socket.destroy(); }
    if (typeof request.challenge !== "string" || request.challenge.length !== 64 ||
        typeof request.signature !== "string" || request.signature !== sign("stop:" + request.challenge)) return socket.destroy();
    void stop().then(stopped => {
      socket.end(JSON.stringify({ stopped, signature: sign(request.challenge + ":" + stopped) }) + "\n", () => void finish());
    }).catch(() => socket.destroy());
  });
  socket.on("error", () => {});
});
for (const signal of ["SIGHUP", "SIGINT", "SIGTERM"]) process.on(signal, () => {
  shutdownRequested = true;
  if (anchor) void finish();
});
(async () => {
  await fs.mkdir(require("node:path").dirname(input.controlPath), { recursive: true, mode: 0o700 });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  anchor = spawn(process.execPath, ["-e", input.sentinelSource, JSON.stringify({ command: input.command, args: input.args })], {
    detached: true, stdio: ["inherit", "inherit", "inherit", "ipc"], env: process.env,
  });
  live = !!anchor.pid;
  anchor.once("exit", () => { live = false; if (!stopPromise) void finish(); });
  anchor.once("error", () => { live = false; void finish(); });
  anchor.on("message", result => { providerResult = result; void finish(); });
  await fs.writeFile(input.controlPath, JSON.stringify({ port: server.address().port }), { mode: 0o600 });
  if (shutdownRequested) void finish();
})().catch(() => { if (anchor) void finish(); else process.exit(255); });
`;

export const SSH_PROCESS_STOP_CLIENT_SOURCE = String.raw`
const fs = require("node:fs/promises");
const net = require("node:net");
const input = JSON.parse(process.argv[1]);
(async () => {
  try { process.stdout.write(await fs.readFile(input.controlPath + ".receipt", "utf8")); return; } catch {}
  const { port } = JSON.parse(await fs.readFile(input.controlPath, "utf8"));
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("Invalid control endpoint");
  await new Promise((resolve, reject) => {
    const socket = net.connect(port, "127.0.0.1");
    let data = "";
    socket.setTimeout(6000, () => socket.destroy(new Error("Stop confirmation timed out")));
    socket.on("connect", () => socket.write(JSON.stringify(input.request) + "\n"));
    socket.on("error", reject);
    socket.on("data", chunk => {
      data += chunk;
      if (data.length > 2048) return socket.destroy(new Error("Invalid stop response"));
      if (data.includes("\n")) { process.stdout.write(data); socket.end(); resolve(); }
    });
    socket.on("end", resolve);
  });
})().catch(() => process.exit(1));
`;

function matchesSignature(actual: unknown, expected: string): boolean {
  return typeof actual === "string" && /^[a-f0-9]{64}$/.test(actual) && actual.length === expected.length &&
    timingSafeEqual(Buffer.from(actual), Buffer.from(expected));
}

/** Scope supervision to Paperclip's disposable UUID run workspace. */
export function prepareSshProcessControl(input: {
  runId: string;
  spec: SshRemoteExecutionSpec;
  command: string;
  args: string[];
  graceSec: number;
}) {
  if (!/^[a-f\d]{8}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{12}$/i.test(input.runId)) return null;
  const runRoot = path.posix.join(input.spec.remoteWorkspacePath, ".paperclip-runtime", "runs", input.runId);
  if (input.spec.remoteCwd !== path.posix.join(runRoot, "workspace")) return null;
  const invocationId = randomUUID();
  const secret = randomBytes(32).toString("hex");
  const controlPath = path.posix.join(runRoot, "ssh-processes", invocationId);
  const sign = (text: string) => createHmac("sha256", secret).update(text).digest("hex");
  const body = JSON.stringify({ runId: input.runId, invocationId, stopped: true });
  const payload = {
    runId: input.runId, invocationId, secret, controlPath,
    command: input.command, args: input.args,
    graceMs: Math.min(3000, Math.max(100, input.graceSec * 1000 || 1000)),
    sentinelSource: SSH_GROUP_SENTINEL_SOURCE,
  };
  return {
    command: "node",
    args: ["-e", SSH_PROCESS_SUPERVISOR_SOURCE, JSON.stringify(payload)],
    async confirmStopped(): Promise<boolean> {
      const challenge = randomBytes(32).toString("hex");
      const request = { challenge, signature: sign("stop:" + challenge) };
      try {
        const command = ["node", "-e", SSH_PROCESS_STOP_CLIENT_SOURCE, JSON.stringify({ controlPath, request })]
          .map(shellQuote).join(" ");
        const response = await runSshCommand(input.spec, command, { timeoutMs: 8000, maxBuffer: 4096 });
        const receipt = JSON.parse(response.stdout);
        return (receipt.body === body && matchesSignature(receipt.signature, sign(body))) ||
          (receipt.stopped === true && matchesSignature(receipt.signature, sign(challenge + ":true")));
      } catch {
        // Transport loss, stale endpoints, or invalid proof leave recovery intact.
        return false;
      }
    },
  };
}
