import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const git = promisify(execFile);
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { taskWorkspaceRoutes } from "../routes/task-workspaces.js";
import { errorHandler } from "../middleware/error-handler.js";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import { agents, companies, createDb, executionWorkspaces, heartbeatRuns, issues, projects, projectWorkspaces,
  EMBEDDED_POSTGRES_TEST_TIMEOUT_MS } from "@paperclipai/db";
import { realizeExecutionWorkspace } from "../services/workspace-runtime.js";
import { executionWorkspaceService } from "../services/execution-workspaces.js";
import { assertNoUnfinishedWorkspaceConsumers, RETAINED_TASK_FILES_KEY } from "../services/workspace-consumers.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

const support = await getEmbeddedPostgresTestSupport();
(support.supported ? describe : describe.skip)("parent and child workspace lifetime", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  const roots: string[] = [];
  const actor = { type: "board" as const, source: "local_implicit" as const, isInstanceAdmin: true };
  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("paperclip-workspace-consumers-");
    db = createDb(temporary.connectionString);
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);
  afterAll(async () => { for (const root of roots) await fs.rm(root, { recursive: true, force: true }); await temporary?.cleanup(); });

  async function fixture() {
    const companyId = randomUUID(), parentId = randomUUID(), childId = randomUUID(), workspaceId = randomUUID();
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "parent-workspace-")); roots.push(root);
    const cwd = path.join(root, "old-parent"), source = path.join(root, "source");
    await fs.mkdir(source);
    await git("git", ["init", "-b", "main", source]);
    await fs.writeFile(path.join(source, "README.md"), "Original project source\n");
    await git("git", ["-C", source, "add", "README.md"]);
    await git("git", ["-C", source, "-c", "user.name=Test", "-c", "user.email=test@example.test", "-c", "core.hooksPath=/dev/null", "commit", "-m", "Initial source"]);
    await git("git", ["-C", source, "worktree", "add", "-b", "parent", cwd]);
    await git("git", ["-C", source, "worktree", "remove", "--force", cwd]);
    await fs.mkdir(cwd);
    await fs.writeFile(path.join(cwd, "walk.cjs"), "// preserved QA script\n");
    await fs.writeFile(path.join(cwd, ".qa-walk-placeholder"), "evidence\n");
    await fs.writeFile(path.join(cwd, "walk2.cjs"), "// another QA script\n");
    await db.insert(companies).values({ id: companyId, name: "Workspace lifecycle", issuePrefix: `W${companyId.slice(0, 6)}` });
    const [project] = await db.insert(projects).values({ companyId, name: "Project" }).returning();
    const [projectWorkspace] = await db.insert(projectWorkspaces).values({ companyId, projectId: project.id,
      name: "Original source", sourceType: "local_path", cwd: source }).returning();
    await db.insert(issues).values([
      { id: parentId, companyId, projectId: project.id, title: "Merged parent", status: "done" },
      { id: childId, companyId, projectId: project.id, parentId, title: "Unfinished QA", status: "blocked" },
    ]);
    const [workspace] = await db.insert(executionWorkspaces).values({ id: workspaceId, companyId,
      projectId: project.id, projectWorkspaceId: projectWorkspace.id, sourceIssueId: parentId,
      mode: "isolated_workspace", strategyType: "git_worktree", providerType: "git_worktree", name: "Parent checkout",
      cwd, providerRef: cwd, metadata: { _issuePrivacySources: { [childId]: true } } }).returning();
    await db.update(issues).set({ executionWorkspaceId: workspaceId, projectWorkspaceId: projectWorkspace.id,
      executionWorkspacePreference: "reuse_existing" }).where(eq(issues.id, childId));
    return { companyId, parentId, childId, workspaceId, workspace, cwd, projectWorkspace, service: executionWorkspaceService(db) };
  }

  it.each(["todo", "in_progress", "blocked", "in_review"])("keeps a merged parent's workspace while its bound child is %s", async status => {
    const f = await fixture();
    await db.update(issues).set({ status }).where(eq(issues.id, f.childId));
    await expect(f.service.archiveWorkspaceUnderLifecycleLock({ id: f.workspaceId, patch: {}, closedAt: new Date() }))
      .rejects.toThrow("unfinished task");
    expect((await db.select().from(executionWorkspaces).where(eq(executionWorkspaces.id, f.workspaceId)))[0].status).toBe("active");
    expect(await fs.readFile(path.join(f.cwd, "walk.cjs"), "utf8")).toContain("preserved");
  });

  it("rechecks consumers after archive, before executing any destructive callback", async () => {
    const f = await fixture();
    await db.update(issues).set({ status: "done" }).where(eq(issues.id, f.childId));
    const archived = await f.service.archiveWorkspaceUnderLifecycleLock({ id: f.workspaceId, patch: {}, closedAt: new Date() });
    expect(archived?.outcome).toBe("archived");
    if (archived?.outcome !== "archived") throw new Error("Expected archive");
    await db.update(issues).set({ status: "blocked" }).where(eq(issues.id, f.childId));
    const destroy = vi.fn(async () => fs.rm(f.cwd, { recursive: true }));
    await expect(f.service.fenceClosedWorkspaceDestruction({ workspaceId: f.workspaceId,
      capturedGeneration: archived.capturedGeneration, destroy })).rejects.toThrow("unfinished task");
    expect(destroy).not.toHaveBeenCalled();
    expect(await fs.readdir(f.cwd)).toHaveLength(3);
  });

  it("also protects an unfinished consumer that is no longer a descendant", async () => {
    const f = await fixture();
    await db.update(issues).set({ parentId: null }).where(eq(issues.id, f.childId));
    await expect(assertNoUnfinishedWorkspaceConsumers(db, f.workspace)).rejects.toThrow("unfinished task");
    await db.update(issues).set({ status: "cancelled" }).where(eq(issues.id, f.childId));
    await expect(assertNoUnfinishedWorkspaceConsumers(db, f.workspace)).resolves.toBeUndefined();
  });

  it("protects a pending selection before the task is bound", async () => {
    const f = await fixture();
    await db.update(issues).set({ executionWorkspaceId: null, workspacePendingSelection: { version: 1,
      requestKey: "next", expectedBindingRevision: 0,
      intent: { version: 1, source: "explicit", selection: { kind: "existing", workspaceId: f.workspaceId } },
    } }).where(eq(issues.id, f.childId));
    await expect(assertNoUnfinishedWorkspaceConsumers(db, f.workspace)).rejects.toThrow("unfinished task");
  });

  it.each(["git_worktree", "local_fs"])("recovers a lost parent checkout recorded as %s and preserves QA files", async providerType => {
    const f = await fixture();
    await db.update(executionWorkspaces).set({ providerType }).where(eq(executionWorkspaces.id, f.workspaceId));
    const input = { companyId: f.companyId, issueId: f.childId, actor, expectedBindingRevision: 0, requestKey: "fresh-qa" };
    expect(await f.service.recoverTaskWorkspace(input)).toMatchObject({ kind: "scheduled", retainedWorkspaceId: f.workspaceId, retainedPath: f.cwd });
    expect(await f.service.recoverTaskWorkspace(input)).toMatchObject({ kind: "scheduled" });
    const view = await f.service.inspectTaskWorkspace(f.companyId, f.childId, actor);
    expect(view.retainedFiles).toEqual([expect.objectContaining({ workspaceId: f.workspaceId, available: true,
      entries: expect.arrayContaining(["walk.cjs", "walk2.cjs", ".qa-walk-placeholder"]) })]);
    expect(await f.service.applyPendingTaskWorkspaceSelection({ companyId: f.companyId, issueId: f.childId,
      runId: randomUUID(), actor })).toBe(true);
    expect(await f.service.recoverTaskWorkspace(input)).toMatchObject({ kind: "applied", retainedWorkspaceId: f.workspaceId });
    const [child] = await db.select().from(issues).where(eq(issues.id, f.childId));
    expect(child).toMatchObject({ executionWorkspaceId: null, executionWorkspacePreference: null,
      projectWorkspaceId: f.projectWorkspace.id, executionWorkspaceSettings: { mode: "isolated_workspace" },
      workspaceSelection: { selection: { kind: "configured_source", mode: "managed_isolated" } } });
    const fresh = await realizeExecutionWorkspace({
      base: { baseCwd: f.projectWorkspace.cwd!, source: "project_primary", projectId: child.projectId,
        workspaceId: f.projectWorkspace.id, repoUrl: null, repoRef: "main" },
      issue: { id: child.id, identifier: child.identifier, title: child.title },
      agent: { companyId: f.companyId, id: null, name: "QA" },
      config: { workspaceStrategy: { type: "git_worktree", baseRef: "main", branchTemplate: `child-${child.id}`,
        worktreeParentDir: path.join(path.dirname(f.cwd), "fresh-worktrees") } }, skipHostProvision: true,
    });
    expect(fresh.cwd).not.toBe(f.cwd);
    expect((await git("git", ["-C", fresh.cwd, "rev-parse", "--is-inside-work-tree"])).stdout.trim()).toBe("true");
    const [replacement] = await db.insert(executionWorkspaces).values({ companyId: f.companyId,
      projectId: child.projectId, projectWorkspaceId: f.projectWorkspace.id, sourceIssueId: child.id,
      mode: "isolated_workspace", strategyType: "git_worktree", providerType: "git_worktree", name: "Child checkout",
      cwd: fresh.cwd, providerRef: fresh.cwd, branchName: fresh.branchName }).returning();
    await f.service.bindTaskWorkspace(f.companyId, child.id, replacement.id);
    const recovered = await f.service.inspectTaskWorkspace(f.companyId, child.id, actor);
    expect(recovered.workspace?.id).toBe(replacement.id);
    expect(recovered.retainedFiles[0]).toMatchObject({ workspaceId: f.workspaceId, available: true });
    const [old] = await db.select().from(executionWorkspaces).where(eq(executionWorkspaces.id, f.workspaceId));
    expect(old.metadata?.[RETAINED_TASK_FILES_KEY]).toHaveProperty(f.childId);
    await expect(assertNoUnfinishedWorkspaceConsumers(db, old)).rejects.toThrow("retains files");
    expect(await fs.readFile(path.join(f.cwd, "walk2.cjs"), "utf8")).toContain("another QA");
    await expect(assertNoUnfinishedWorkspaceConsumers(db, old, { destructive: false })).resolves.toBeUndefined();
    await expect(f.service.releaseRetainedTaskFiles({ companyId: f.companyId, issueId: child.id,
      workspaceId: f.workspaceId, requestKey: "stale-recovery", actor })).rejects.toThrow("Retained files changed");
    expect((await f.service.inspectTaskWorkspace(f.companyId, child.id, actor)).retainedFiles).toHaveLength(1);
    expect(await f.service.releaseRetainedTaskFiles({ companyId: f.companyId, issueId: child.id,
      workspaceId: f.workspaceId, requestKey: input.requestKey, actor })).toEqual({ released: true });
    expect((await f.service.inspectTaskWorkspace(f.companyId, child.id, actor)).retainedFiles).toEqual([]);
    expect(await fs.readdir(f.cwd)).toHaveLength(3);
  });

  it("rejects stale recovery and preserves both the binding and all files", async () => {
    const f = await fixture();
    await expect(f.service.recoverTaskWorkspace({ companyId: f.companyId, issueId: f.childId, actor,
      expectedBindingRevision: 99, requestKey: "stale" })).rejects.toThrow("binding changed");
    expect((await f.service.inspectTaskWorkspace(f.companyId, f.childId, actor)).workspace?.id).toBe(f.workspaceId);
    expect(await fs.readdir(f.cwd)).toHaveLength(3);
  });
  it("does not bind a new child after the parent workspace has closed", async () => {
    const f = await fixture();
    await db.update(issues).set({ status: "done" }).where(eq(issues.id, f.childId));
    await f.service.archiveWorkspaceUnderLifecycleLock({ id: f.workspaceId, patch: {}, closedAt: new Date() });
    const [next] = await db.insert(issues).values({ companyId: f.companyId, parentId: f.parentId, title: "Later QA" }).returning();
    await expect(f.service.bindTaskWorkspace(f.companyId, next.id, f.workspaceId)).rejects.toThrow("closed");
    expect((await db.select().from(issues).where(eq(issues.id, next.id)))[0].executionWorkspaceId).toBeNull();
  });

  it("exposes recovery through the authorized task API without starting a run", async () => {
    const f = await fixture();
    const app = express(); app.use(express.json());
    app.use((req, _res, next) => { req.actor = actor; next(); });
    app.use("/api", taskWorkspaceRoutes(db)); app.use(errorHandler);
    await request(app).post(`/api/issues/${f.childId}/workspace/recover`)
      .send({ expectedBindingRevision: 0, requestKey: "api", cwd: "/untrusted" }).expect(400);
    const response = await request(app).post(`/api/issues/${f.childId}/workspace/recover`)
      .send({ expectedBindingRevision: 0, requestKey: "api" }).expect(200);
    expect(response.body).toMatchObject({ kind: "scheduled", retainedWorkspaceId: f.workspaceId });
    const view = await request(app).get(`/api/issues/${f.childId}/workspace`).expect(200);
    expect(view.body.retainedFiles[0]).toMatchObject({ workspaceId: f.workspaceId, cwd: f.cwd,
      entries: expect.arrayContaining(["walk.cjs"]) });
    expect(await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.companyId, f.companyId))).toEqual([]);
  });

  it("queues recovery while a child agent is running without changing its files or binding", async () => {
    const f = await fixture();
    const [agent] = await db.insert(agents).values({ companyId: f.companyId, name: "QA", status: "running" }).returning();
    const [run] = await db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: agent.id,
      status: "running", invocationSource: "on_demand", contextSnapshot: { issueId: f.childId } }).returning();
    await expect(f.service.recoverTaskWorkspace({ companyId: f.companyId, issueId: f.childId, actor,
      expectedBindingRevision: 0, requestKey: "busy" })).resolves.toMatchObject({ kind: "scheduled" });
    await expect(f.service.applyPendingTaskWorkspaceSelection({ companyId: f.companyId, issueId: f.childId,
      runId: randomUUID(), actor })).rejects.toThrow("Previous task run must finish persistence");
    expect((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id)))[0].status).toBe("running");
    expect((await db.select().from(issues).where(eq(issues.id, f.childId)))[0].executionWorkspaceId).toBe(f.workspaceId);
  });

  it("does not use a caller-edited retained path as filesystem read authority", async () => {
    const f = await fixture();
    const outside = path.join(path.dirname(f.cwd), "other-files");
    await fs.mkdir(outside); await fs.writeFile(path.join(outside, "private-name.txt"), "private");
    await db.update(executionWorkspaces).set({ metadata: { _issuePrivacySources: { [f.childId]: true },
      [RETAINED_TASK_FILES_KEY]: { [f.childId]: { cwd: outside, requestKey: "forged" } },
    } }).where(eq(executionWorkspaces.id, f.workspaceId));
    const view = await f.service.inspectTaskWorkspace(f.companyId, f.childId, actor);
    expect(view.retainedFiles).toEqual([expect.objectContaining({ available: false, entries: [] })]);
    expect(JSON.stringify(view)).not.toContain("private-name.txt");
  });

});
