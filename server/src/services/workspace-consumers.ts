import { and, eq, notInArray, or, sql } from "drizzle-orm";
import { issues, type Db } from "@paperclipai/db";
import { conflict } from "../errors.js";

type Transaction = Parameters<Parameters<Db["transaction"]>[0]>[0];
export const RETAINED_TASK_FILES_KEY = "_retainedTaskFiles";

// Binding changes and destruction must take the same database-owned lock.
export async function lockWorkspaceLifecycle(tx: Transaction, workspaceId: string) {
  await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`execution_workspace_lifecycle:${workspaceId}`}, 0))`);
}

export async function assertNoUnfinishedWorkspaceConsumers(reader: Db | Transaction, workspace: {
  id: string; companyId: string; mode: string; metadata?: Record<string, unknown> | null;
}, options: { destructive?: boolean } = {}) {
  if (workspace.mode !== "isolated_workspace") return;
  if (options.destructive !== false && Object.keys((workspace.metadata?.[RETAINED_TASK_FILES_KEY] ?? {}) as object).length) {
    throw conflict("This workspace retains files from a task moved to a fresh workspace. Preserve or resolve those files before cleanup.");
  }
  const [consumer] = await reader.select({ id: issues.id }).from(issues).where(and(
    eq(issues.companyId, workspace.companyId),
    notInArray(issues.status, ["done", "cancelled"]),
    or(eq(issues.executionWorkspaceId, workspace.id),
      sql`${issues.workspacePendingSelection}->'intent'->'selection'->>'kind' = 'existing'
        AND ${issues.workspacePendingSelection}->'intent'->'selection'->>'workspaceId' = ${workspace.id}`),
  )).limit(1);
  if (consumer) throw conflict("This workspace is still needed by an unfinished task. Finish or move the task before cleanup.");
}
