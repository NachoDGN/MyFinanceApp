import { randomUUID } from "node:crypto";

import type {
  ApplyRuleDraftInput,
  CreateRuleInput,
  QueueRuleDraftInput,
} from "@myfinance/domain";

import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import { parseJsonColumn, serializeJson } from "./sql-json";
import { withSeededUserContext } from "./sql-runtime";

type CreateRule = (
  input: CreateRuleInput,
) => Promise<{ applied: boolean; ruleId: string }>;

export function queueRuleDraftForUser(
  _userId: string,
  input: QueueRuleDraftInput,
) {
  return withSeededUserContext(async (sql) => {
    const jobId = randomUUID();
    if (input.apply) {
      await sql`
        insert into public.jobs (
          id,
          job_type,
          payload_json,
          status,
          attempts,
          available_at
        ) values (
          ${jobId},
          ${"rule_parse"},
          ${serializeJson(sql, { requestText: input.requestText })}::jsonb,
          ${"queued"},
          0,
          ${new Date().toISOString()}
        )
      `;
      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "rules.queue-draft",
        "job",
        jobId,
        null,
        { requestText: input.requestText },
      );
      await insertAuditEventRecord(sql, auditEvent);
    }
    return { applied: input.apply, jobId };
  });
}

export function applyRuleDraftForUser(
  _userId: string,
  input: ApplyRuleDraftInput,
  createRule: CreateRule,
) {
  return withSeededUserContext(async (sql) => {
    const rows = await sql`
      select * from public.jobs
      where id = ${input.jobId}
        and job_type = 'rule_parse'
      limit 1
    `;
    const job = rows[0];
    if (!job) {
      throw new Error(`Rule draft job ${input.jobId} not found.`);
    }

    const payloadJson = parseJsonColumn<Record<string, unknown>>(
      job.payload_json ?? {},
    );
    const parsedRule =
      payloadJson &&
      typeof payloadJson === "object" &&
      "parsedRule" in payloadJson &&
      typeof payloadJson.parsedRule === "object"
        ? (payloadJson.parsedRule as Record<string, unknown>)
        : null;

    if (!parsedRule) {
      throw new Error("Rule draft has not been parsed yet.");
    }

    const createResult = await createRule({
      priority: Number(parsedRule.priority ?? 60),
      scopeJson: (parsedRule.scopeJson ?? {}) as Record<string, unknown>,
      conditionsJson: (parsedRule.conditionsJson ?? {}) as Record<
        string,
        unknown
      >,
      outputsJson: (parsedRule.outputsJson ?? {}) as Record<string, unknown>,
      actorName: input.actorName,
      sourceChannel: input.sourceChannel,
      apply: input.apply,
    });

    if (input.apply) {
      await sql`
        update public.jobs
        set payload_json = ${serializeJson(sql, {
          ...payloadJson,
          appliedRuleId: createResult.ruleId,
        })}::jsonb
        where id = ${input.jobId}
      `;
    }

    return { applied: input.apply, ruleId: createResult.ruleId };
  });
}
