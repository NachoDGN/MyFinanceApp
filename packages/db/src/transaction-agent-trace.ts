import type {
  TransactionQuestionEvidence,
  TransactionQuestionTraceEvent,
} from "@myfinance/domain";

import { serializeJson } from "./sql-json";
import type { SqlClient } from "./sql-runtime";

function isMissingTraceTableError(error: unknown) {
  const message =
    error instanceof Error ? error.message.toLowerCase() : String(error);
  return (
    message.includes("transaction_agent_runs") ||
    message.includes("transaction_agent_events") ||
    message.includes("transaction_agent_evidence") ||
    message.includes("does not exist")
  );
}

export async function createTransactionAgentRun(
  sql: SqlClient,
  input: {
    userId: string;
    question: string;
    executorModel: string;
    settingsJson: Record<string, unknown>;
  },
) {
  try {
    const rows = await sql`
      insert into public.transaction_agent_runs (
        user_id,
        question,
        executor_model,
        settings_json
      ) values (
        ${input.userId},
        ${input.question},
        ${input.executorModel},
        ${serializeJson(sql, input.settingsJson)}::jsonb
      )
      returning id
    `;

    return typeof rows[0]?.id === "string" ? rows[0].id : null;
  } catch (error) {
    if (isMissingTraceTableError(error)) {
      return null;
    }
    throw error;
  }
}

export async function appendTransactionAgentEvent(
  sql: SqlClient,
  input: TransactionQuestionTraceEvent & {
    runId: string | null;
    payload?: Record<string, unknown>;
  },
) {
  if (!input.runId) {
    return null;
  }

  try {
    const rows = await sql`
      insert into public.transaction_agent_events (
        run_id,
        step_index,
        actor,
        event_type,
        summary,
        payload,
        latency_ms
      ) values (
        ${input.runId},
        ${input.stepIndex},
        ${input.actor},
        ${input.eventType},
        ${input.summary},
        ${serializeJson(sql, input.payload ?? {})}::jsonb,
        ${input.latencyMs ?? null}
      )
      returning id
    `;

    return typeof rows[0]?.id === "bigint" ||
      typeof rows[0]?.id === "number" ||
      typeof rows[0]?.id === "string"
      ? String(rows[0].id)
      : null;
  } catch (error) {
    if (isMissingTraceTableError(error)) {
      return null;
    }
    throw error;
  }
}

export async function appendTransactionAgentEvidence(
  sql: SqlClient,
  input: {
    runId: string | null;
    eventId: string | null;
    evidence: TransactionQuestionEvidence[];
  },
) {
  if (!input.runId || input.evidence.length === 0) {
    return;
  }

  try {
    for (const evidence of input.evidence) {
      await sql`
        insert into public.transaction_agent_evidence (
          run_id,
          event_id,
          evidence_id,
          evidence_type,
          source_id,
          title,
          summary,
          metadata
        ) values (
          ${input.runId},
          ${input.eventId},
          ${evidence.id},
          ${evidence.type},
          ${evidence.sourceId ?? evidence.transactionId ?? null},
          ${evidence.title},
          ${evidence.summary},
          ${serializeJson(sql, evidence.metadata)}::jsonb
        )
      `;
    }
  } catch (error) {
    if (isMissingTraceTableError(error)) {
      return;
    }
    throw error;
  }
}

export async function completeTransactionAgentRun(
  sql: SqlClient,
  input: {
    runId: string | null;
    finalAnswer: string;
    citationIds: string[];
    stepCount: number;
    toolCallCount: number;
    status: "succeeded" | "failed";
    failureMessage?: string | null;
    metadataJson?: Record<string, unknown>;
  },
) {
  if (!input.runId) {
    return;
  }

  try {
    await sql`
      update public.transaction_agent_runs
      set status = ${input.status},
          final_answer = ${input.finalAnswer},
          citation_ids = ${input.citationIds},
          step_count = ${input.stepCount},
          tool_call_count = ${input.toolCallCount},
          failure_message = ${input.failureMessage ?? null},
          metadata_json = ${serializeJson(sql, input.metadataJson ?? {})}::jsonb,
          completed_at = timezone('utc', now()),
          updated_at = timezone('utc', now())
      where id = ${input.runId}
    `;
  } catch (error) {
    if (isMissingTraceTableError(error)) {
      return;
    }
    throw error;
  }
}
