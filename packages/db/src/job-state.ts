import { randomUUID } from "node:crypto";

import type { SqlClient } from "./sql-runtime";
import { parseJsonColumn, serializeJson } from "./sql-json";

const STALE_RUNNING_JOB_THRESHOLD_MS = 10 * 60_000;

export async function queueJob(
  sql: SqlClient,
  jobType: string,
  payloadJson: Record<string, unknown> = {},
  options: {
    availableAt?: string;
  } = {},
) {
  const jobId = randomUUID();
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
      ${jobType},
      ${serializeJson(sql, payloadJson)}::jsonb,
      ${"queued"},
      0,
      ${options.availableAt ?? new Date().toISOString()}
    )
  `;
  return jobId;
}

export async function supportsJobType(sql: SqlClient, jobType: string) {
  const rows = await sql`
    select exists (
      select 1
      from pg_enum enum_value
      join pg_type enum_type on enum_type.oid = enum_value.enumtypid
      join pg_namespace enum_namespace
        on enum_namespace.oid = enum_type.typnamespace
      where enum_namespace.nspname = 'public'
        and enum_type.typname = 'job_type'
        and enum_value.enumlabel = ${jobType}
    ) as supported
  `;

  return rows[0]?.supported === true;
}

export async function claimNextQueuedJob(sql: SqlClient, workerId: string) {
  const startedAt = new Date().toISOString();
  const claimed = await sql`
    with next_job as (
      select id
      from public.jobs
      where status = 'queued'
        and available_at <= ${startedAt}
      order by
        case job_type
          when 'review_reanalyze' then 0
          when 'rule_parse' then 1
          when 'bank_sync' then 2
          when 'classification' then 3
          when 'transfer_rematch' then 4
          when 'security_resolution' then 5
          when 'price_refresh' then 6
          when 'position_rebuild' then 7
          when 'metric_refresh' then 8
          when 'review_propagation' then 9
          else 99
        end asc,
        available_at asc,
        created_at asc
      limit 1
      for update skip locked
    )
    update public.jobs as job
    set status = 'running',
        started_at = ${startedAt},
        locked_by = ${workerId}
    from next_job
    where job.id = next_job.id
    returning job.*
  `;

  return claimed[0] ?? null;
}

export async function completeJob(
  sql: SqlClient,
  jobId: string,
  startedAt: string,
  payloadJson: Record<string, unknown>,
) {
  await sql`
    update public.jobs
    set status = 'completed',
        attempts = attempts + 1,
        started_at = ${startedAt},
        finished_at = ${new Date().toISOString()},
        last_error = null,
        locked_by = null,
        payload_json = ${serializeJson(sql, payloadJson)}::jsonb
    where id = ${jobId}
  `;
}

export async function failJob(
  sql: SqlClient,
  jobId: string,
  startedAt: string,
  error: unknown,
) {
  await sql`
    update public.jobs
    set status = 'failed',
        attempts = attempts + 1,
        started_at = ${startedAt},
        finished_at = ${new Date().toISOString()},
        last_error = ${
          error instanceof Error ? error.message : "Unknown job failure"
        },
        locked_by = null
    where id = ${jobId}
  `;
}

export async function updateRunningJobPayload(
  sql: SqlClient,
  jobId: string,
  payloadJson: Record<string, unknown>,
) {
  await sql`
    update public.jobs
    set payload_json = ${serializeJson(sql, {
      ...payloadJson,
      heartbeatAt: new Date().toISOString(),
    })}::jsonb
    where id = ${jobId}
      and status = 'running'
  `;
}

function parseTimestampMs(value: unknown) {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export async function recoverStaleRunningJobs(sql: SqlClient) {
  const rows = await sql`
    select *
    from public.jobs
    where status = 'running'
  `;
  if (rows.length === 0) {
    return [];
  }

  const cutoffMs = Date.now() - STALE_RUNNING_JOB_THRESHOLD_MS;
  const staleJobIds = rows
    .filter((row) => {
      const payloadJson = parseJsonColumn<Record<string, unknown>>(
        row.payload_json ?? {},
      );
      const heartbeatMs = parseTimestampMs(payloadJson.heartbeatAt);
      const startedAtMs = parseTimestampMs(row.started_at);
      const availableAtMs = parseTimestampMs(row.available_at);
      const referenceMs = heartbeatMs ?? startedAtMs ?? availableAtMs;
      return referenceMs !== null && referenceMs <= cutoffMs;
    })
    .map((row) => row.id as string);

  if (staleJobIds.length === 0) {
    return [];
  }

  return sql`
    update public.jobs
    set status = 'queued',
        available_at = ${new Date().toISOString()},
        started_at = null,
        finished_at = null,
        last_error = 'Recovered stale running job after worker interruption.',
        locked_by = null
    where id in ${sql(staleJobIds)}
    returning *
  `;
}
