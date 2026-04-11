import type { AuditEvent } from "@myfinance/domain";

import { getDbRuntimeConfig, type SqlClient } from "./sql-runtime";

export function createAuditEvent(
  sourceChannel: AuditEvent["sourceChannel"],
  actorName: string,
  commandName: string,
  objectType: string,
  objectId: string,
  beforeJson: Record<string, unknown> | null,
  afterJson: Record<string, unknown> | null,
): AuditEvent {
  return {
    id: crypto.randomUUID(),
    actorType: "agent",
    actorId: getDbRuntimeConfig().seededUserId,
    actorName,
    sourceChannel,
    commandName,
    objectType,
    objectId,
    beforeJson,
    afterJson,
    createdAt: new Date().toISOString(),
    notes: null,
  };
}

export async function insertAuditEventRecord(
  sql: SqlClient,
  auditEvent: AuditEvent,
  notes: string | null = auditEvent.notes ?? null,
) {
  await sql`
    insert into public.audit_events ${sql({
      actor_type: auditEvent.actorType,
      actor_id: auditEvent.actorId,
      actor_name: auditEvent.actorName,
      source_channel: auditEvent.sourceChannel,
      command_name: auditEvent.commandName,
      object_type: auditEvent.objectType,
      object_id: auditEvent.objectId,
      before_json: auditEvent.beforeJson,
      after_json: auditEvent.afterJson,
      created_at: auditEvent.createdAt,
      notes,
    } as Record<string, unknown>)}
  `;
}
