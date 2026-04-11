import { randomUUID } from "node:crypto";

import { loadDatasetForUser } from "./dataset-loader";
import {
  buildRevolutAuthorizationUrl,
  createSignedRevolutState,
  encryptBankSecret,
  exchangeRevolutAuthorizationCode,
  fetchRevolutAccounts,
  getRevolutRuntimeConfig,
  verifyRevolutWebhookSignature,
  verifyRevolutWebhookTimestamp,
  verifySignedRevolutState,
} from "./revolut";
import { serializeJson } from "./sql-json";
import { getDbRuntimeConfig, withSeededUserContext } from "./sql-runtime";
import {
  queueUniqueRevolutSyncJob,
  resolveOrCreateRevolutAccountLinks,
  REVOLUT_CONNECTION_LABEL,
  REVOLUT_PROVIDER_NAME,
  type BankSyncTrigger,
} from "./revolut-sync-support";

export async function beginRevolutAuthorization(input: { entityId: string }) {
  const config = getRevolutRuntimeConfig();
  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const entityRows = await sql`
      select *
      from public.entities
      where id = ${input.entityId}
        and user_id = ${userId}
      limit 1
    `;
    const entity = entityRows[0];
    if (!entity) {
      throw new Error(`Entity ${input.entityId} was not found.`);
    }
    if (entity.entity_kind !== "company") {
      throw new Error(
        "Revolut Business connections can only be attached to company entities.",
      );
    }

    const state = createSignedRevolutState(config, {
      userId,
      entityId: input.entityId,
    });

    return {
      url: buildRevolutAuthorizationUrl(config, state),
      state,
    };
  });
}

export async function completeRevolutAuthorization(input: {
  code: string;
  state: string;
}) {
  const config = getRevolutRuntimeConfig();
  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const statePayload = verifySignedRevolutState(config, input.state);
    const entityId =
      typeof statePayload.entityId === "string" ? statePayload.entityId : "";
    const stateUserId =
      typeof statePayload.userId === "string" ? statePayload.userId : "";
    if (!entityId || stateUserId !== userId) {
      throw new Error("Revolut OAuth state is invalid for this user session.");
    }

    const tokens = await exchangeRevolutAuthorizationCode(config, input.code);
    const revolutAccounts = await fetchRevolutAccounts(
      config,
      tokens.access_token,
    );
    const encryptedRefreshToken = tokens.refresh_token
      ? encryptBankSecret(config.masterKey, tokens.refresh_token)
      : null;
    if (!encryptedRefreshToken) {
      throw new Error("Revolut did not return a refresh token.");
    }

    const nowIso = new Date().toISOString();
    const upsertedConnections = await sql`
      insert into public.bank_connections ${sql({
        id: randomUUID(),
        user_id: userId,
        entity_id: entityId,
        provider: REVOLUT_PROVIDER_NAME,
        connection_label: REVOLUT_CONNECTION_LABEL,
        status: "active",
        encrypted_refresh_token: encryptedRefreshToken,
        external_business_id: null,
        last_cursor_created_at: null,
        last_successful_sync_at: null,
        last_sync_queued_at: null,
        last_webhook_at: null,
        auth_expires_at: new Date(
          Date.now() + tokens.expires_in * 1000,
        ).toISOString(),
        last_error: null,
        metadata_json: serializeJson(sql, {
          scopes: ["READ"],
          connectedVia: "oauth_callback",
        }),
        created_at: nowIso,
        updated_at: nowIso,
      } as Record<string, unknown>)}
      on conflict (user_id, provider, entity_id)
      do update set
        connection_label = excluded.connection_label,
        status = excluded.status,
        encrypted_refresh_token = excluded.encrypted_refresh_token,
        auth_expires_at = excluded.auth_expires_at,
        last_error = excluded.last_error,
        metadata_json = excluded.metadata_json,
        updated_at = excluded.updated_at
      returning id
    `;
    const connectionId = String(upsertedConnections[0]?.id ?? "");
    if (!connectionId) {
      throw new Error("Failed to persist the Revolut bank connection.");
    }

    let dataset = await loadDatasetForUser(sql, userId);
    const linked = await resolveOrCreateRevolutAccountLinks(sql, {
      userId,
      dataset,
      connectionId,
      entityId,
      revolutAccounts,
      actorName: "web-revolut-connect",
      sourceChannel: "web",
    });
    dataset = linked.dataset;

    await queueUniqueRevolutSyncJob(sql, {
      userId,
      connectionId,
      trigger: "oauth_callback",
    });

    return {
      connectionId,
      linkedAccountIds: linked.bankAccountLinks.map((link) => link.accountId),
    };
  });
}

export async function queueRevolutConnectionSync(input: {
  connectionId: string;
  trigger: Exclude<BankSyncTrigger, "oauth_callback" | "scheduled">;
}) {
  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const connectionRows = await sql`
      select id
      from public.bank_connections
      where id = ${input.connectionId}
        and user_id = ${userId}
        and provider = ${REVOLUT_PROVIDER_NAME}
      limit 1
    `;
    if (!connectionRows[0]) {
      throw new Error(`Bank connection ${input.connectionId} was not found.`);
    }

    return queueUniqueRevolutSyncJob(sql, {
      userId,
      connectionId: input.connectionId,
      trigger: input.trigger,
    });
  });
}

export async function processRevolutWebhookEvent(input: {
  headers: Record<string, string | null | undefined>;
  body: string;
}) {
  const config = getRevolutRuntimeConfig();
  const timestamp =
    input.headers["revolut-request-timestamp"] ??
    input.headers["Revolut-Request-Timestamp"] ??
    null;
  const signature =
    input.headers["revolut-signature"] ??
    input.headers["Revolut-Signature"] ??
    null;
  if (!config.webhookSigningSecret) {
    throw new Error(
      "REVOLUT_WEBHOOK_SIGNING_SECRET is required to validate Revolut webhooks.",
    );
  }
  if (!timestamp || !signature) {
    throw new Error("Revolut webhook is missing signature headers.");
  }
  if (!verifyRevolutWebhookTimestamp(timestamp)) {
    throw new Error("Revolut webhook timestamp is outside the allowed window.");
  }
  if (
    !verifyRevolutWebhookSignature({
      signingSecret: config.webhookSigningSecret,
      timestamp,
      signatureHeader: signature,
      body: input.body,
    })
  ) {
    throw new Error("Revolut webhook signature verification failed.");
  }

  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const connectionRows = await sql`
      select id
      from public.bank_connections
      where user_id = ${userId}
        and provider = ${REVOLUT_PROVIDER_NAME}
        and status = ${"active"}
    `;
    const nowIso = new Date().toISOString();
    const queuedConnectionIds: string[] = [];
    for (const row of connectionRows) {
      const connectionId = String(row.id ?? "");
      if (!connectionId) {
        continue;
      }
      const queued = await queueUniqueRevolutSyncJob(sql, {
        userId,
        connectionId,
        trigger: "webhook",
      });
      if (queued.queued) {
        queuedConnectionIds.push(connectionId);
      }
      await sql`
        update public.bank_connections
        set last_webhook_at = ${nowIso},
            updated_at = ${nowIso}
        where id = ${connectionId}
          and user_id = ${userId}
      `;
    }

    return {
      accepted: true,
      queuedConnectionIds,
    };
  });
}
