import {
  getInvestmentTransactionClassifierConfig,
  getTransactionClassifierConfig,
} from "@myfinance/classification";
import type { AuditEvent } from "@myfinance/domain";
import { getImportTemplateInferenceConfig } from "@myfinance/ingestion";
import {
  buildPromptProfilePreview,
  listPromptProfileDefinitions,
  resolvePromptProfileSections,
  sanitizePromptProfileSectionOverrides,
  type PromptProfileId,
  type PromptProfileOverrides,
} from "@myfinance/llm";

import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import { serializeJson } from "./sql-json";
import {
  getDbRuntimeConfig,
  withSeededUserContext,
  type SqlClient,
} from "./sql-runtime";
import { getRuleParserConfig } from "./rule-drafts";

export interface PromptProfileModel {
  id: PromptProfileId;
  title: string;
  description: string;
  modelName: string;
  editableSections: ReturnType<typeof resolvePromptProfileSections>;
  preview: ReturnType<typeof buildPromptProfilePreview>;
}

function normalizePromptOverrides(value: unknown): PromptProfileOverrides {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).filter(
      ([, promptOverrides]) =>
        Boolean(promptOverrides) &&
        typeof promptOverrides === "object" &&
        !Array.isArray(promptOverrides),
    ),
  ) as PromptProfileOverrides;
}

export async function loadPromptOverrides(sql: SqlClient, userId: string) {
  const rows = await sql`
    select prompt_overrides_json
    from public.profiles
    where id = ${userId}
    limit 1
  `;

  return normalizePromptOverrides(rows[0]?.prompt_overrides_json);
}

function resolvePromptModelName(promptId: PromptProfileId) {
  switch (promptId) {
    case "cash_transaction_analyzer":
      return getTransactionClassifierConfig().model;
    case "investment_transaction_analyzer":
      return getInvestmentTransactionClassifierConfig().model;
    case "spreadsheet_table_start":
    case "spreadsheet_layout":
      return getImportTemplateInferenceConfig().model;
    case "rule_draft_parser":
      return getRuleParserConfig().model;
  }
}

function buildPromptProfileModels(promptOverrides: PromptProfileOverrides) {
  return listPromptProfileDefinitions().map((definition) => ({
    id: definition.id,
    title: definition.title,
    description: definition.description,
    modelName: resolvePromptModelName(definition.id),
    editableSections: resolvePromptProfileSections(
      definition.id,
      promptOverrides[definition.id],
    ),
    preview: buildPromptProfilePreview(
      definition.id,
      promptOverrides[definition.id],
    ),
  })) satisfies PromptProfileModel[];
}

export async function getPromptOverrides() {
  const userId = getDbRuntimeConfig().seededUserId;
  return withSeededUserContext((sql) => loadPromptOverrides(sql, userId));
}

export async function listPromptProfiles() {
  const userId = getDbRuntimeConfig().seededUserId;
  return withSeededUserContext(async (sql) => {
    const promptOverrides = await loadPromptOverrides(sql, userId);
    return buildPromptProfileModels(promptOverrides);
  });
}

export async function updatePromptProfile(input: {
  promptId: PromptProfileId;
  sections: Record<string, unknown>;
  actorName: string;
  sourceChannel: AuditEvent["sourceChannel"];
}) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const profileRows = await sql`
      select prompt_overrides_json
      from public.profiles
      where id = ${userId}
      limit 1
    `;
    if (!profileRows[0]) {
      throw new Error(`Profile ${userId} was not found.`);
    }

    const beforeOverrides = normalizePromptOverrides(
      profileRows[0].prompt_overrides_json,
    );
    const nextSections = sanitizePromptProfileSectionOverrides(
      input.promptId,
      input.sections,
    );
    const afterOverrides: PromptProfileOverrides = {
      ...beforeOverrides,
      [input.promptId]: nextSections,
    };

    await sql`
      update public.profiles
      set prompt_overrides_json = ${serializeJson(sql, afterOverrides)}::jsonb
      where id = ${userId}
    `;

    await insertAuditEventRecord(
      sql,
      createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "prompts.update",
        "profile",
        userId,
        { promptOverridesJson: beforeOverrides, promptId: input.promptId },
        { promptOverridesJson: afterOverrides, promptId: input.promptId },
      ),
      `Updated prompt template overrides for ${input.promptId}.`,
    );

    return buildPromptProfileModels(afterOverrides).find(
      (profile) => profile.id === input.promptId,
    );
  });
}
