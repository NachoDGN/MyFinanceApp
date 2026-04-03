import { randomUUID } from "node:crypto";

import postgres from "postgres";

import { enrichImportedTransaction, getTransactionClassifierConfig } from "@myfinance/classification";
import {
  parseRuleDraftRequest,
  buildImportedTransactions,
  createFixtureRepository,
  normalizeImportExecutionInput,
  runDeterministicImport,
  sanitizeImportResult,
  type AddOpeningPositionInput,
  type ApplyRuleDraftInput,
  type AuditEvent,
  type CreateRuleInput,
  type CreateTemplateInput,
  type DomainDataset,
  type FinanceRepository,
  type ImportExecutionInput,
  type ImportCommitResult,
  type ImportPreviewResult,
  type JobRunResult,
  type QueueRuleDraftInput,
  type Transaction,
  SEEDED_USER_ID,
  type UpdateTransactionInput,
} from "@myfinance/domain";

export interface DbRuntimeConfig {
  databaseUrl?: string;
  seededUserId: string;
}

export function getDbRuntimeConfig(): DbRuntimeConfig {
  return {
    databaseUrl: process.env.DATABASE_URL,
    seededUserId: process.env.APP_SEEDED_USER_ID ?? SEEDED_USER_ID,
  };
}

export function createSqlClient() {
  const { databaseUrl } = getDbRuntimeConfig();
  if (!databaseUrl) return null;
  return postgres(databaseUrl, {
    max: 1,
    prepare: false,
    transform: {
      undefined: null,
    },
  });
}

type SqlClient = ReturnType<typeof createSqlClient> extends infer T ? Exclude<T, null> : never;

async function withSeededUserContext<T>(
  runner: (sql: SqlClient) => Promise<T>,
): Promise<T | null> {
  const sql = createSqlClient();
  if (!sql) return null;
  const { seededUserId } = getDbRuntimeConfig();
  try {
    await sql`select set_config('app.current_user_id', ${seededUserId}, true)`;
    return await runner(sql);
  } finally {
    await sql.end({ timeout: 1 });
  }
}

function camelizeKey(value: string) {
  return value.replace(/_([a-z])/g, (_, character: string) => character.toUpperCase());
}

function camelizeValue<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((item) => camelizeValue(item)) as T;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        return camelizeValue(JSON.parse(trimmed)) as T;
      } catch {
        return value;
      }
    }
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString() as T;
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nested]) => [
        camelizeKey(key),
        camelizeValue(nested),
      ]),
    ) as T;
  }
  return value;
}

function mapFromSql<T>(value: unknown): T {
  return camelizeValue(value as T);
}

function serializeJson(sql: SqlClient, value: unknown) {
  return sql.json((value ?? {}) as Parameters<SqlClient["json"]>[0]);
}

function parseJsonColumn<T>(value: unknown): T {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return value as T;
    }
  }
  return value as T;
}

function createAuditEvent(
  sourceChannel: AuditEvent["sourceChannel"],
  actorName: string,
  commandName: string,
  objectType: string,
  objectId: string,
  beforeJson: Record<string, unknown> | null,
  afterJson: Record<string, unknown> | null,
): AuditEvent {
  return {
    id: randomUUID(),
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

class SqlFinanceRepository implements FinanceRepository {
  private userId = getDbRuntimeConfig().seededUserId;

  async getDataset(): Promise<DomainDataset> {
    const dataset = await withSeededUserContext(async (sql) => {
      const [
        profiles,
        entities,
        accounts,
        templates,
        importBatches,
        transactions,
        categories,
        rules,
        auditEvents,
        jobs,
        accountBalanceSnapshots,
        securities,
        securityAliases,
        securityPrices,
        fxRates,
        holdingAdjustments,
        investmentPositions,
        dailyPortfolioSnapshots,
        monthlyCashFlowRollups,
      ] = await Promise.all([
        sql`select * from public.profiles where id = ${this.userId} limit 1`,
        sql`select * from public.entities where user_id = ${this.userId} order by created_at`,
        sql`select * from public.accounts where user_id = ${this.userId} order by created_at`,
        sql`select * from public.import_templates where user_id = ${this.userId} order by created_at`,
        sql`select * from public.import_batches where user_id = ${this.userId} order by imported_at desc`,
        sql`select * from public.transactions where user_id = ${this.userId} order by transaction_date desc, created_at desc`,
        sql`select * from public.categories order by sort_order, code`,
        sql`select * from public.classification_rules where user_id = ${this.userId} order by priority`,
        sql`select * from public.audit_events order by created_at desc limit 200`,
        sql`select * from public.jobs order by created_at desc`,
        sql`select * from public.account_balance_snapshots where account_id in (select id from public.accounts where user_id = ${this.userId}) order by as_of_date desc`,
        sql`select * from public.securities order by display_symbol`,
        sql`select * from public.security_aliases order by created_at desc`,
        sql`select * from public.security_prices order by price_date desc, quote_timestamp desc`,
        sql`select * from public.fx_rates order by as_of_date desc`,
        sql`select * from public.holding_adjustments where user_id = ${this.userId} order by effective_date desc`,
        sql`select * from public.investment_positions where user_id = ${this.userId}`,
        sql`select * from public.daily_portfolio_snapshots where user_id = ${this.userId} order by snapshot_date desc`,
        sql`
          with income as (
            select entity_id, month, income_total_eur
            from public.mv_monthly_income_totals
            where user_id = ${this.userId}
          ),
          spending as (
            select entity_id, month, sum(spending_total_eur) as spending_total_eur
            from public.mv_monthly_spending_totals
            where user_id = ${this.userId}
            group by entity_id, month
          )
          select
            coalesce(income.entity_id, spending.entity_id) as entity_id,
            coalesce(income.month, spending.month) as month,
            coalesce(income.income_total_eur, 0) as income_eur,
            coalesce(spending.spending_total_eur, 0) as spending_eur,
            coalesce(income.income_total_eur, 0) - coalesce(spending.spending_total_eur, 0) as operating_net_eur
          from income
          full outer join spending
            on spending.entity_id = income.entity_id
           and spending.month = income.month
          order by month asc
        `,
      ]);

      if (!profiles[0]) {
        return null;
      }

      return {
        schemaVersion: "v1" as const,
        profile: mapFromSql<DomainDataset["profile"]>(profiles[0]),
        entities: mapFromSql<DomainDataset["entities"]>(entities),
        accounts: mapFromSql<DomainDataset["accounts"]>(accounts),
        templates: mapFromSql<DomainDataset["templates"]>(templates),
        importBatches: mapFromSql<DomainDataset["importBatches"]>(importBatches),
        transactions: mapFromSql<DomainDataset["transactions"]>(transactions),
        categories: mapFromSql<DomainDataset["categories"]>(categories),
        rules: mapFromSql<DomainDataset["rules"]>(rules),
        auditEvents: mapFromSql<DomainDataset["auditEvents"]>(auditEvents),
        jobs: mapFromSql<DomainDataset["jobs"]>(jobs),
        accountBalanceSnapshots: mapFromSql<DomainDataset["accountBalanceSnapshots"]>(
          accountBalanceSnapshots,
        ),
        securities: mapFromSql<DomainDataset["securities"]>(securities),
        securityAliases: mapFromSql<DomainDataset["securityAliases"]>(securityAliases),
        securityPrices: mapFromSql<DomainDataset["securityPrices"]>(securityPrices),
        fxRates: mapFromSql<DomainDataset["fxRates"]>(fxRates),
        holdingAdjustments: mapFromSql<DomainDataset["holdingAdjustments"]>(
          holdingAdjustments,
        ),
        investmentPositions: mapFromSql<DomainDataset["investmentPositions"]>(
          investmentPositions,
        ),
        dailyPortfolioSnapshots: mapFromSql<DomainDataset["dailyPortfolioSnapshots"]>(
          dailyPortfolioSnapshots,
        ),
        monthlyCashFlowRollups: mapFromSql<DomainDataset["monthlyCashFlowRollups"]>(
          monthlyCashFlowRollups,
        ),
      };
    });

    return dataset ?? (await createFixtureRepository().getDataset());
  }

  async updateTransaction(
    input: UpdateTransactionInput,
  ): Promise<{
    applied: boolean;
    transaction: Transaction;
    auditEvent: AuditEvent;
    generatedRuleId?: string;
  }> {
    const result = await withSeededUserContext(async (sql) => {
      const before = await sql`
        select * from public.transactions
        where id = ${input.transactionId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = before[0];
      if (!beforeRow) {
        throw new Error(`Transaction ${input.transactionId} not found.`);
      }

      const patch = camelizeValue(input.patch);
      const updatePayload: Record<string, unknown> = {
        updated_at: new Date().toISOString(),
      };
      if (patch.transactionClass !== undefined) updatePayload.transaction_class = patch.transactionClass;
      if (patch.categoryCode !== undefined) updatePayload.category_code = patch.categoryCode;
      if (patch.economicEntityId !== undefined) updatePayload.economic_entity_id = patch.economicEntityId;
      if (patch.merchantNormalized !== undefined) updatePayload.merchant_normalized = patch.merchantNormalized;
      if (patch.counterpartyName !== undefined) updatePayload.counterparty_name = patch.counterpartyName;
      if (patch.needsReview !== undefined) updatePayload.needs_review = patch.needsReview;
      if (patch.reviewReason !== undefined) updatePayload.review_reason = patch.reviewReason;
      if (patch.excludeFromAnalytics !== undefined) updatePayload.exclude_from_analytics = patch.excludeFromAnalytics;
      if (patch.securityId !== undefined) updatePayload.security_id = patch.securityId;
      if (patch.manualNotes !== undefined) updatePayload.manual_notes = patch.manualNotes;
      if (Object.keys(input.patch).length > 0) {
        updatePayload.classification_status = "manual_override";
        updatePayload.classification_source = "manual";
        updatePayload.classification_confidence = 1;
      }

      const after = input.apply
        ? await sql`
            update public.transactions
            set ${sql(updatePayload)}
            where id = ${input.transactionId}
              and user_id = ${this.userId}
            returning *
          `
        : [{ ...beforeRow, ...updatePayload }];

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "transactions.update",
        "transaction",
        input.transactionId,
        beforeRow,
        after[0],
      );

      let generatedRuleId: string | undefined;

      if (input.apply) {
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
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;

        if (input.createRuleFromTransaction) {
          const persistedTransaction = mapFromSql<Transaction>(after[0]);
          generatedRuleId = randomUUID();
          await sql`
            insert into public.classification_rules (
              id,
              user_id,
              priority,
              active,
              scope_json,
              conditions_json,
              outputs_json,
              created_from_transaction_id,
              auto_generated
            ) values (
              ${generatedRuleId},
              ${this.userId},
              50,
              true,
              ${serializeJson(sql, { account_id: persistedTransaction.accountId })}::jsonb,
              ${serializeJson(sql, {
                normalized_description_regex: persistedTransaction.descriptionClean,
              })}::jsonb,
              ${serializeJson(sql, {
                transaction_class: persistedTransaction.transactionClass,
                category_code: persistedTransaction.categoryCode,
                economic_entity_id_override: persistedTransaction.economicEntityId,
              })}::jsonb,
              ${persistedTransaction.id},
              true
            )
          `;
        }
      }

      return {
        applied: input.apply,
        transaction: mapFromSql<Transaction>(after[0]),
        auditEvent,
        generatedRuleId,
      };
    });

    return result ?? createFixtureRepository().updateTransaction(input);
  }

  async createRule(input: CreateRuleInput) {
    const result = await withSeededUserContext(async (sql) => {
      const ruleId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.classification_rules (
            id,
            user_id,
            priority,
            active,
            scope_json,
            conditions_json,
            outputs_json,
            auto_generated
          ) values (
            ${ruleId},
            ${this.userId},
            ${input.priority},
            true,
            ${serializeJson(sql, input.scopeJson)}::jsonb,
            ${serializeJson(sql, input.conditionsJson)}::jsonb,
            ${serializeJson(sql, input.outputsJson)}::jsonb,
            false
          )
        `;
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.create",
          "classification_rule",
          ruleId,
          null,
          {
            priority: input.priority,
            scopeJson: input.scopeJson,
            conditionsJson: input.conditionsJson,
            outputsJson: input.outputsJson,
          },
        );
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
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }
      return { applied: input.apply, ruleId };
    });
    return result ?? createFixtureRepository().createRule(input);
  }

  async createTemplate(input: CreateTemplateInput) {
    const result = await withSeededUserContext(async (sql) => {
      const templateId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.import_templates (
            id,
            user_id,
            name,
            institution_name,
            compatible_account_type,
            file_kind,
            sheet_name,
            header_row_index,
            rows_to_skip_before_header,
            rows_to_skip_after_header,
            delimiter,
            encoding,
            decimal_separator,
            thousands_separator,
            date_format,
            default_currency,
            column_map_json,
            sign_logic_json,
            normalization_rules_json,
            active,
            version
          ) values (
            ${templateId},
            ${this.userId},
            ${input.template.name},
            ${input.template.institutionName},
            ${input.template.compatibleAccountType},
            ${input.template.fileKind},
            ${input.template.sheetName ?? null},
            ${input.template.headerRowIndex},
            ${input.template.rowsToSkipBeforeHeader},
            ${input.template.rowsToSkipAfterHeader},
            ${input.template.delimiter ?? null},
            ${input.template.encoding ?? null},
            ${input.template.decimalSeparator ?? null},
            ${input.template.thousandsSeparator ?? null},
            ${input.template.dateFormat},
            ${input.template.defaultCurrency},
            ${serializeJson(sql, input.template.columnMapJson)}::jsonb,
            ${serializeJson(sql, input.template.signLogicJson)}::jsonb,
            ${serializeJson(sql, input.template.normalizationRulesJson)}::jsonb,
            ${input.template.active},
            1
          )
        `;
      }
      return { applied: input.apply, templateId };
    });
    return result ?? createFixtureRepository().createTemplate(input);
  }

  async addOpeningPosition(input: AddOpeningPositionInput) {
    const result = await withSeededUserContext(async (sql) => {
      const adjustmentId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.holding_adjustments ${sql({
            id: adjustmentId,
            user_id: this.userId,
            entity_id: input.entityId,
            account_id: input.accountId,
            security_id: input.securityId,
            effective_date: input.effectiveDate,
            share_delta: input.shareDelta,
            cost_basis_delta_eur: input.costBasisDeltaEur,
            reason: "opening_position",
            note: "Created from app/CLI.",
          })}
        `;
      }
      return { applied: input.apply, adjustmentId };
    });
    return result ?? createFixtureRepository().addOpeningPosition(input);
  }

  async queueRuleDraft(input: QueueRuleDraftInput) {
    const result = await withSeededUserContext(async (sql) => {
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
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }
      return { applied: input.apply, jobId };
    });
    return result ?? createFixtureRepository().queueRuleDraft(input);
  }

  async applyRuleDraft(input: ApplyRuleDraftInput) {
    const result = await withSeededUserContext(async (sql) => {
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

      const payloadJson = parseJsonColumn<Record<string, unknown>>(job.payload_json ?? {});
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

      const createResult = await this.createRule({
        priority: Number(parsedRule.priority ?? 60),
        scopeJson: (parsedRule.scopeJson ?? {}) as Record<string, unknown>,
        conditionsJson: (parsedRule.conditionsJson ?? {}) as Record<string, unknown>,
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
    return result ?? createFixtureRepository().applyRuleDraft(input);
  }

  async previewImport(input: ImportExecutionInput): Promise<ImportPreviewResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    if (!normalizedInput.filePath) {
      return createFixtureRepository().previewImport(normalizedInput);
    }

    const dataset = await this.getDataset();
    const rawResult = await runDeterministicImport(
      "preview",
      normalizedInput,
      dataset,
    );
    const prepared = buildImportedTransactions(
      dataset,
      normalizedInput,
      "preview-batch",
      rawResult.normalizedRows ?? [],
    );
    const publicResult = sanitizeImportResult(rawResult) as ImportPreviewResult;
    return {
      ...publicResult,
      rowCountDuplicates: prepared.duplicateCount,
    };
  }

  async commitImport(input: ImportExecutionInput): Promise<ImportCommitResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    const result = await withSeededUserContext(async (sql) => {
      const dataset = await this.getDataset();
      const commitResult = normalizedInput.filePath
        ? (await runDeterministicImport(
            "commit",
            normalizedInput,
            dataset,
          ))
        : null;
      const importBatchId =
        (commitResult as ImportCommitResult | null)?.importBatchId ?? randomUUID();
      const preparedTransactions =
        commitResult && normalizedInput.filePath
          ? buildImportedTransactions(
              dataset,
              normalizedInput,
              importBatchId,
              commitResult.normalizedRows ?? [],
            )
          : null;
      const preview =
        commitResult && normalizedInput.filePath
          ? ({
              ...(sanitizeImportResult(commitResult) as ImportCommitResult),
              rowCountDuplicates: preparedTransactions?.duplicateCount ?? 0,
            } satisfies ImportCommitResult)
          : await this.previewImport(normalizedInput);
      const jobsQueued =
        (commitResult as ImportCommitResult | null)?.jobsQueued ??
        ([
          "classification",
          "transfer_rematch",
          "position_rebuild",
          "metric_refresh",
          "insight_refresh",
        ] as const);
      await sql`
        insert into public.import_batches (
          id,
          user_id,
          account_id,
          template_id,
          storage_path,
          original_filename,
          file_sha256,
          status,
          row_count_detected,
          row_count_parsed,
          row_count_inserted,
          row_count_duplicates,
          row_count_failed,
          preview_summary_json,
          commit_summary_json,
          imported_by_actor,
          imported_at
        ) values (
          ${importBatchId},
          ${this.userId},
          ${normalizedInput.accountId},
          ${normalizedInput.templateId},
          ${
            normalizedInput.filePath
              ? `private-imports/local/${normalizedInput.originalFilename}`
              : `private-imports/manual/${normalizedInput.originalFilename}`
          },
          ${normalizedInput.originalFilename},
          ${randomUUID().replace(/-/g, "")},
          ${"committed"},
          ${preview.rowCountDetected},
          ${preview.rowCountParsed},
          ${preparedTransactions?.inserted.length ?? preview.rowCountParsed},
          ${preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates},
          ${preview.rowCountFailed},
          ${serializeJson(sql, {
            sampleRows: preview.sampleRows,
            parseErrors: preview.parseErrors,
            dateRange: preview.dateRange,
          })}::jsonb,
          ${serializeJson(sql, { jobsQueued })}::jsonb,
          ${"web-cli"},
          ${new Date().toISOString()}
        )
      `;
      for (const jobType of jobsQueued) {
        await sql`
          insert into public.jobs (
            id,
            job_type,
            payload_json,
            status,
            attempts,
            available_at
          ) values (
            ${randomUUID()},
            ${jobType},
            ${serializeJson(sql, { importBatchId, accountId: normalizedInput.accountId })}::jsonb,
            ${"queued"},
            0,
            ${new Date().toISOString()}
          )
        `;
      }
      const insertedTransactions: Transaction[] = [];
      if (preparedTransactions) {
        for (const transaction of preparedTransactions.inserted) {
          const inserted = await sql`
            insert into public.transactions (
              id,
              user_id,
              account_id,
              account_entity_id,
              economic_entity_id,
              import_batch_id,
              source_fingerprint,
              duplicate_key,
              transaction_date,
              posted_date,
              amount_original,
              currency_original,
              amount_base_eur,
              fx_rate_to_eur,
              description_raw,
              description_clean,
              merchant_normalized,
              counterparty_name,
              transaction_class,
              category_code,
              transfer_match_status,
              cross_entity_flag,
              reimbursement_status,
              classification_status,
              classification_source,
              classification_confidence,
              needs_review,
              review_reason,
              exclude_from_analytics,
              llm_payload,
              raw_payload,
              security_id,
              quantity,
              unit_price_original,
              created_at,
              updated_at
            ) values (
              ${transaction.id},
              ${transaction.userId},
              ${transaction.accountId},
              ${transaction.accountEntityId},
              ${transaction.economicEntityId},
              ${transaction.importBatchId ?? null},
              ${transaction.sourceFingerprint},
              ${transaction.duplicateKey ?? null},
              ${transaction.transactionDate},
              ${transaction.postedDate ?? null},
              ${transaction.amountOriginal},
              ${transaction.currencyOriginal},
              ${transaction.amountBaseEur},
              ${transaction.fxRateToEur ?? null},
              ${transaction.descriptionRaw},
              ${transaction.descriptionClean},
              ${transaction.merchantNormalized ?? null},
              ${transaction.counterpartyName ?? null},
              ${transaction.transactionClass},
              ${transaction.categoryCode ?? null},
              ${transaction.transferMatchStatus},
              ${transaction.crossEntityFlag},
              ${transaction.reimbursementStatus},
              ${transaction.classificationStatus},
              ${transaction.classificationSource},
              ${transaction.classificationConfidence},
              ${transaction.needsReview},
              ${transaction.reviewReason ?? null},
              ${transaction.excludeFromAnalytics},
              ${serializeJson(sql, transaction.llmPayload)}::jsonb,
              ${serializeJson(sql, transaction.rawPayload)}::jsonb,
              ${transaction.securityId ?? null},
              ${transaction.quantity ?? null},
              ${transaction.unitPriceOriginal ?? null},
              ${transaction.createdAt},
              ${transaction.updatedAt}
            )
            on conflict (user_id, source_fingerprint) do nothing
            returning id
          `;
          if (inserted.length > 0) {
            insertedTransactions.push(transaction);
          }
        }
      }
      await sql`
        update public.accounts
        set last_imported_at = ${new Date().toISOString()}
        where id = ${normalizedInput.accountId}
          and user_id = ${this.userId}
      `;
      const commitDuplicates =
        (preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates) +
        ((preparedTransactions?.inserted.length ?? 0) - insertedTransactions.length);
      await sql`
        update public.import_batches
        set row_count_inserted = ${insertedTransactions.length || (preparedTransactions ? 0 : preview.rowCountParsed)},
            row_count_duplicates = ${preparedTransactions ? commitDuplicates : preview.rowCountDuplicates},
            commit_summary_json = ${serializeJson(sql, {
              jobsQueued,
              transactionIds: insertedTransactions.map((transaction) => transaction.id),
            })}::jsonb
        where id = ${importBatchId}
          and user_id = ${this.userId}
      `;
      return {
        ...preview,
        importBatchId,
        rowCountInserted: insertedTransactions.length || (preparedTransactions ? 0 : preview.rowCountParsed),
        rowCountDuplicates: preparedTransactions ? commitDuplicates : preview.rowCountDuplicates,
        transactionIds: insertedTransactions.map((transaction) => transaction.id),
        jobsQueued: [...jobsQueued],
      };
    });
    return result ?? createFixtureRepository().commitImport(normalizedInput);
  }

  async runPendingJobs(apply: boolean): Promise<JobRunResult> {
    const result = await withSeededUserContext(async (sql) => {
      const queued = await sql`
        select * from public.jobs
        where status = 'queued'
        order by available_at asc
      `;
      const processedJobs: JobRunResult["processedJobs"] = [];
      if (apply && queued.length > 0) {
        const dataset = await this.getDataset();
        for (const job of queued) {
          const startedAt = new Date().toISOString();
          try {
            if (job.job_type === "rule_parse") {
              const payloadJson = parseJsonColumn<Record<string, unknown>>(job.payload_json ?? {});
              const requestText =
                payloadJson && typeof payloadJson.requestText === "string"
                  ? payloadJson.requestText
                  : "";
              if (!requestText) {
                throw new Error("Rule draft job is missing requestText.");
              }

              const parsedRule = await parseRuleDraftRequest(requestText, dataset);
              await sql`
                update public.jobs
                set status = 'completed',
                    attempts = attempts + 1,
                    started_at = ${startedAt},
                    finished_at = ${new Date().toISOString()},
                    last_error = null,
                    payload_json = ${serializeJson(sql, {
                      ...payloadJson,
                      parsedRule,
                    })}::jsonb
                where id = ${job.id}
              `;
              processedJobs.push({
                id: job.id,
                jobType: "rule_parse",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "classification") {
              const payloadJson = parseJsonColumn<Record<string, unknown>>(job.payload_json ?? {});
              const importBatchId =
                payloadJson && typeof payloadJson.importBatchId === "string"
                  ? payloadJson.importBatchId
                  : "";
              if (!importBatchId) {
                throw new Error("Classification job is missing importBatchId.");
              }

              const latestDataset = await this.getDataset();
              const rows = await sql`
                select * from public.transactions
                where user_id = ${this.userId}
                  and import_batch_id = ${importBatchId}
                  and coalesce(llm_payload->>'analysisStatus', 'pending') = 'pending'
                order by transaction_date asc, created_at asc
              `;

              let failedTransactions = 0;
              for (const row of rows) {
                const transaction = mapFromSql<Transaction>(row);
                const account = latestDataset.accounts.find(
                  (candidate) => candidate.id === transaction.accountId,
                );
                if (!account) {
                  throw new Error(`Account ${transaction.accountId} not found for classification.`);
                }

                try {
                  const decision = await enrichImportedTransaction(
                    latestDataset,
                    account,
                    transaction,
                  );
                  await sql`
                    update public.transactions
                    set transaction_class = ${decision.transactionClass},
                        category_code = ${decision.categoryCode ?? null},
                        merchant_normalized = ${decision.merchantNormalized ?? null},
                        counterparty_name = ${decision.counterpartyName ?? null},
                        economic_entity_id = ${decision.economicEntityId},
                        classification_status = ${decision.classificationStatus},
                        classification_source = ${decision.classificationSource},
                        classification_confidence = ${decision.classificationConfidence},
                        needs_review = ${decision.needsReview},
                        review_reason = ${decision.reviewReason ?? null},
                        llm_payload = ${serializeJson(sql, decision.llmPayload)}::jsonb,
                        updated_at = ${new Date().toISOString()}
                    where id = ${transaction.id}
                      and user_id = ${this.userId}
                  `;
                } catch (transactionError) {
                  failedTransactions += 1;
                  await sql`
                    update public.transactions
                    set needs_review = true,
                        review_reason = ${
                          transactionError instanceof Error
                            ? transactionError.message
                            : "Transaction enrichment failed."
                        },
                        llm_payload = ${serializeJson(sql, {
                          ...(parseJsonColumn<Record<string, unknown>>(row.llm_payload ?? {}) ?? {}),
                          analysisStatus: "failed",
                          explanation: null,
                          model: getTransactionClassifierConfig().model,
                          error:
                            transactionError instanceof Error
                              ? transactionError.message
                              : "Transaction enrichment failed.",
                          analyzedAt: new Date().toISOString(),
                        })}::jsonb,
                        updated_at = ${new Date().toISOString()}
                    where id = ${transaction.id}
                      and user_id = ${this.userId}
                  `;
                }
              }

              await sql`
                update public.import_batches
                set classification_triggered_at = ${new Date().toISOString()}
                where id = ${importBatchId}
                  and user_id = ${this.userId}
              `;
              await sql`
                update public.jobs
                set status = 'completed',
                    attempts = attempts + 1,
                    started_at = ${startedAt},
                    finished_at = ${new Date().toISOString()},
                    last_error = null,
                    payload_json = ${serializeJson(sql, {
                      ...payloadJson,
                      processedTransactions: rows.length,
                      failedTransactions,
                    })}::jsonb
                where id = ${job.id}
              `;
              processedJobs.push({
                id: job.id,
                jobType: "classification",
                status: "completed",
              });
              continue;
            }

            await sql`
              update public.jobs
              set status = 'completed',
                  attempts = attempts + 1,
                  started_at = ${startedAt},
                  finished_at = ${new Date().toISOString()},
                  last_error = null
              where id = ${job.id}
            `;
            processedJobs.push({
              id: job.id,
              jobType: job.job_type,
              status: "completed",
            });
          } catch (error) {
            await sql`
              update public.jobs
              set status = 'failed',
                  attempts = attempts + 1,
                  started_at = ${startedAt},
                  finished_at = ${new Date().toISOString()},
                  last_error = ${
                    error instanceof Error ? error.message : "Unknown job failure"
                  }
              where id = ${job.id}
            `;
            processedJobs.push({
              id: job.id,
              jobType: job.job_type,
              status: "failed",
            });
          }
        }
      }
      return {
        schemaVersion: "v1" as const,
        applied: apply,
        processedJobs: apply
          ? processedJobs
          : queued.map((job) => ({
              id: job.id,
              jobType: job.job_type,
              status: job.status,
            })),
        generatedAt: new Date().toISOString(),
      };
    });
    return result ?? createFixtureRepository().runPendingJobs(apply);
  }
}

export function createFinanceRepository(): FinanceRepository {
  return getDbRuntimeConfig().databaseUrl
    ? new SqlFinanceRepository()
    : createFixtureRepository();
}
