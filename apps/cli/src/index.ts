#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { extname } from "node:path";

import { Command } from "commander";

import {
  buildDashboardSummary,
  buildInsights,
  buildMetricResult,
} from "@myfinance/analytics";
import { createFinanceRepository, getDbRuntimeConfig } from "@myfinance/db";
import {
  buildFundOrderHistoryImportPlan,
  createDefaultColumnMappings,
  createTemplateConfig,
  FinanceDomainService,
  getScopeLatestDate,
  isCanonicalFieldKey,
  parseMyInvestorFundOrderHistoryText,
  reconcileFundOrderHistoryImportPlan,
  resolvePeriodSelection,
  signModeOptions,
  todayIso,
  type TemplateColumnMapping,
} from "@myfinance/domain";
import { parseMyInvestorFundOrderHistorySpreadsheet } from "@myfinance/ingestion";
import { createMarketDataProvider } from "@myfinance/market-data";

const repository = createFinanceRepository();
const domain = new FinanceDomainService(repository);

type CommonOptions = {
  scope?: string;
  currency?: string;
  json?: boolean;
  period?: string;
  asOf?: string;
  start?: string;
  end?: string;
};

async function resolveScope(scopeValue = "consolidated") {
  const dataset = await repository.getDataset();
  if (scopeValue === "consolidated")
    return { dataset, scope: { kind: "consolidated" as const } };
  if (scopeValue.startsWith("account:")) {
    return {
      dataset,
      scope: {
        kind: "account" as const,
        accountId: scopeValue.replace("account:", ""),
      },
    };
  }
  const entity = dataset.entities.find((row) => row.slug === scopeValue);
  return {
    dataset,
    scope: { kind: "entity" as const, entityId: entity?.id },
  };
}

function render(output: unknown, json = false) {
  if (json) {
    process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
    return;
  }
  if (typeof output === "string") {
    process.stdout.write(`${output}\n`);
    return;
  }
  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
}

function getDisplayCurrency(options: CommonOptions) {
  return options.currency === "USD" ? "USD" : "EUR";
}

function getResolvedReferenceDate(
  dataset: Awaited<ReturnType<typeof repository.getDataset>>,
  scope: Awaited<ReturnType<typeof resolveScope>>["scope"],
  options: CommonOptions,
) {
  return options.asOf ?? getScopeLatestDate(dataset, scope, todayIso());
}

function getPeriod(options: CommonOptions, referenceDate: string) {
  return resolvePeriodSelection({
    preset: options.period,
    start: options.start,
    end: options.end,
    referenceDate,
  });
}

function parseTemplateMappings(mapOptions?: string[]) {
  if (!mapOptions || mapOptions.length === 0) {
    return createDefaultColumnMappings();
  }

  return mapOptions.map((entry) => {
    const separatorIndex = entry.indexOf("=");
    if (separatorIndex <= 0 || separatorIndex === entry.length - 1) {
      throw new Error(`Invalid mapping "${entry}". Use --map target=source.`);
    }

    const target = entry.slice(0, separatorIndex).trim();
    const source = entry.slice(separatorIndex + 1).trim();
    if (!isCanonicalFieldKey(target)) {
      throw new Error(`Unknown canonical field: ${target}`);
    }
    if (!source) {
      throw new Error(`Mapping for ${target} requires a source column.`);
    }

    return {
      target,
      source,
    } satisfies TemplateColumnMapping;
  });
}

const program = new Command();

program
  .name("myfinance")
  .description("Agent-facing CLI for MyFinanceApp")
  .version("0.1.0");

program
  .command("dashboard summary")
  .option(
    "--scope <scope>",
    "consolidated, personal, company_a, company_b, or account:<id>",
  )
  .option("--currency <currency>", "EUR or USD", "EUR")
  .option("--period <period>", "week, mtd, ytd, 24m, or custom", "mtd")
  .option("--as-of <date>", "Reference date in YYYY-MM-DD format")
  .option("--start <date>", "Custom period start in YYYY-MM-DD format")
  .option("--end <date>", "Custom period end in YYYY-MM-DD format")
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const referenceDate = getResolvedReferenceDate(dataset, scope, options);
    const summary = buildDashboardSummary(dataset, {
      scope,
      displayCurrency: getDisplayCurrency(options),
      period: getPeriod(options, referenceDate),
      referenceDate,
    });
    render(summary, options.json);
  });

program
  .command("metrics get")
  .argument("<metricId>")
  .option(
    "--scope <scope>",
    "consolidated, personal, company_a, company_b, or account:<id>",
  )
  .option("--currency <currency>", "EUR or USD", "EUR")
  .option("--period <period>", "week, mtd, ytd, 24m, or custom", "mtd")
  .option("--as-of <date>", "Reference date in YYYY-MM-DD format")
  .option("--start <date>", "Custom period start in YYYY-MM-DD format")
  .option("--end <date>", "Custom period end in YYYY-MM-DD format")
  .option("--json", "Output JSON")
  .action(async (metricId: string, options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const referenceDate = getResolvedReferenceDate(dataset, scope, options);
    const metric = buildMetricResult(
      dataset,
      scope,
      getDisplayCurrency(options),
      metricId,
      {
        referenceDate,
        period: getPeriod(options, referenceDate),
      },
    );
    render(metric, options.json);
  });

program
  .command("insights list")
  .option(
    "--scope <scope>",
    "consolidated, personal, company_a, company_b, or account:<id>",
  )
  .option("--period <period>", "week, mtd, ytd, 24m, or custom", "mtd")
  .option("--as-of <date>", "Reference date in YYYY-MM-DD format")
  .option("--start <date>", "Custom period start in YYYY-MM-DD format")
  .option("--end <date>", "Custom period end in YYYY-MM-DD format")
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const referenceDate = getResolvedReferenceDate(dataset, scope, options);
    const insights = {
      schemaVersion: "v1",
      insights: buildInsights(dataset, scope, {
        referenceDate,
        period: getPeriod(options, referenceDate),
      }),
      generatedAt: new Date().toISOString(),
    };
    render(insights, options.json);
  });

program
  .command("transactions list")
  .option(
    "--scope <scope>",
    "consolidated, personal, company_a, company_b, or account:<id>",
  )
  .option("--period <period>", "week, mtd, ytd, 24m, or custom", "mtd")
  .option("--as-of <date>", "Reference date in YYYY-MM-DD format")
  .option("--start <date>", "Custom period start in YYYY-MM-DD format")
  .option("--end <date>", "Custom period end in YYYY-MM-DD format")
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const referenceDate = getResolvedReferenceDate(dataset, scope, options);
    const result = await domain.listTransactions(scope, {
      referenceDate,
      period: getPeriod(options, referenceDate),
    });
    render(result, options.json);
  });

program
  .command("transaction update")
  .argument("<transactionId>")
  .option("--class <transactionClass>")
  .option("--category <categoryCode>")
  .option("--entity <entitySlug>")
  .option("--security <securityId>")
  .option("--quantity <quantity>")
  .option("--note <manualNote>")
  .option("--needs-review", "Mark the row as needs review")
  .option("--create-rule", "Also create a reusable rule")
  .option("--apply", "Persist the change")
  .option("--json", "Output JSON")
  .action(
    async (
      transactionId: string,
      options: CommonOptions & Record<string, string | boolean | undefined>,
    ) => {
      const dataset = await repository.getDataset();
      const entityId =
        typeof options.entity === "string"
          ? dataset.entities.find((row) => row.slug === options.entity)?.id
          : undefined;
      const result = await domain.updateTransaction({
        transactionId,
        patch: {
          transactionClass:
            typeof options.class === "string"
              ? (options.class as never)
              : undefined,
          categoryCode:
            typeof options.category === "string" ? options.category : undefined,
          economicEntityId: entityId,
          securityId:
            typeof options.security === "string" ? options.security : undefined,
          quantity:
            typeof options.quantity === "string" ? options.quantity : undefined,
          manualNotes:
            typeof options.note === "string" ? options.note : undefined,
          needsReview: options.needsReview ? true : undefined,
        },
        createRuleFromTransaction: Boolean(options.createRule),
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

const importsCommand = program.command("imports");

importsCommand
  .command("preview")
  .requiredOption("--account <accountId>")
  .requiredOption("--template <templateId>")
  .requiredOption(
    "--file <filePath>",
    "Local file path passed to the pandas ingest wrapper",
  )
  .option("--json", "Output JSON")
  .action(
    async (options: {
      account: string;
      template: string;
      file: string;
      json?: boolean;
    }) => {
      const result = await domain.previewImport({
        accountId: options.account,
        templateId: options.template,
        filePath: options.file,
      });
      render(result, options.json);
    },
  );

importsCommand
  .command("commit")
  .requiredOption("--account <accountId>")
  .requiredOption("--template <templateId>")
  .requiredOption(
    "--file <filePath>",
    "Local file path passed to the pandas ingest wrapper",
  )
  .option("--apply", "Persist the commit")
  .option("--json", "Output JSON")
  .action(
    async (options: {
      account: string;
      template: string;
      file: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const result = options.apply
        ? await domain.commitImport({
            accountId: options.account,
            templateId: options.template,
            filePath: options.file,
          })
        : await domain.previewImport({
            accountId: options.account,
            templateId: options.template,
            filePath: options.file,
          });
      render(result, options.json);
    },
  );

const templatesCommand = program.command("templates");

templatesCommand
  .command("list")
  .option("--json", "Output JSON")
  .action(async (options: { json?: boolean }) => {
    render(await domain.listTemplates(), options.json);
  });

templatesCommand
  .command("create")
  .requiredOption("--name <name>")
  .requiredOption("--institution <institution>")
  .requiredOption("--account-type <accountType>")
  .requiredOption("--file-kind <fileKind>")
  .requiredOption("--default-currency <currency>")
  .option(
    "--map <target=source>",
    "Add a canonical field mapping",
    (value, rows: string[] = []) => [...rows, value],
    [],
  )
  .option(
    "--sign-mode <mode>",
    `signed_amount, amount_direction_column, or debit_credit_columns`,
    "signed_amount",
  )
  .option("--invert-sign", "Invert the parsed amount sign")
  .option("--direction-column <column>")
  .option("--debit-column <column>")
  .option("--credit-column <column>")
  .option("--debit-values <values>", "Comma-separated values treated as debits")
  .option(
    "--credit-values <values>",
    "Comma-separated values treated as credits",
  )
  .option("--date-day-first", "Parse ambiguous dates as day-first")
  .option("--date-month-first", "Parse ambiguous dates as month-first")
  .option("--apply", "Persist the template")
  .option("--json", "Output JSON")
  .action(
    async (options: {
      name: string;
      institution: string;
      accountType: string;
      fileKind: "csv" | "xls" | "xlsx";
      defaultCurrency: string;
      map?: string[];
      signMode?: string;
      invertSign?: boolean;
      directionColumn?: string;
      debitColumn?: string;
      creditColumn?: string;
      debitValues?: string;
      creditValues?: string;
      dateDayFirst?: boolean;
      dateMonthFirst?: boolean;
      apply?: boolean;
      json?: boolean;
    }) => {
      if (
        !signModeOptions.includes(
          (options.signMode ?? "signed_amount") as never,
        )
      ) {
        throw new Error(`Unsupported sign mode: ${options.signMode}`);
      }

      const { columnMapJson, signLogicJson, normalizationRulesJson } =
        createTemplateConfig({
          columnMappings: parseTemplateMappings(options.map),
          signMode: (options.signMode ??
            "signed_amount") as (typeof signModeOptions)[number],
          invertSign: Boolean(options.invertSign),
          directionColumn: options.directionColumn,
          debitColumn: options.debitColumn,
          creditColumn: options.creditColumn,
          debitValuesText: options.debitValues,
          creditValuesText: options.creditValues,
          dateDayFirst: options.dateMonthFirst ? false : true,
        });
      const { seededUserId } = getDbRuntimeConfig();
      const result = await domain.createTemplate({
        template: {
          userId: seededUserId,
          name: options.name,
          institutionName: options.institution,
          compatibleAccountType: options.accountType as never,
          fileKind: options.fileKind,
          sheetName: null,
          headerRowIndex: 1,
          rowsToSkipBeforeHeader: 0,
          rowsToSkipAfterHeader: 0,
          delimiter: ",",
          encoding: "utf-8",
          decimalSeparator: ".",
          thousandsSeparator: ",",
          dateFormat: "%Y-%m-%d",
          defaultCurrency: options.defaultCurrency,
          columnMapJson,
          signLogicJson,
          normalizationRulesJson,
          active: true,
        },
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

const rulesCommand = program.command("rules");

rulesCommand
  .command("list")
  .option("--json", "Output JSON")
  .action(async (options: { json?: boolean }) => {
    render(await domain.listRules(), options.json);
  });

rulesCommand
  .command("drafts")
  .option("--json", "Output JSON")
  .action(async (options: { json?: boolean }) => {
    render(await domain.listRuleDrafts(), options.json);
  });

rulesCommand
  .command("queue-draft")
  .requiredOption("--text <requestText>")
  .option("--apply", "Persist the draft job")
  .option("--json", "Output JSON")
  .action(
    async (options: { text: string; apply?: boolean; json?: boolean }) => {
      const result = await domain.queueRuleDraft({
        requestText: options.text,
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

rulesCommand
  .command("apply-draft")
  .requiredOption("--job <jobId>")
  .option("--apply", "Persist the resulting rule")
  .option("--json", "Output JSON")
  .action(async (options: { job: string; apply?: boolean; json?: boolean }) => {
    const result = await domain.applyRuleDraft({
      jobId: options.job,
      actorName: "cli",
      sourceChannel: "cli",
      apply: Boolean(options.apply),
    });
    render(result, options.json);
  });

rulesCommand
  .command("create")
  .requiredOption("--priority <priority>")
  .requiredOption("--regex <regex>")
  .requiredOption("--class <transactionClass>")
  .requiredOption("--category <categoryCode>")
  .option("--apply", "Persist the rule")
  .option("--json", "Output JSON")
  .action(
    async (options: {
      priority: string;
      regex: string;
      class: string;
      category: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const result = await domain.createRule({
        priority: Number(options.priority),
        scopeJson: { global: true },
        conditionsJson: { normalized_description_regex: options.regex },
        outputsJson: {
          transaction_class: options.class,
          category_code: options.category,
        },
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

const investmentsCommand = program.command("investments");

investmentsCommand
  .command("holdings")
  .option(
    "--scope <scope>",
    "consolidated, personal, company_a, company_b, or account:<id>",
  )
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { scope } = await resolveScope(options.scope);
    render(await domain.listHoldings(scope), options.json);
  });

investmentsCommand
  .command("resolve-security")
  .requiredOption("--transaction <transactionId>")
  .requiredOption("--security <securityId>")
  .option("--apply", "Persist the resolution")
  .option("--json", "Output JSON")
  .action(
    async (options: {
      transaction: string;
      security: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const result = await domain.updateTransaction({
        transactionId: options.transaction,
        patch: {
          securityId: options.security,
          needsReview: false,
          reviewReason: null,
        },
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

const positionsCommand = program.command("positions");

positionsCommand
  .command("add-opening")
  .requiredOption("--account <accountId>")
  .requiredOption("--entity <entitySlug>")
  .requiredOption("--security <securityId>")
  .requiredOption("--date <effectiveDate>")
  .requiredOption("--quantity <shareDelta>")
  .option("--cost-basis <costBasisDeltaEur>")
  .option("--apply", "Persist the adjustment")
  .option("--json", "Output JSON")
  .action(
    async (options: {
      account: string;
      entity: string;
      security: string;
      date: string;
      quantity: string;
      costBasis?: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const dataset = await repository.getDataset();
      const entityId = dataset.entities.find(
        (row) => row.slug === options.entity,
      )?.id;
      if (!entityId) {
        throw new Error(`Unknown entity slug: ${options.entity}`);
      }
      const result = await domain.addOpeningPosition({
        accountId: options.account,
        entityId,
        securityId: options.security,
        effectiveDate: options.date,
        shareDelta: options.quantity,
        costBasisDeltaEur: options.costBasis ?? null,
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

positionsCommand
  .command("import-fund-history")
  .requiredOption("--account <accountId>")
  .requiredOption("--entity <entitySlug>")
  .requiredOption("--file <filePath>")
  .option(
    "--apply",
    "Persist the transaction quantity fixes and opening positions",
  )
  .option("--json", "Output JSON")
  .action(
    async (options: {
      account: string;
      entity: string;
      file: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const dataset = await repository.getDataset();
      const entityId = dataset.entities.find(
        (row) => row.slug === options.entity,
      )?.id;
      if (!entityId) {
        throw new Error(`Unknown entity slug: ${options.entity}`);
      }

      const parsedRows = await readFundHistoryRows(
        options.file,
        options.account,
      );
      const plan = buildFundOrderHistoryImportPlan(
        dataset,
        options.account,
        parsedRows,
      );
      const reconciliation = reconcileFundOrderHistoryImportPlan(
        dataset,
        options.account,
        plan,
      );
      const applyChanges = Boolean(options.apply);

      const patchedTransactions = [];
      const deletedOpeningAdjustments = [];
      const createdOpeningPositions = [];

      if (applyChanges) {
        for (const patch of plan.matchedTransactionPatches) {
          patchedTransactions.push(
            await domain.updateTransaction({
              transactionId: patch.transactionId,
              patch: {
                securityId: patch.securityId,
                quantity: patch.quantity,
                unitPriceOriginal: patch.unitPriceOriginal,
              },
              actorName: "cli",
              sourceChannel: "cli",
              apply: true,
            }),
          );
        }

        for (const adjustment of reconciliation.staleOpeningAdjustments) {
          deletedOpeningAdjustments.push(
            await domain.deleteHoldingAdjustment({
              adjustmentId: adjustment.adjustmentId,
              actorName: "cli",
              sourceChannel: "cli",
              apply: true,
            }),
          );
        }

        for (const openingPosition of reconciliation.openingPositionsToCreate) {
          createdOpeningPositions.push(
            await domain.addOpeningPosition({
              accountId: options.account,
              entityId,
              securityId: openingPosition.securityId,
              effectiveDate: openingPosition.orderDate,
              shareDelta: openingPosition.quantity,
              costBasisDeltaEur: openingPosition.costBasisEur,
              actorName: "cli",
              sourceChannel: "cli",
              apply: true,
            }),
          );
        }

        await domain.runPendingJobs(true);
      }

      render(
        {
          schemaVersion: "v1",
          applied: applyChanges,
          parsedRowCount: plan.parsedRows.length,
          finalizedRowCount: plan.finalizedRows.length,
          rejectedRowCount: plan.rejectedRows.length,
          unresolvedRows: plan.unresolvedRows,
          matchedTransactionPatches: plan.matchedTransactionPatches,
          staleOpeningAdjustments: reconciliation.staleOpeningAdjustments,
          existingOpeningPositions: reconciliation.existingOpeningPositions,
          openingPositions: reconciliation.openingPositionsToCreate,
          patchedTransactionCount: patchedTransactions.length,
          deletedOpeningAdjustmentCount: deletedOpeningAdjustments.length,
          createdOpeningPositionCount: createdOpeningPositions.length,
          generatedAt: new Date().toISOString(),
        },
        options.json,
      );
    },
  );

const pricesCommand = program.command("prices");

pricesCommand
  .command("refresh")
  .option("--symbol <symbol>")
  .option("--apply", "Persist the refreshed price downstream")
  .option("--json", "Output JSON")
  .action(
    async (options: { symbol?: string; apply?: boolean; json?: boolean }) => {
      const dataset = await repository.getDataset();
      const provider = createMarketDataProvider(dataset);
      const symbols = options.symbol
        ? [options.symbol]
        : dataset.securities.map((security) => security.displaySymbol);
      const quotes = await Promise.all(
        symbols.map(async (symbol) => ({
          symbol,
          quote: await provider.getLatestQuote(symbol),
        })),
      );
      render(
        {
          schemaVersion: "v1",
          applied: Boolean(options.apply),
          quotes,
          generatedAt: new Date().toISOString(),
        },
        options.json,
      );
    },
  );

const jobsCommand = program.command("jobs");

jobsCommand
  .command("run")
  .option("--apply", "Process queued jobs instead of previewing them")
  .option("--json", "Output JSON")
  .action(async (options: { apply?: boolean; json?: boolean }) => {
    render(await domain.runPendingJobs(Boolean(options.apply)), options.json);
  });

program.parseAsync(process.argv);

function readFundHistoryText(filePath: string) {
  const buffer = readFileSync(filePath);
  try {
    return buffer.toString("utf8");
  } catch {
    return buffer.toString("latin1");
  }
}

async function readFundHistoryRows(filePath: string, accountId: string) {
  const extension = extname(filePath).toLowerCase();
  if ([".csv", ".xls", ".xlsx"].includes(extension)) {
    return parseMyInvestorFundOrderHistorySpreadsheet(filePath, accountId);
  }

  return parseMyInvestorFundOrderHistoryText(readFundHistoryText(filePath));
}
