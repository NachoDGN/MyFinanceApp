#!/usr/bin/env node

import { Command } from "commander";

import {
  buildDashboardSummary,
  buildInsights,
  buildMetricResult,
} from "@myfinance/analytics";
import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";
import { createMarketDataProvider } from "@myfinance/market-data";

const repository = createFinanceRepository();
const domain = new FinanceDomainService(repository);

type CommonOptions = {
  scope?: string;
  currency?: string;
  json?: boolean;
  period?: string;
};

async function resolveScope(scopeValue = "consolidated") {
  const dataset = await repository.getDataset();
  if (scopeValue === "consolidated") return { dataset, scope: { kind: "consolidated" as const } };
  if (scopeValue.startsWith("account:")) {
    return {
      dataset,
      scope: { kind: "account" as const, accountId: scopeValue.replace("account:", "") },
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

function getPeriod(options: CommonOptions) {
  return options.period === "ytd"
    ? { start: "2026-01-01", end: "2026-04-03", preset: "ytd" as const }
    : { start: "2026-04-01", end: "2026-04-03", preset: "mtd" as const };
}

const program = new Command();

program
  .name("myfinance")
  .description("Agent-facing CLI for MyFinanceApp")
  .version("0.1.0");

program
  .command("dashboard summary")
  .option("--scope <scope>", "consolidated, personal, company_a, company_b, or account:<id>")
  .option("--currency <currency>", "EUR or USD", "EUR")
  .option("--period <period>", "mtd or ytd", "mtd")
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const summary = buildDashboardSummary(dataset, {
      scope,
      displayCurrency: getDisplayCurrency(options),
      period: getPeriod(options),
    });
    render(summary, options.json);
  });

program
  .command("metrics get")
  .argument("<metricId>")
  .option("--scope <scope>", "consolidated, personal, company_a, company_b, or account:<id>")
  .option("--currency <currency>", "EUR or USD", "EUR")
  .option("--json", "Output JSON")
  .action(async (metricId: string, options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const metric = buildMetricResult(dataset, scope, getDisplayCurrency(options), metricId);
    render(metric, options.json);
  });

program
  .command("insights list")
  .option("--scope <scope>", "consolidated, personal, company_a, company_b, or account:<id>")
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { dataset, scope } = await resolveScope(options.scope);
    const insights = {
      schemaVersion: "v1",
      insights: buildInsights(dataset, scope),
      generatedAt: new Date().toISOString(),
    };
    render(insights, options.json);
  });

program
  .command("transactions list")
  .option("--scope <scope>", "consolidated, personal, company_a, company_b, or account:<id>")
  .option("--json", "Output JSON")
  .action(async (options: CommonOptions) => {
    const { scope } = await resolveScope(options.scope);
    const result = await domain.listTransactions(scope);
    render(result, options.json);
  });

program
  .command("transaction update")
  .argument("<transactionId>")
  .option("--class <transactionClass>")
  .option("--category <categoryCode>")
  .option("--entity <entitySlug>")
  .option("--security <securityId>")
  .option("--note <manualNote>")
  .option("--needs-review", "Mark the row as needs review")
  .option("--create-rule", "Also create a reusable rule")
  .option("--apply", "Persist the change")
  .option("--json", "Output JSON")
  .action(async (transactionId: string, options: CommonOptions & Record<string, string | boolean | undefined>) => {
    const dataset = await repository.getDataset();
    const entityId = typeof options.entity === "string"
      ? dataset.entities.find((row) => row.slug === options.entity)?.id
      : undefined;
    const result = await domain.updateTransaction({
      transactionId,
      patch: {
        transactionClass: typeof options.class === "string" ? (options.class as never) : undefined,
        categoryCode: typeof options.category === "string" ? options.category : undefined,
        economicEntityId: entityId,
        securityId: typeof options.security === "string" ? options.security : undefined,
        manualNotes: typeof options.note === "string" ? options.note : undefined,
        needsReview: options.needsReview ? true : undefined,
      },
      createRuleFromTransaction: Boolean(options.createRule),
      actorName: "cli",
      sourceChannel: "cli",
      apply: Boolean(options.apply),
    });
    render(result, options.json);
  });

const importsCommand = program.command("imports");

importsCommand
  .command("preview")
  .requiredOption("--account <accountId>")
  .requiredOption("--template <templateId>")
  .requiredOption("--file <filePath>", "Local file path passed to the pandas ingest wrapper")
  .option("--json", "Output JSON")
  .action(async (options: { account: string; template: string; file: string; json?: boolean }) => {
    const result = await domain.previewImport({
      accountId: options.account,
      templateId: options.template,
      filePath: options.file,
    });
    render(result, options.json);
  });

importsCommand
  .command("commit")
  .requiredOption("--account <accountId>")
  .requiredOption("--template <templateId>")
  .requiredOption("--file <filePath>", "Local file path passed to the pandas ingest wrapper")
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
  .option("--apply", "Persist the template")
  .option("--json", "Output JSON")
  .action(
    async (options: {
      name: string;
      institution: string;
      accountType: string;
      fileKind: "csv" | "xlsx";
      defaultCurrency: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const result = await domain.createTemplate({
        template: {
          userId: "00000000-0000-0000-0000-000000000001",
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
          columnMapJson: {
            transaction_date: "date",
            description_raw: "description",
            amount_original_signed: "amount",
          },
          signLogicJson: { mode: "signed_amount" },
          normalizationRulesJson: { trim_whitespace: true },
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
    async (options: {
      text: string;
      apply?: boolean;
      json?: boolean;
    }) => {
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
  .action(
    async (options: {
      job: string;
      apply?: boolean;
      json?: boolean;
    }) => {
      const result = await domain.applyRuleDraft({
        jobId: options.job,
        actorName: "cli",
        sourceChannel: "cli",
        apply: Boolean(options.apply),
      });
      render(result, options.json);
    },
  );

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
  .option("--scope <scope>", "consolidated, personal, company_a, company_b, or account:<id>")
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
      const entityId = dataset.entities.find((row) => row.slug === options.entity)?.id;
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

const pricesCommand = program.command("prices");

pricesCommand
  .command("refresh")
  .option("--symbol <symbol>")
  .option("--apply", "Persist the refreshed price downstream")
  .option("--json", "Output JSON")
  .action(async (options: { symbol?: string; apply?: boolean; json?: boolean }) => {
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
  });

const jobsCommand = program.command("jobs");

jobsCommand
  .command("run")
  .option("--apply", "Process queued jobs instead of previewing them")
  .option("--json", "Output JSON")
  .action(async (options: { apply?: boolean; json?: boolean }) => {
    render(await domain.runPendingJobs(Boolean(options.apply)), options.json);
  });

program.parseAsync(process.argv);
