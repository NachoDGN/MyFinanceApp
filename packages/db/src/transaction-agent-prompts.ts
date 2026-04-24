import { z } from "zod";

import type { DomainDataset } from "@myfinance/domain";

import type {
  TransactionAgentDecision,
  TransactionAgentSettings,
  TransactionAgentState,
  TransactionAgentToolName,
} from "./transaction-agent-types";

export const transactionAgentDecisionSchema = z.object({
  reasoningSummary: z.string().min(1),
  action: z.object({
    type: z.literal("tool"),
    toolName: z.enum([
      "answer",
      "hybrid_transaction_search",
      "sql_ledger_query",
      "get_transaction",
      "get_related_entries",
    ]),
    arguments: z.record(z.string(), z.unknown()).default({}),
  }),
}) satisfies z.ZodType<TransactionAgentDecision>;

export const transactionAgentDecisionJsonSchema = {
  type: "object",
  required: ["reasoningSummary", "action"],
  additionalProperties: false,
  properties: {
    reasoningSummary: { type: "string" },
    action: {
      type: "object",
      required: ["type", "toolName", "arguments"],
      additionalProperties: false,
      properties: {
        type: { type: "string", enum: ["tool"] },
        toolName: {
          type: "string",
          enum: [
            "answer",
            "hybrid_transaction_search",
            "sql_ledger_query",
            "get_transaction",
            "get_related_entries",
          ],
        },
        arguments: {
          type: "object",
          required: [
            "query",
            "limit",
            "transactionId",
            "answer",
            "answerType",
            "citationIds",
            "confidence",
            "insufficiencyReason",
          ],
          additionalProperties: false,
          properties: {
            query: { type: ["string", "null"] },
            limit: { type: ["number", "null"] },
            transactionId: { type: ["string", "null"] },
            answer: { type: ["string", "null"] },
            answerType: {
              type: ["string", "null"],
              enum: [
                "single_transaction",
                "aggregate",
                "comparison",
                "clarification",
                "no_result",
                null,
              ],
            },
            citationIds: {
              type: "array",
              items: { type: "string" },
            },
            confidence: { type: ["number", "null"] },
            insufficiencyReason: { type: ["string", "null"] },
          },
        },
      },
    },
  },
} as const;

function summarizeDataset(dataset: DomainDataset) {
  const aliasesBySecurityId = new Map<string, string[]>();
  for (const alias of dataset.securityAliases) {
    const aliases = aliasesBySecurityId.get(alias.securityId) ?? [];
    aliases.push(alias.aliasTextNormalized);
    aliasesBySecurityId.set(alias.securityId, aliases);
  }

  return [
    "Entities:",
    ...dataset.entities
      .filter((entity) => entity.active)
      .slice(0, 12)
      .map(
        (entity) =>
          `- ${entity.displayName} id=${entity.id} kind=${entity.entityKind} slug=${entity.slug}`,
      ),
    "Accounts:",
    ...dataset.accounts.slice(0, 24).map((account) => {
      const entity = dataset.entities.find(
        (candidate) => candidate.id === account.entityId,
      );
      return `- ${account.displayName} id=${account.id} institution=${account.institutionName} type=${account.accountType} entity=${entity?.displayName ?? account.entityId}`;
    }),
    "Securities:",
    ...dataset.securities
      .filter((security) => security.active)
      .slice(0, 40)
      .map((security) => {
        const aliases = (aliasesBySecurityId.get(security.id) ?? [])
          .slice(0, 4)
          .join(", ");
        return `- ${security.displaySymbol} canonical=${security.canonicalSymbol} name=${security.name} assetType=${security.assetType}${aliases ? ` aliases=${aliases}` : ""}`;
      }),
  ].join("\n");
}

export function buildTransactionAgentSystemPrompt() {
  return [
    "You are a ledger investigation agent for a private finance app.",
    "The system already ran one hybrid transaction search on the raw user question before your first decision.",
    "Treat the initial retrieval as starting evidence, not proof.",
    "You must call exactly one tool per step. Finalization is also a tool call through answer.",
    "Decompose questions before searching when they require totals, comparisons, latest/earliest selection, multiple parties, temporal stages, or synthesis.",
    "Use SQL for exact dates, amounts, totals, counts, ordering, and boolean ledger states.",
    "Use hybrid_transaction_search for fuzzy merchant names, business intent, loose descriptions, and retrieval over contextualized transaction text.",
    "Use get_transaction or get_related_entries when retrieved rows are suggestive but you need canonical proof.",
    "Never invent transactions, totals, dates, or invoice state. If evidence is insufficient, answer with the missing proof.",
    "Citations must be evidence IDs already produced by previous tool results.",
  ].join("\n");
}

export function buildTransactionAgentDecisionPrompt(input: {
  question: string;
  settings: TransactionAgentSettings;
  dataset: DomainDataset;
  displayCurrency: string;
  referenceDate: string;
  state: TransactionAgentState;
  completedSteps: number;
  toolNames: TransactionAgentToolName[];
}) {
  const initialRows = input.state.initialRetrieval?.rows ?? [];
  return [
    "Task:",
    `- User question: ${input.question}`,
    `- Current display currency: ${input.displayCurrency}`,
    `- Reference date: ${input.referenceDate}`,
    `- Current selected period: ${input.state.initialRetrieval?.filters.dateStart ?? "none"} to ${input.state.initialRetrieval?.filters.dateEnd ?? "none"}`,
    "",
    "Budget:",
    `- maxSteps=${input.settings.maxSteps}`,
    `- completedSteps=${input.completedSteps}`,
    "",
    "Available Tools:",
    input.toolNames.includes("answer")
      ? "- answer(answer, answerType, citationIds?, confidence?, insufficiencyReason?) -> final answer. Requires prior evidence unless insufficiencyReason is provided."
      : "",
    input.toolNames.includes("hybrid_transaction_search")
      ? "- hybrid_transaction_search(query, limit?) -> semantic+keyword transaction retrieval over contextualized ledger rows. Keep limit small, normally 3."
      : "",
    input.toolNames.includes("sql_ledger_query")
      ? "- sql_ledger_query(query) -> guarded read-only SQL over agent_ledger_* views only. Use for totals, counts, latest/earliest, exact filters, dates, amounts, and grouping."
      : "",
    input.toolNames.includes("get_transaction")
      ? "- get_transaction(transactionId) -> canonical transaction plus account/entity/category/import/search context."
      : "",
    input.toolNames.includes("get_related_entries")
      ? "- get_related_entries(transactionId) -> related transfer/correction/import-batch/audit context."
      : "",
    "",
    "Safe SQL Views:",
    "- agent_ledger_transactions",
    "- agent_ledger_search_rows",
    "- agent_ledger_accounts",
    "- agent_ledger_entities",
    "- agent_ledger_categories",
    "- agent_ledger_import_batches",
    "- agent_ledger_audit_events",
    "- agent_ledger_fx_rates",
    "",
    "Key agent_ledger_transactions Columns:",
    "- transaction_id, transaction_date, created_at",
    "- amount_original, currency_original, amount_base_eur, fx_rate_to_eur",
    "- merchant_normalized, counterparty_name, description_clean, description_raw",
    "- transaction_class, category_code, category_name, account_type, account_name",
    "- security_id, security_display_symbol, security_canonical_symbol, security_provider_symbol, security_name, quantity, unit_price_original",
    "- needs_review, classification_status, credit_card_statement_status, transfer_match_status",
    "",
    "Key agent_ledger_fx_rates Columns:",
    "- base_currency, quote_currency, as_of_date, as_of_timestamp, rate, source_name",
    "",
    "Ledger Conventions:",
    "- Spending-like rows usually have transaction_class in ('expense','fee','refund','loan_principal_payment','loan_interest_payment'); refunds reduce spend.",
    "- amount_base_eur is signed. For spending totals use absolute amount for expenses/fees/loan payments and subtract refunds.",
    "- Investment-buy totals use transaction_class = 'investment_trade_buy' and sum abs(amount_base_eur); do not count brokerage cash transfers as invested unless the user explicitly asks for transfers.",
    "- If the user says spend/spent on a security symbol or company in the Securities list, ordinary expense spend may be zero while investment buys are the relevant money out. Check investment_trade_buy before answering zero.",
    "- Dollars/USD means display the answer in USD. Convert EUR totals through agent_ledger_fx_rates or cite the EUR base amount if conversion evidence is unavailable.",
    "- Do arithmetic in SQL/tool calls. Do not mentally multiply, convert currencies, calculate totals, percentages, or differences in the final answer unless that exact numeric result appeared in prior tool evidence.",
    "- For latest/last questions, use transaction_date desc and then created_at desc.",
    "- Match people/vendors/securities through merchant_normalized, counterparty_name, description_clean, description_raw, contextualized search text, and acronym/ticker-style variants when the user supplies a short uppercase code such as AMD.",
    "",
    "Initial Hybrid Search Already Run:",
    initialRows.length === 0
      ? "- none"
      : initialRows
          .slice(0, 3)
          .map((row, index) => {
            const transaction = row.transaction;
            return [
              `- [${index + 1}] evidence=transaction:${transaction.id}`,
              `date=${transaction.transactionDate}`,
              `amount=${transaction.amountOriginal} ${transaction.currencyOriginal}`,
              `class=${transaction.transactionClass}`,
              `merchant=${transaction.merchantNormalized ?? "unknown"}`,
              `counterparty=${transaction.counterpartyName ?? "unknown"}`,
              `description=${transaction.descriptionRaw.slice(0, 180)}`,
            ].join(" | ");
          })
          .join("\n"),
    "",
    "Workspace Overview:",
    summarizeDataset(input.dataset),
    "",
    "Prior Search Queries:",
    input.state.priorSearchQueries.length === 0
      ? "- none"
      : input.state.priorSearchQueries.map((query) => `- ${query}`).join("\n"),
    "",
    "Latest Detailed Tool Observation:",
    input.state.transientObservation ?? "- none",
    "",
    "Prior Events:",
    input.state.priorEvents.length === 0
      ? "- none"
      : input.state.priorEvents.map((event) => `- ${event}`).join("\n"),
    "",
    "Current Evidence Ledger:",
    input.state.evidenceSummary.length === 0
      ? "- no evidence collected yet"
      : input.state.evidenceSummary.map((item) => `- ${item}`).join("\n"),
    "",
    "Decision Rules:",
    "- The JSON action.arguments object must always include query, limit, transactionId, answer, answerType, citationIds, confidence, and insufficiencyReason. Use null for unused scalar fields and [] for citationIds when unused.",
    "- Do not repeat identical or near-identical searches.",
    "- If the question asks for a total, count, yearly/monthly spend, latest, earliest, or ordered fact, prefer SQL before answering.",
    "- A zero-count SQL result from only ordinary spending classes is not enough to answer a security/company/ticker question. Broaden or switch classes and include known security aliases before finalizing.",
    "- If the answer requires currency conversion or another calculation, ask SQL for the final calculated value and cite that result.",
    "- If a candidate transaction seems likely but the question asks about proof or related context, call get_transaction or get_related_entries.",
    "- Call answer only when collected evidence covers every required part of the question.",
    "- Return exactly one JSON action.",
  ]
    .filter(Boolean)
    .join("\n");
}
