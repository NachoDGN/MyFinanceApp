export type PromptProfileId =
  | "cash_transaction_analyzer"
  | "investment_transaction_analyzer"
  | "spreadsheet_table_start"
  | "spreadsheet_layout"
  | "rule_draft_parser";

export type PromptSectionOverrides = Record<string, string>;
export type PromptProfileOverrides = Partial<
  Record<PromptProfileId, PromptSectionOverrides>
>;

export interface PromptSectionDefinition {
  id: string;
  label: string;
  description: string;
  defaultValue: string;
  requiredPlaceholders: string[];
}

export interface PromptProfileDefinition {
  id: PromptProfileId;
  title: string;
  description: string;
  editableSections: PromptSectionDefinition[];
}

export interface PromptProfileResolvedSection extends Omit<
  PromptSectionDefinition,
  "defaultValue"
> {
  value: string;
}

type PromptPreview = {
  systemPrompt: string;
  userPrompt: string;
};

type TransactionPromptExample = {
  transaction: string;
  initialInference: string;
  userFeedback: string;
  correctedOutcome: string;
};

type TransactionPromptReviewContext = {
  trigger: string;
  previousReviewReason: string;
  previousUserContext: string;
  userProvidedContext: string;
  previousLlmPayload: string;
  propagatedContexts: string;
  persistedSecurityMappings: string;
  resolvedSourcePrecedent: string;
};

type TransactionPromptBatchContext = {
  phase: string;
  sourceBatchKey: string;
  batchSummary: string;
  retrievalContext: string;
  totalTransactions: string;
  trustedResolvedCount: string;
};

const transactionPromptPlaceholders = [
  "institution_name",
  "account_display_name",
  "account_type",
  "account_id",
  "allowed_transaction_classes",
  "allowed_category_codes",
  "transaction_date",
  "posted_date",
  "amount_original",
  "currency_original",
  "description_raw",
  "merchant_normalized",
  "counterparty_name",
  "security_id",
  "quantity",
  "unit_price_original",
  "provider_context",
  "raw_payload",
  "deterministic_hint",
  "portfolio_state",
  "similar_account_history",
  "batch_context_section",
  "review_examples_section",
  "review_context_section",
] as const;

const reviewExamplePlaceholders = [
  "example_index",
  "transaction",
  "initial_inference",
  "user_feedback",
  "corrected_outcome",
] as const;

const reviewContextPlaceholders = [
  "review_trigger",
  "previous_review_reason",
  "previous_user_review_context",
  "new_user_review_context",
  "previous_llm_analysis",
  "propagated_contexts",
  "persisted_security_mappings",
  "resolved_source_precedent",
] as const;

const batchContextPlaceholders = [
  "batch_phase",
  "source_batch_key",
  "batch_summary",
  "retrieval_context",
  "total_transactions",
  "trusted_resolved_count",
] as const;

const spreadsheetTableStartPlaceholders = [
  "file_kind",
  "sheet_previews_block",
] as const;
const spreadsheetTableStartPreviewPlaceholders = [
  "sheet_index",
  "sheet_name",
  "preview_csv",
] as const;

const spreadsheetLayoutPlaceholders = [
  "file_kind",
  "sheet_name",
  "account_type",
  "default_currency",
  "reference_date",
  "canonical_fields",
  "detected_headers",
  "table_preview_csv",
] as const;

const ruleDraftPlaceholders = [
  "supported_condition_keys",
  "supported_output_keys",
  "allowed_transaction_classes",
  "allowed_category_codes",
  "entities",
  "accounts",
  "request_text",
] as const;

const promptProfiles: Record<PromptProfileId, PromptProfileDefinition> = {
  cash_transaction_analyzer: {
    id: "cash_transaction_analyzer",
    title: "Cash Transaction Analyzer",
    description:
      "Classifies bank and card transactions for cash accounts into the existing taxonomy.",
    editableSections: [
      {
        id: "system_prompt",
        label: "System Prompt",
        description:
          "Core classifier instructions. Keep it strict and JSON-only.",
        defaultValue: [
          "You classify cash and company account transactions into existing taxonomy codes only.",
          "Return one strict JSON object only.",
          "Use only the allowed transaction classes and category codes provided.",
          "Use null instead of guessing unsupported values.",
          "Keep the explanation to one short sentence.",
          "For cash accounts, keep economic_entity_override null unless the input explicitly proves the owning account entity is wrong; company names belong in counterparty_name, not economic_entity_override.",
          "If the transaction is fully resolved, populate resolution_process with a concise evidence chain. If it remains unresolved, set resolution_process to null.",
          "Do not claim that quantity or price was derived from a later rebuild step unless that derivation is explicitly present in the transaction input.",
          "Never invent merchants, counterparties, or categories.",
          "When similar same-account history is provided, use it as supporting precedent rather than a hard rule.",
          "When the review trigger is manual_resolved_review, treat it as a clean reanalysis of a previously resolved transaction and do not anchor on any prior inferred structured fields unless the current review context explicitly reintroduces them.",
        ].join(" "),
        requiredPlaceholders: [],
      },
      {
        id: "user_prompt_template",
        label: "User Prompt Template",
        description:
          "Transaction metadata and deterministic context. Keep every placeholder token.",
        defaultValue: [
          "Institution: {{institution_name}}.",
          "Account: {{account_display_name}}.",
          "Account type: {{account_type}}.",
          "Account id: {{account_id}}.",
          "Allowed transaction classes: {{allowed_transaction_classes}}.",
          "Allowed category codes: {{allowed_category_codes}}.",
          "Transaction date: {{transaction_date}}.",
          "Posted date: {{posted_date}}.",
          "Amount: {{amount_original}} {{currency_original}}.",
          "Description: {{description_raw}}.",
          "Existing merchant: {{merchant_normalized}}.",
          "Existing counterparty: {{counterparty_name}}.",
          "Security id: {{security_id}}.",
          "Quantity: {{quantity}}.",
          "Unit price: {{unit_price_original}}.",
          "Provider context: {{provider_context}}.",
          "For cash accounts, do not use investment classes unless the transaction data explicitly supports them.",
          "For cash accounts, keep the owning account's economic attribution fixed. If a personal account receives money from a company, classify the inflow within the allowed personal/system taxonomy and use counterparty_name for the company name.",
          "Current raw payload: {{raw_payload}}.",
          "Deterministic hint: {{deterministic_hint}}.",
          "Portfolio state: {{portfolio_state}}.",
          "Similar same-account resolved history: {{similar_account_history}}.",
          "{{batch_context_section}}",
          "{{review_examples_section}}",
          "{{review_context_section}}",
        ].join("\n"),
        requiredPlaceholders: [...transactionPromptPlaceholders],
      },
      {
        id: "batch_context_template",
        label: "Batch Context",
        description:
          "Shared batch snapshot context used during parallel import processing.",
        defaultValue: [
          "Batch processing phase: {{batch_phase}}.",
          "Batch source key: {{source_batch_key}}.",
          "Batch summary: {{batch_summary}}.",
          "Retriever context for this row: {{retrieval_context}}.",
          "Batch transaction count: {{total_transactions}}.",
          "Trusted resolved transactions already available: {{trusted_resolved_count}}.",
        ].join("\n"),
        requiredPlaceholders: [...batchContextPlaceholders],
      },
      {
        id: "review_examples_wrapper",
        label: "Review Examples Wrapper",
        description:
          "Heading and block container for previous user corrections.",
        defaultValue: [
          "Examples from prior user corrections:",
          "{{review_examples_block}}",
        ].join("\n"),
        requiredPlaceholders: ["review_examples_block"],
      },
      {
        id: "review_example_template",
        label: "Single Review Example",
        description:
          "How one prior user correction is shown inside the prompt.",
        defaultValue: [
          "Example {{example_index}} transaction metadata: {{transaction}}.",
          "Example {{example_index}} initial inference: {{initial_inference}}.",
          "Example {{example_index}} user feedback: {{user_feedback}}.",
          "Example {{example_index}} corrected outcome: {{corrected_outcome}}.",
        ].join("\n"),
        requiredPlaceholders: [...reviewExamplePlaceholders],
      },
      {
        id: "review_context_template",
        label: "Current Review Context",
        description:
          "Context passed when a user manually updates a transaction review.",
        defaultValue: [
          "Review trigger: {{review_trigger}}.",
          "Previous review reason: {{previous_review_reason}}.",
          "Previous user review context: {{previous_user_review_context}}.",
          "New user review context: {{new_user_review_context}}.",
          "Previous LLM analysis: {{previous_llm_analysis}}.",
          "Propagated contexts from similar unresolved transactions: {{propagated_contexts}}.",
          "Persisted confirmed security mappings: {{persisted_security_mappings}}.",
          "Resolved source precedent from a similar transaction: {{resolved_source_precedent}}.",
        ].join("\n"),
        requiredPlaceholders: [...reviewContextPlaceholders],
      },
    ],
  },
  investment_transaction_analyzer: {
    id: "investment_transaction_analyzer",
    title: "Investment Transaction Analyzer",
    description:
      "Classifies brokerage transactions with portfolio-awareness and security hints.",
    editableSections: [
      {
        id: "system_prompt",
        label: "System Prompt",
        description:
          "Core investment-classification instructions. Keep it strict and JSON-only.",
        defaultValue: [
          "You classify brokerage and investment account transactions with security-aware structured output.",
          "Return one strict JSON object only.",
          "Use only the allowed transaction classes and category codes provided.",
          "Use null instead of guessing unsupported values.",
          "Keep the explanation to one short sentence.",
          "Treat clearly named stocks, ETFs, index funds, and mutual funds as investment transactions even when ticker or quantity is missing.",
          "Use security_hint for the best normalized issuer or fund name visible in the description.",
          "If the instrument is recognizable but the exact catalog mapping is uncertain, still classify the transaction and explain the remaining ambiguity in reason.",
          "If the transaction is fully resolved, populate resolution_process with a concise but complete evidence chain describing how the exact instrument or event was resolved, what identifiers or sources were used, and why the match is exact. If the transaction remains unresolved, set resolution_process to null.",
          "Do not invent quantity or unit price. If those values are not directly stated or supported by exact trade-date evidence in the input or review context, leave them null so rebuild can derive them later.",
          "When the review context explicitly provides the trade-date NAV, trade price, or share count for this exact transaction, and it is internally consistent with the amount, currency, and trade direction, populate quantity and unit_price_original directly instead of deferring.",
          "Do not say quantity belongs to a later rebuild step when the user already supplied an exact transaction-specific NAV, trade price, or share count that makes the trade computable now.",
          "Never invent security ids or ticker symbols.",
          "When persisted security mappings from prior confirmed resolutions are provided, treat them as the strongest internal precedent, but only reuse them when the current transaction wording and context are consistent with that mapping.",
          "Use the latest portfolio snapshot when provided to sanity-check whether the row can realistically be a buy, sell, or fee.",
          "When similar same-account history is provided, use it as supporting precedent rather than a hard rule.",
          "When the review trigger is manual_resolved_review, treat it as a clean reanalysis of a previously resolved transaction and do not anchor on any prior inferred structured fields unless the current review context explicitly reintroduces them.",
          "Broker commissions can mention a security name and quantity without being a real disposal.",
          "If a positive row implies a per-share price that is far below the latest quote for a still-held security, classify it as fee instead of investment_trade_sell unless the row clearly states a real sale.",
          "You are a financial instrument identification expert. When you receive a partial asset name or description, do not provide a single best-guess ISIN or ticker unless the identification is totally clear.",
          "First decompose the instrument into issuer, benchmark index, and geographic region. Then identify the plausible vehicles, explicitly distinguishing ETFs from mutual funds.",
          "For each plausible vehicle, call out the variables that change the ISIN, including dividend treatment, legal domicile, and share class. If the description is still ambiguous, explain exactly what information is missing instead of making assumptions.",
          "Apply the following fund and ETF resolution workflow as an addition to, not a replacement for, the broader stock and investment-classification instructions above.",
          "You are a security-resolution and pricing agent for a personal finance application. Your task is to identify the exact fund or ETF referenced by an investment transaction and then retrieve its current price. The transaction text may be abbreviated, noisy, incomplete, broker-specific, or ambiguous. Your job is not to find any fund that tracks the named index. Your job is to determine which exact tradable instrument the transaction most likely refers to, or to state clearly that the evidence is insufficient.",
          "Treat the following as distinct and non-interchangeable: issuer family, benchmark or index name, fund name, fund share class, ETF listing, domicile, currency, exchange, and legal wrapper. Many different products can track the same index, and one issuer can offer multiple funds with extremely similar names. Never map a transaction to a security based on index wording alone. Never return a price for a guessed or ambiguous match.",
          "Use every clue available in the input, not just the description string. Consider the transaction description, trade date, settlement date, transaction amount, trade currency, unit count, implied execution price, commissions, taxes, FX conversion, broker or platform name, user country, account type, tax wrapper, nearby transactions, dividend lines, reinvestment lines, fee lines, recurring purchase patterns, exchange suffixes, and any fragments of ISIN, CUSIP, SEDOL, ticker, or official fund name. Expand plausible abbreviations during search, such as GLOB to GLOBAL, SM CAP to SMALL CAP, IDX to INDEX, ACC to ACCUMULATING, INC to INCOME or DISTRIBUTING, and UCITS/OEIC/ETF variants where relevant.",
          "When units and transaction amount are available, compute an implied trade price and use it as a validation signal. Allow reasonable tolerance for commissions, taxes, FX effects, and rounding. Also verify that the candidate fund, share class, and listing existed on the trade date. Historical trade price is only a validation signal. It must never be used as a substitute for the current price.",
          "If any exact identifier such as ISIN, CUSIP, or SEDOL appears anywhere in the transaction, prior analysis, or user review context, search that identifier directly first and treat it as the highest-priority evidence for resolution.",
          "Once an exact ISIN is known for a mutual fund or index fund, use it to lock identity from issuer-originated pages that explicitly reference that ISIN. Do not spend web-search effort trying to recover the trade-date NAV for that fund; the application resolves historical NAV from internal stored price history once the exact ISIN is known.",
          "When you have an exact or near-exact resolution, populate the structured fields explicitly instead of leaving them only in explanation or reason. Put the exact fund or ETF name in resolved_instrument_name, the identifier in resolved_instrument_isin or the relevant ticker and exchange fields, and the retrieved quote or NAV in current_price, current_price_currency, current_price_timestamp, current_price_source, and current_price_type whenever those values are known.",
          "For mutual funds and non-exchange-traded index funds, use an explicit two-step workflow: first resolve identity, then stop at the exact ISIN and share class. When an exact ISIN is present, treat it as definitive for identity resolution and query issuer-originated pages, factsheets, product pages, KIIDs/KIDs, and regulator-grade listings that explicitly mention that ISIN to lock the exact fund and share class. Leave quantity and unit_price_original null unless the input or review context already provides exact trade-date NAV or share count. You may return a latest published NAV in the current_price fields when confidently available, but do not search the web for the transaction-date NAV because rebuild derives that from internal price history.",
          "When searching the internet, follow a disciplined sequence. Start with the exact raw description in quotes. Then search a normalized form of the description. Then search combinations of issuer name, index phrase, and likely security terms such as ETF, fund, mutual fund, index fund, tracker, OEIC, UCITS, Acc, Inc, distributing, accumulating, Class A, Class C, institutional, investor, Admiral, and exchange-specific tickers. Prefer official issuer pages, prospectuses, factsheets, KIIDs/KIDs, exchange listings, and regulator filings. Use secondary market-data sites only as cross-checks or when official sources do not provide the needed quote. Ignore low-quality pages, discussion forums, and pages that only repeat index wording without identifying a specific tradable product.",
          'Be especially careful with transactions that mention only a brand plus an index phrase, such as "VANGUARD GLOB SMALL CAP INDEX." That kind of text often narrows the strategy but not the exact product. In those cases, search for all official funds under that issuer that plausibly match the phrase, then eliminate candidates using share class, domicile, currency, listing, trade-date existence, implied trade price, broker availability, and jurisdiction. Do not collapse multiple plausible funds into a single answer without disambiguating evidence.',
          "For cross-listed ETFs, remember that one fund may have multiple tickers across multiple exchanges and currencies. Use ISIN to identify the underlying instrument, then use ticker plus exchange to identify the correct listing for pricing. Select the listing most consistent with transaction currency, broker region, and user jurisdiction. Do not price a USD listing when the transaction strongly suggests a GBP or EUR listing unless the evidence clearly shows a foreign-currency trade.",
          "Only call a match exact when the transaction can be tied to a specific tradable instrument with strong evidence. Strong evidence means either one unique identifier, such as ISIN, CUSIP, SEDOL, or ticker plus exchange, or at least two independent non-unique clues that converge on the same security, such as issuer plus exact official fund name, or issuer plus index phrase plus matching share class and currency, or issuer plus index phrase plus trade-date price consistency. If you cannot isolate a single exact instrument, do not guess. Mark the result as ambiguous or probable and return the best candidates with reasons.",
          "After, and only after, you have identified the exact instrument, retrieve its current price. For exchange-traded funds, return the most recent market quote and specify whether it is live, delayed, or last close. For mutual funds and non-exchange-traded index funds, return the latest published NAV, not an ETF-style market quote. Always return the quote currency, timestamp, pricing source, and price type. If the match is not exact, set current_price to null. If the exact instrument is known but a reliable current quote or NAV cannot be retrieved from a credible source, set current_price to null rather than estimating.",
          "Never do any of the following: infer an exact fund from index wording alone; pretend the base fund name is sufficient when the share class is unresolved; return the price of a related fund that tracks the same index; return the price of a different exchange listing without explaining the listing choice; reuse a historical trade price as the current price; or upgrade a weak match into an exact match because it feels plausible.",
        ].join(" "),
        requiredPlaceholders: [],
      },
      {
        id: "user_prompt_template",
        label: "User Prompt Template",
        description:
          "Transaction metadata, deterministic hints, and portfolio state. Keep every placeholder token.",
        defaultValue: [
          "Institution: {{institution_name}}.",
          "Account: {{account_display_name}}.",
          "Account type: {{account_type}}.",
          "Account id: {{account_id}}.",
          "Allowed transaction classes: {{allowed_transaction_classes}}.",
          "Allowed category codes: {{allowed_category_codes}}.",
          "Transaction date: {{transaction_date}}.",
          "Posted date: {{posted_date}}.",
          "Amount: {{amount_original}} {{currency_original}}.",
          "Description: {{description_raw}}.",
          "Existing merchant: {{merchant_normalized}}.",
          "Existing counterparty: {{counterparty_name}}.",
          "Security id: {{security_id}}.",
          "Quantity: {{quantity}}.",
          "Unit price: {{unit_price_original}}.",
          "Provider context: {{provider_context}}.",
          "For investment accounts, prefer investment_trade_buy or investment_trade_sell when a company, fund, ETF, or index instrument is clearly named. Use transfer_internal for broker cash movements between owned accounts and leave statement-period rows as unknown.",
          "Current raw payload: {{raw_payload}}.",
          "Deterministic hint: {{deterministic_hint}}.",
          "Portfolio state: {{portfolio_state}}.",
          "Similar same-account resolved history: {{similar_account_history}}.",
          "{{batch_context_section}}",
          "{{review_examples_section}}",
          "{{review_context_section}}",
        ].join("\n"),
        requiredPlaceholders: [...transactionPromptPlaceholders],
      },
      {
        id: "batch_context_template",
        label: "Batch Context",
        description:
          "Shared batch snapshot context used during parallel import processing.",
        defaultValue: [
          "Batch processing phase: {{batch_phase}}.",
          "Batch source key: {{source_batch_key}}.",
          "Batch summary: {{batch_summary}}.",
          "Retriever context for this row: {{retrieval_context}}.",
          "Batch transaction count: {{total_transactions}}.",
          "Trusted resolved transactions already available: {{trusted_resolved_count}}.",
        ].join("\n"),
        requiredPlaceholders: [...batchContextPlaceholders],
      },
      {
        id: "review_examples_wrapper",
        label: "Review Examples Wrapper",
        description:
          "Heading and block container for previous user corrections.",
        defaultValue: [
          "Examples from prior user corrections:",
          "{{review_examples_block}}",
        ].join("\n"),
        requiredPlaceholders: ["review_examples_block"],
      },
      {
        id: "review_example_template",
        label: "Single Review Example",
        description:
          "How one prior user correction is shown inside the prompt.",
        defaultValue: [
          "Example {{example_index}} transaction metadata: {{transaction}}.",
          "Example {{example_index}} initial inference: {{initial_inference}}.",
          "Example {{example_index}} user feedback: {{user_feedback}}.",
          "Example {{example_index}} corrected outcome: {{corrected_outcome}}.",
        ].join("\n"),
        requiredPlaceholders: [...reviewExamplePlaceholders],
      },
      {
        id: "review_context_template",
        label: "Current Review Context",
        description:
          "Context passed when a user manually updates a transaction review.",
        defaultValue: [
          "Review trigger: {{review_trigger}}.",
          "Previous review reason: {{previous_review_reason}}.",
          "Previous user review context: {{previous_user_review_context}}.",
          "New user review context: {{new_user_review_context}}.",
          "Previous LLM analysis: {{previous_llm_analysis}}.",
          "Propagated contexts from similar unresolved transactions: {{propagated_contexts}}.",
          "Persisted confirmed security mappings: {{persisted_security_mappings}}.",
          "Resolved source precedent from a similar transaction: {{resolved_source_precedent}}.",
        ].join("\n"),
        requiredPlaceholders: [...reviewContextPlaceholders],
      },
    ],
  },
  spreadsheet_table_start: {
    id: "spreadsheet_table_start",
    title: "Spreadsheet Table Start Inference",
    description:
      "Finds the transaction table within workbook previews before column mapping runs.",
    editableSections: [
      {
        id: "system_prompt",
        label: "System Prompt",
        description:
          "Instructions for identifying the start of the transaction table.",
        defaultValue: [
          "Locate the transaction table within a spreadsheet preview.",
          "Return one strict JSON object only.",
          "Each preview includes row numbers and Excel-style column letters.",
          "Identify the header row and the left-most column of the transaction table.",
          "Prefer the sheet that clearly contains transaction rows rather than cover pages or summaries.",
          "For XLSX files, sheet_name must exactly match one of the provided sheet labels. Do not invent, translate, or paraphrase sheet names.",
          "Always include sheet_name. Use null for CSV files or when uncertain.",
        ].join(" "),
        requiredPlaceholders: [],
      },
      {
        id: "user_prompt_template",
        label: "User Prompt Template",
        description: "Workbook preview wrapper. Keep every placeholder token.",
        defaultValue: [
          "File kind: {{file_kind}}.",
          "Workbook previews:",
          "{{sheet_previews_block}}",
        ].join("\n\n"),
        requiredPlaceholders: [...spreadsheetTableStartPlaceholders],
      },
      {
        id: "sheet_preview_template",
        label: "Sheet Preview Template",
        description: "How each sheet preview is rendered inside the prompt.",
        defaultValue: [
          "Sheet {{sheet_index}}: {{sheet_name}}",
          "{{preview_csv}}",
        ].join("\n"),
        requiredPlaceholders: [...spreadsheetTableStartPreviewPlaceholders],
      },
    ],
  },
  spreadsheet_layout: {
    id: "spreadsheet_layout",
    title: "Spreadsheet Layout Inference",
    description:
      "Infers canonical field mappings and sign logic for import tables.",
    editableSections: [
      {
        id: "system_prompt",
        label: "System Prompt",
        description:
          "Instructions for mapping headers and sign logic from a table preview.",
        defaultValue: [
          "Infer the canonical column mapping and sign logic for a bank-import table.",
          "Return one strict JSON object only.",
          "Only map headers that are clearly present in the preview.",
          "Use only the exact source headers shown in the preview.",
          "Choose one sign logic mode and fill only the fields needed for that mode.",
          "If debits and credits are already signed in one column, use signed_amount.",
          "Always include every field in column_map and sign_logic. Use null when a field does not apply.",
          "If the date format is ambiguous, prefer the interpretation that stays consistent with the sheet and does not create impossible future transaction dates relative to the reference date.",
        ].join(" "),
        requiredPlaceholders: [],
      },
      {
        id: "user_prompt_template",
        label: "User Prompt Template",
        description: "Table-preview wrapper. Keep every placeholder token.",
        defaultValue: [
          "File kind: {{file_kind}}.",
          "Sheet name: {{sheet_name}}.",
          "Account type: {{account_type}}.",
          "Default currency if no currency column exists: {{default_currency}}.",
          "Reference date: {{reference_date}}.",
          "Canonical fields: {{canonical_fields}}.",
          "Detected headers: {{detected_headers}}.",
          "Table preview CSV:",
          "{{table_preview_csv}}",
        ].join("\n"),
        requiredPlaceholders: [...spreadsheetLayoutPlaceholders],
      },
    ],
  },
  rule_draft_parser: {
    id: "rule_draft_parser",
    title: "Rule Draft Parser",
    description:
      "Converts natural-language rule requests into deterministic rule JSON.",
    editableSections: [
      {
        id: "system_prompt",
        label: "System Prompt",
        description:
          "Core instructions for translating user rule requests into supported rule fields.",
        defaultValue: [
          "Convert the user's natural-language rule request into deterministic transaction rule logic.",
          "Return one strict JSON object only.",
          "Use only the supported condition keys and output keys provided.",
          "Do not invent taxonomy codes, entity ids, account ids, or transaction classes.",
          "If the request is ambiguous, make the narrowest safe rule and lower confidence.",
        ].join(" "),
        requiredPlaceholders: [],
      },
      {
        id: "user_prompt_template",
        label: "User Prompt Template",
        description: "Rule-parser context. Keep every placeholder token.",
        defaultValue: [
          "Supported condition keys: {{supported_condition_keys}}",
          "Supported output keys: {{supported_output_keys}}",
          "Allowed transaction classes: {{allowed_transaction_classes}}",
          "Allowed category codes: {{allowed_category_codes}}",
          "Entities: {{entities}}",
          "Accounts: {{accounts}}",
          "User request: {{request_text}}",
        ].join("\n"),
        requiredPlaceholders: [...ruleDraftPlaceholders],
      },
    ],
  },
};

function getDefinition(promptId: PromptProfileId) {
  return promptProfiles[promptId];
}

function interpolateTemplate(
  template: string,
  variables: Record<string, string>,
) {
  return template.replace(/\{\{([a-z0-9_]+)\}\}/gi, (_, key: string) => {
    return variables[key] ?? "";
  });
}

function resolveSections(
  promptId: PromptProfileId,
  overrides?: Record<string, unknown> | null,
) {
  const definition = getDefinition(promptId);
  const overrideRecord =
    overrides && typeof overrides === "object" && !Array.isArray(overrides)
      ? (overrides as Record<string, unknown>)
      : {};

  return Object.fromEntries(
    definition.editableSections.map((section) => {
      const override = overrideRecord[section.id];
      let value =
        typeof override === "string" && override.trim()
          ? override.trim()
          : section.defaultValue;
      try {
        validateRequiredPlaceholders(section, value);
      } catch {
        value = section.defaultValue;
      }
      return [section.id, value];
    }),
  ) as Record<string, string>;
}

function validateRequiredPlaceholders(
  section: PromptSectionDefinition,
  value: string,
) {
  for (const placeholder of section.requiredPlaceholders) {
    const token = `{{${placeholder}}}`;
    if (!value.includes(token)) {
      throw new Error(`${section.label} must keep the placeholder ${token}.`);
    }
  }
}

function renderTransactionPrompt(
  promptId: "cash_transaction_analyzer" | "investment_transaction_analyzer",
  overrides: Record<string, unknown> | null | undefined,
  variables: Record<string, string>,
  batchContext: TransactionPromptBatchContext | null,
  reviewExamples: TransactionPromptExample[],
  reviewContext: TransactionPromptReviewContext | null,
): PromptPreview {
  const sections = resolveSections(promptId, overrides);
  const batchContextBlock = batchContext
    ? interpolateTemplate(sections.batch_context_template, {
        batch_phase: batchContext.phase,
        source_batch_key: batchContext.sourceBatchKey,
        batch_summary: batchContext.batchSummary,
        retrieval_context: batchContext.retrievalContext,
        total_transactions: batchContext.totalTransactions,
        trusted_resolved_count: batchContext.trustedResolvedCount,
      })
    : "";
  const reviewExamplesBlock =
    reviewExamples.length > 0
      ? interpolateTemplate(sections.review_examples_wrapper, {
          review_examples_block: reviewExamples
            .map((example, index) =>
              interpolateTemplate(sections.review_example_template, {
                example_index: String(index + 1),
                transaction: example.transaction,
                initial_inference: example.initialInference,
                user_feedback: example.userFeedback,
                corrected_outcome: example.correctedOutcome,
              }),
            )
            .join("\n"),
        })
      : "";
  const reviewContextBlock = reviewContext
    ? interpolateTemplate(sections.review_context_template, {
        review_trigger: reviewContext.trigger,
        previous_review_reason: reviewContext.previousReviewReason,
        previous_user_review_context: reviewContext.previousUserContext,
        new_user_review_context: reviewContext.userProvidedContext,
        previous_llm_analysis: reviewContext.previousLlmPayload,
        propagated_contexts: reviewContext.propagatedContexts,
        persisted_security_mappings: reviewContext.persistedSecurityMappings,
        resolved_source_precedent: reviewContext.resolvedSourcePrecedent,
      })
    : "";

  return {
    systemPrompt: sections.system_prompt,
    userPrompt: interpolateTemplate(sections.user_prompt_template, {
      ...variables,
      batch_context_section: batchContextBlock,
      review_examples_section: reviewExamplesBlock,
      review_context_section: reviewContextBlock,
    }),
  };
}

function renderSpreadsheetTableStartPrompt(
  overrides: Record<string, unknown> | null | undefined,
  variables: {
    fileKind: string;
    sheetPreviews: Array<{ sheetName: string; previewCsv: string }>;
  },
): PromptPreview {
  const sections = resolveSections("spreadsheet_table_start", overrides);
  return {
    systemPrompt: sections.system_prompt,
    userPrompt: interpolateTemplate(sections.user_prompt_template, {
      file_kind: variables.fileKind,
      sheet_previews_block: variables.sheetPreviews
        .map((preview, index) =>
          interpolateTemplate(sections.sheet_preview_template, {
            sheet_index: String(index + 1),
            sheet_name: preview.sheetName,
            preview_csv: preview.previewCsv,
          }),
        )
        .join("\n\n"),
    }),
  };
}

function renderSpreadsheetLayoutPrompt(
  overrides: Record<string, unknown> | null | undefined,
  variables: Record<string, string>,
): PromptPreview {
  const sections = resolveSections("spreadsheet_layout", overrides);
  return {
    systemPrompt: sections.system_prompt,
    userPrompt: interpolateTemplate(sections.user_prompt_template, variables),
  };
}

function renderRuleDraftParserPrompt(
  overrides: Record<string, unknown> | null | undefined,
  variables: Record<string, string>,
): PromptPreview {
  const sections = resolveSections("rule_draft_parser", overrides);
  return {
    systemPrompt: sections.system_prompt,
    userPrompt: interpolateTemplate(sections.user_prompt_template, variables),
  };
}

export function listPromptProfileDefinitions() {
  return Object.values(promptProfiles);
}

export function getPromptProfileDefinition(promptId: PromptProfileId) {
  return getDefinition(promptId);
}

export function resolvePromptProfileSections(
  promptId: PromptProfileId,
  overrides?: Record<string, unknown> | null,
): PromptProfileResolvedSection[] {
  const definition = getDefinition(promptId);
  const resolved = resolveSections(promptId, overrides);
  return definition.editableSections.map((section) => ({
    id: section.id,
    label: section.label,
    description: section.description,
    requiredPlaceholders: section.requiredPlaceholders,
    value: resolved[section.id],
  }));
}

export function sanitizePromptProfileSectionOverrides(
  promptId: PromptProfileId,
  overrides: Record<string, unknown>,
) {
  const definition = getDefinition(promptId);
  const sanitized: PromptSectionOverrides = {};

  for (const section of definition.editableSections) {
    const raw = overrides[section.id];
    if (typeof raw !== "string" || !raw.trim()) {
      throw new Error(`${section.label} cannot be empty.`);
    }

    const value = raw.trim();
    validateRequiredPlaceholders(section, value);
    sanitized[section.id] = value;
  }

  return sanitized;
}

export function buildPromptProfilePreview(
  promptId: PromptProfileId,
  overrides?: Record<string, unknown> | null,
): PromptPreview {
  switch (promptId) {
    case "cash_transaction_analyzer":
      return renderTransactionPrompt(
        promptId,
        overrides,
        {
          institution_name: "{{institution_name}}",
          account_display_name: "{{account_display_name}}",
          account_type: "{{account_type}}",
          account_id: "{{account_id}}",
          allowed_transaction_classes: "{{allowed_transaction_classes}}",
          allowed_category_codes: "{{allowed_category_codes}}",
          transaction_date: "{{transaction_date}}",
          posted_date: "{{posted_date}}",
          amount_original: "{{amount_original}}",
          currency_original: "{{currency_original}}",
          description_raw: "{{description_raw}}",
          merchant_normalized: "{{merchant_normalized}}",
          counterparty_name: "{{counterparty_name}}",
          security_id: "{{security_id}}",
          quantity: "{{quantity}}",
          unit_price_original: "{{unit_price_original}}",
          provider_context: "{{provider_context}}",
          raw_payload: "{{raw_payload}}",
          deterministic_hint: "{{deterministic_hint}}",
          portfolio_state: "{{portfolio_state}}",
          similar_account_history: "{{similar_account_history}}",
        },
        {
          phase: "{{batch_phase}}",
          sourceBatchKey: "{{source_batch_key}}",
          batchSummary: "{{batch_summary}}",
          retrievalContext: "{{retrieval_context}}",
          totalTransactions: "{{total_transactions}}",
          trustedResolvedCount: "{{trusted_resolved_count}}",
        },
        [
          {
            transaction: "{{example_transaction_metadata}}",
            initialInference: "{{example_initial_inference}}",
            userFeedback: "{{example_user_feedback}}",
            correctedOutcome: "{{example_corrected_outcome}}",
          },
        ],
        {
          trigger: "{{review_trigger}}",
          previousReviewReason: "{{previous_review_reason}}",
          previousUserContext: "{{previous_user_review_context}}",
          userProvidedContext: "{{new_user_review_context}}",
          previousLlmPayload: "{{previous_llm_analysis}}",
          propagatedContexts: "{{propagated_contexts}}",
          persistedSecurityMappings: "{{persisted_security_mappings}}",
          resolvedSourcePrecedent: "{{resolved_source_precedent}}",
        },
      );
    case "investment_transaction_analyzer":
      return renderTransactionPrompt(
        promptId,
        overrides,
        {
          institution_name: "{{institution_name}}",
          account_display_name: "{{account_display_name}}",
          account_type: "{{account_type}}",
          account_id: "{{account_id}}",
          allowed_transaction_classes: "{{allowed_transaction_classes}}",
          allowed_category_codes: "{{allowed_category_codes}}",
          transaction_date: "{{transaction_date}}",
          posted_date: "{{posted_date}}",
          amount_original: "{{amount_original}}",
          currency_original: "{{currency_original}}",
          description_raw: "{{description_raw}}",
          merchant_normalized: "{{merchant_normalized}}",
          counterparty_name: "{{counterparty_name}}",
          security_id: "{{security_id}}",
          quantity: "{{quantity}}",
          unit_price_original: "{{unit_price_original}}",
          provider_context: "{{provider_context}}",
          raw_payload: "{{raw_payload}}",
          deterministic_hint: "{{deterministic_hint}}",
          portfolio_state: "{{portfolio_state}}",
          similar_account_history: "{{similar_account_history}}",
        },
        {
          phase: "{{batch_phase}}",
          sourceBatchKey: "{{source_batch_key}}",
          batchSummary: "{{batch_summary}}",
          retrievalContext: "{{retrieval_context}}",
          totalTransactions: "{{total_transactions}}",
          trustedResolvedCount: "{{trusted_resolved_count}}",
        },
        [
          {
            transaction: "{{example_transaction_metadata}}",
            initialInference: "{{example_initial_inference}}",
            userFeedback: "{{example_user_feedback}}",
            correctedOutcome: "{{example_corrected_outcome}}",
          },
        ],
        {
          trigger: "{{review_trigger}}",
          previousReviewReason: "{{previous_review_reason}}",
          previousUserContext: "{{previous_user_review_context}}",
          userProvidedContext: "{{new_user_review_context}}",
          previousLlmPayload: "{{previous_llm_analysis}}",
          propagatedContexts: "{{propagated_contexts}}",
          persistedSecurityMappings: "{{persisted_security_mappings}}",
          resolvedSourcePrecedent: "{{resolved_source_precedent}}",
        },
      );
    case "spreadsheet_table_start":
      return renderSpreadsheetTableStartPrompt(overrides, {
        fileKind: "{{file_kind}}",
        sheetPreviews: [
          {
            sheetName: "{{sheet_name}}",
            previewCsv: "{{preview_csv}}",
          },
        ],
      });
    case "spreadsheet_layout":
      return renderSpreadsheetLayoutPrompt(overrides, {
        file_kind: "{{file_kind}}",
        sheet_name: "{{sheet_name}}",
        account_type: "{{account_type}}",
        default_currency: "{{default_currency}}",
        reference_date: "{{reference_date}}",
        canonical_fields: "{{canonical_fields}}",
        detected_headers: "{{detected_headers}}",
        table_preview_csv: "{{table_preview_csv}}",
      });
    case "rule_draft_parser":
      return renderRuleDraftParserPrompt(overrides, {
        supported_condition_keys: "{{supported_condition_keys}}",
        supported_output_keys: "{{supported_output_keys}}",
        allowed_transaction_classes: "{{allowed_transaction_classes}}",
        allowed_category_codes: "{{allowed_category_codes}}",
        entities: "{{entities}}",
        accounts: "{{accounts}}",
        request_text: "{{request_text}}",
      });
  }
}

export function renderTransactionAnalyzerPrompt(
  assetDomain: "cash" | "investment",
  input: {
    institutionName: string;
    accountDisplayName: string;
    accountType: string;
    accountId: string;
    allowedTransactionClasses: string;
    allowedCategoryCodes: string;
    transactionDate: string;
    postedDate: string;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    merchantNormalized: string;
    counterpartyName: string;
    securityId: string;
    quantity: string;
    unitPriceOriginal: string;
    providerContext: string;
    rawPayload: string;
    deterministicHint: string;
    portfolioState: string;
    similarAccountHistory: string;
    reviewExamples: TransactionPromptExample[];
    batchContext: TransactionPromptBatchContext | null;
    reviewContext: TransactionPromptReviewContext | null;
    promptOverrides?: Record<string, unknown> | null;
  },
) {
  return renderTransactionPrompt(
    assetDomain === "investment"
      ? "investment_transaction_analyzer"
      : "cash_transaction_analyzer",
    input.promptOverrides,
    {
      institution_name: input.institutionName,
      account_display_name: input.accountDisplayName,
      account_type: input.accountType,
      account_id: input.accountId,
      allowed_transaction_classes: input.allowedTransactionClasses,
      allowed_category_codes: input.allowedCategoryCodes,
      transaction_date: input.transactionDate,
      posted_date: input.postedDate,
      amount_original: input.amountOriginal,
      currency_original: input.currencyOriginal,
      description_raw: input.descriptionRaw,
      merchant_normalized: input.merchantNormalized,
      counterparty_name: input.counterpartyName,
      security_id: input.securityId,
      quantity: input.quantity,
      unit_price_original: input.unitPriceOriginal,
      provider_context: input.providerContext,
      raw_payload: input.rawPayload,
      deterministic_hint: input.deterministicHint,
      portfolio_state: input.portfolioState,
      similar_account_history: input.similarAccountHistory,
      batch_context_section: "",
      review_examples_section: "",
      review_context_section: "",
    },
    input.batchContext,
    input.reviewExamples,
    input.reviewContext,
  );
}

export function renderSpreadsheetTableStartPromptFromInput(input: {
  fileKind: string;
  sheetPreviews: Array<{ sheetName: string; previewCsv: string }>;
  promptOverrides?: Record<string, unknown> | null;
}) {
  return renderSpreadsheetTableStartPrompt(input.promptOverrides, {
    fileKind: input.fileKind,
    sheetPreviews: input.sheetPreviews.map((preview) => ({
      sheetName: preview.sheetName,
      previewCsv: preview.previewCsv,
    })),
  });
}

export function renderSpreadsheetLayoutPromptFromInput(input: {
  fileKind: string;
  sheetName: string;
  accountType: string;
  defaultCurrency: string;
  referenceDate: string;
  canonicalFields: string;
  detectedHeaders: string;
  tablePreviewCsv: string;
  promptOverrides?: Record<string, unknown> | null;
}) {
  return renderSpreadsheetLayoutPrompt(input.promptOverrides, {
    file_kind: input.fileKind,
    sheet_name: input.sheetName,
    account_type: input.accountType,
    default_currency: input.defaultCurrency,
    reference_date: input.referenceDate,
    canonical_fields: input.canonicalFields,
    detected_headers: input.detectedHeaders,
    table_preview_csv: input.tablePreviewCsv,
  });
}

export function renderRuleDraftParserPromptFromInput(input: {
  supportedConditionKeys: string;
  supportedOutputKeys: string;
  allowedTransactionClasses: string;
  allowedCategoryCodes: string;
  entities: string;
  accounts: string;
  requestText: string;
  promptOverrides?: Record<string, unknown> | null;
}) {
  return renderRuleDraftParserPrompt(input.promptOverrides, {
    supported_condition_keys: input.supportedConditionKeys,
    supported_output_keys: input.supportedOutputKeys,
    allowed_transaction_classes: input.allowedTransactionClasses,
    allowed_category_codes: input.allowedCategoryCodes,
    entities: input.entities,
    accounts: input.accounts,
    request_text: input.requestText,
  });
}
