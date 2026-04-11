import { randomUUID } from "node:crypto";

import type {
  Account,
  AuditEvent,
  DomainDataset,
} from "@myfinance/domain";

import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import type { SqlClient } from "./sql-runtime";

export function normalizeCreditCardSettlementText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

function extractCreditCardContractSuffix(value: string) {
  const normalized = normalizeCreditCardSettlementText(value);
  const contractMatch = normalized.match(/CONTRATO\s+(\d{3,})/);
  if (contractMatch?.[1]) {
    return contractMatch[1];
  }

  const cardMatch = normalized.match(/TARJETAS?\s+DE\s+CREDITO.*?(\d{3,})/);
  return cardMatch?.[1] ?? null;
}

function findLinkedCreditCardAccount(input: {
  dataset: DomainDataset;
  settlementLinkedCreditCardAccountId?: string | null;
  settlementAccount: Pick<
    Account,
    "entityId" | "institutionName" | "defaultCurrency" | "staleAfterDays"
  >;
  settlementDescriptionRaw: string;
}) {
  if (input.settlementLinkedCreditCardAccountId) {
    const linkedAccount = input.dataset.accounts.find(
      (candidate) => candidate.id === input.settlementLinkedCreditCardAccountId,
    );
    if (linkedAccount) {
      return {
        linkedAccount,
        contractSuffix:
          linkedAccount.accountSuffix ??
          extractCreditCardContractSuffix(input.settlementDescriptionRaw),
      };
    }
  }

  const contractSuffix = extractCreditCardContractSuffix(
    input.settlementDescriptionRaw,
  );
  const candidateAccounts = input.dataset.accounts.filter(
    (candidate) =>
      candidate.accountType === "credit_card" &&
      candidate.isActive &&
      candidate.entityId === input.settlementAccount.entityId &&
      candidate.institutionName === input.settlementAccount.institutionName,
  );
  const linkedAccount =
    (contractSuffix
      ? candidateAccounts.find(
          (candidate) =>
            candidate.accountSuffix === contractSuffix ||
            candidate.matchingAliases.includes(contractSuffix),
        )
      : null) ?? (candidateAccounts.length === 1 ? candidateAccounts[0] : null);

  return { linkedAccount, contractSuffix };
}

export async function resolveOrCreateLinkedCreditCardAccount(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    settlementLinkedCreditCardAccountId?: string | null;
    settlementDescriptionRaw: string;
    settlementAccount: Pick<
      Account,
      | "entityId"
      | "institutionName"
      | "defaultCurrency"
      | "staleAfterDays"
    >;
    templateId: string;
    actorName: string;
    sourceChannel: AuditEvent["sourceChannel"];
  },
) {
  const template = input.dataset.templates.find(
    (candidate) => candidate.id === input.templateId,
  );
  if (!template) {
    throw new Error(`Template ${input.templateId} was not found.`);
  }
  if (template.compatibleAccountType !== "credit_card") {
    throw new Error(
      `Template ${input.templateId} is not compatible with credit-card statements.`,
    );
  }

  const { linkedAccount, contractSuffix } = findLinkedCreditCardAccount(input);
  if (linkedAccount) {
    return linkedAccount;
  }

  const accountId = randomUUID();
  const displayName = contractSuffix
    ? `${input.settlementAccount.institutionName} Credit Card ${contractSuffix}`
    : `${input.settlementAccount.institutionName} Credit Card`;
  const afterJson = {
    id: accountId,
    userId: input.userId,
    entityId: input.settlementAccount.entityId,
    institutionName: input.settlementAccount.institutionName,
    displayName,
    accountType: "credit_card",
    assetDomain: "cash",
    defaultCurrency: input.settlementAccount.defaultCurrency,
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: input.templateId,
    matchingAliases: contractSuffix ? [contractSuffix] : [],
    accountSuffix: contractSuffix,
    balanceMode: "computed",
    staleAfterDays: input.settlementAccount.staleAfterDays ?? null,
    lastImportedAt: null,
    createdAt: new Date().toISOString(),
  } satisfies Account;

  await sql`
    insert into public.accounts ${sql({
      id: accountId,
      user_id: input.userId,
      entity_id: input.settlementAccount.entityId,
      institution_name: input.settlementAccount.institutionName,
      display_name: afterJson.displayName,
      account_type: "credit_card",
      asset_domain: "cash",
      default_currency: input.settlementAccount.defaultCurrency,
      opening_balance_original: null,
      opening_balance_currency: null,
      opening_balance_date: null,
      include_in_consolidation: true,
      is_active: true,
      import_template_default_id: input.templateId,
      matching_aliases: contractSuffix ? [contractSuffix] : [],
      account_suffix: contractSuffix,
      balance_mode: "computed",
      stale_after_days: input.settlementAccount.staleAfterDays ?? null,
    } as Record<string, unknown>)}
  `;

  await insertAuditEventRecord(
    sql,
    createAuditEvent(
      input.sourceChannel,
      input.actorName,
      "accounts.create",
      "account",
      accountId,
      null,
      afterJson as unknown as Record<string, unknown>,
    ),
    "Auto-created linked credit-card account from a settlement-row statement upload.",
  );

  return afterJson;
}
