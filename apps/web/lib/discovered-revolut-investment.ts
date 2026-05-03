import { Decimal } from "decimal.js";

import {
  buildManualInvestmentMatchHaystack,
  parseManualInvestmentMatcherTerms,
  type Account,
  type DomainDataset,
  type ManualInvestment,
  type ManualInvestmentValuation,
  type Transaction,
} from "@myfinance/domain";

export const REVOLUT_LOW_RISK_FUND_LABEL = "Revolut Inversión riesgo bajo";
export const REVOLUT_LOW_RISK_FUND_MATCHER_TEXT =
  "Inversión riesgo bajo, SP:b9611010-6e70-49bd-b353-0f959610715d";
export const REVOLUT_LOW_RISK_FUND_NOTE =
  "Auto-discovered from Revolut internal low-risk fund transfers.";

const VIRTUAL_MANUAL_INVESTMENT_ID = "00000000-0000-0000-0000-00000000f001";
const VIRTUAL_VALUATION_ID = "00000000-0000-0000-0000-00000000f002";
const MATCHER_TERMS = parseManualInvestmentMatcherTerms(
  REVOLUT_LOW_RISK_FUND_MATCHER_TEXT,
);

type FundingSummary = {
  count: number;
  principalOriginal: Decimal;
  principalBaseEur: Decimal;
};

export type DiscoveredRevolutLowRiskFund = {
  entityId: string;
  entityName: string;
  fundingAccountId: string;
  fundingAccountName: string;
  manualInvestmentId: string | null;
  label: string;
  matcherText: string;
  principalOriginal: string;
  principalBaseEur: string;
  principalCurrency: string;
  returnOriginal: string;
  currentValueOriginal: string;
  snapshotDate: string;
  matchedTransactionCount: number;
};

function findRevolutUsdFundingAccount(dataset: DomainDataset) {
  return (
    dataset.accounts
      .filter(
        (account) =>
          account.isActive &&
          account.institutionName === "Revolut Business" &&
          account.accountType === "company_bank" &&
          account.assetDomain === "cash" &&
          account.defaultCurrency === "USD",
      )
      .sort((left, right) => {
        const leftIsMain = left.displayName === "Main" ? 0 : 1;
        const rightIsMain = right.displayName === "Main" ? 0 : 1;
        return leftIsMain - rightIsMain || left.id.localeCompare(right.id);
      })[0] ?? null
  );
}

function investmentMatcherIncludesRevolutFund(matcherText: string) {
  const terms = parseManualInvestmentMatcherTerms(matcherText);
  return MATCHER_TERMS.every((term) => terms.includes(term));
}

function findPersistedInvestment(
  dataset: DomainDataset,
  fundingAccount: Account,
) {
  return (
    dataset.manualInvestments.find(
      (investment) =>
        investment.fundingAccountId === fundingAccount.id &&
        investmentMatcherIncludesRevolutFund(investment.matcherText),
    ) ?? null
  );
}

function transactionMatchesTerms(transaction: Transaction, terms: string[]) {
  const haystack = buildManualInvestmentMatchHaystack(transaction);
  return terms.some((term) => haystack.includes(term));
}

function summarizeFunding(
  dataset: DomainDataset,
  fundingAccount: Account,
  matcherText: string,
  asOfDate: string,
): FundingSummary {
  const terms = parseManualInvestmentMatcherTerms(matcherText);
  return dataset.transactions
    .filter(
      (transaction) =>
        transaction.accountId === fundingAccount.id &&
        transaction.economicEntityId === fundingAccount.entityId &&
        transaction.transactionDate <= asOfDate &&
        transaction.voidedAt === null &&
        transaction.currencyOriginal === fundingAccount.defaultCurrency &&
        transactionMatchesTerms(transaction, terms),
    )
    .reduce<FundingSummary>(
      (summary, transaction) => ({
        count: summary.count + 1,
        principalOriginal: summary.principalOriginal.minus(
          transaction.amountOriginal,
        ),
        principalBaseEur: summary.principalBaseEur.minus(
          transaction.amountBaseEur,
        ),
      }),
      {
        count: 0,
        principalOriginal: new Decimal(0),
        principalBaseEur: new Decimal(0),
      },
    );
}

function latestValuation(
  dataset: DomainDataset,
  investment: ManualInvestment,
  referenceDate: string,
) {
  return (
    dataset.manualInvestmentValuations
      .filter(
        (valuation) =>
          valuation.manualInvestmentId === investment.id &&
          valuation.snapshotDate <= referenceDate,
      )
      .sort(
        (left, right) =>
          right.snapshotDate.localeCompare(left.snapshotDate) ||
          right.updatedAt.localeCompare(left.updatedAt) ||
          right.createdAt.localeCompare(left.createdAt),
      )[0] ?? null
  );
}

function resolveReturnOriginal(
  dataset: DomainDataset,
  fundingAccount: Account,
  investment: ManualInvestment | null,
  valuation: ManualInvestmentValuation | null,
) {
  if (!investment || !valuation) {
    return new Decimal(0);
  }
  if (valuation.currentValueCurrency !== fundingAccount.defaultCurrency) {
    return new Decimal(0);
  }

  const principalAtSnapshot = summarizeFunding(
    dataset,
    fundingAccount,
    investment.matcherText,
    valuation.snapshotDate,
  ).principalOriginal;

  return new Decimal(valuation.currentValueOriginal).minus(
    principalAtSnapshot,
  );
}

export function resolveDiscoveredRevolutLowRiskFund(
  dataset: DomainDataset,
  referenceDate: string,
): DiscoveredRevolutLowRiskFund | null {
  const fundingAccount = findRevolutUsdFundingAccount(dataset);
  if (!fundingAccount) {
    return null;
  }

  const persistedInvestment = findPersistedInvestment(dataset, fundingAccount);
  const matcherText =
    persistedInvestment?.matcherText ?? REVOLUT_LOW_RISK_FUND_MATCHER_TEXT;
  const fundingSummary = summarizeFunding(
    dataset,
    fundingAccount,
    matcherText,
    referenceDate,
  );
  if (fundingSummary.count === 0 || fundingSummary.principalOriginal.lte(0)) {
    return null;
  }

  const valuation = persistedInvestment
    ? latestValuation(dataset, persistedInvestment, referenceDate)
    : null;
  const returnOriginal = resolveReturnOriginal(
    dataset,
    fundingAccount,
    persistedInvestment,
    valuation,
  );
  const entity = dataset.entities.find(
    (candidate) => candidate.id === fundingAccount.entityId,
  );

  return {
    entityId: fundingAccount.entityId,
    entityName: entity?.displayName ?? fundingAccount.entityId,
    fundingAccountId: fundingAccount.id,
    fundingAccountName: `${fundingAccount.displayName} (${fundingAccount.defaultCurrency})`,
    manualInvestmentId: persistedInvestment?.id ?? null,
    label: persistedInvestment?.label ?? REVOLUT_LOW_RISK_FUND_LABEL,
    matcherText,
    principalOriginal: fundingSummary.principalOriginal.toFixed(2),
    principalBaseEur: fundingSummary.principalBaseEur.toFixed(2),
    principalCurrency: fundingAccount.defaultCurrency,
    returnOriginal: returnOriginal.toFixed(2),
    currentValueOriginal: fundingSummary.principalOriginal
      .plus(returnOriginal)
      .toFixed(2),
    snapshotDate: referenceDate,
    matchedTransactionCount: fundingSummary.count,
  };
}

export function augmentDatasetWithDiscoveredRevolutLowRiskFund(
  dataset: DomainDataset,
  referenceDate: string,
): DomainDataset {
  const discovered = resolveDiscoveredRevolutLowRiskFund(
    dataset,
    referenceDate,
  );
  if (!discovered) {
    return dataset;
  }

  const nowIso = new Date().toISOString();
  const manualInvestmentId =
    discovered.manualInvestmentId ?? VIRTUAL_MANUAL_INVESTMENT_ID;
  const hasPersistedInvestment = Boolean(discovered.manualInvestmentId);
  const manualInvestment: ManualInvestment = hasPersistedInvestment
    ? dataset.manualInvestments.find(
        (investment) => investment.id === manualInvestmentId,
      )!
    : {
        id: manualInvestmentId,
        userId: dataset.profile.id,
        entityId: discovered.entityId,
        fundingAccountId: discovered.fundingAccountId,
        label: discovered.label,
        matcherText: discovered.matcherText,
        note: REVOLUT_LOW_RISK_FUND_NOTE,
        createdAt: nowIso,
        updatedAt: nowIso,
      };
  const virtualValuation: ManualInvestmentValuation = {
    id: VIRTUAL_VALUATION_ID,
    userId: dataset.profile.id,
    manualInvestmentId,
    snapshotDate: referenceDate,
    currentValueOriginal: discovered.currentValueOriginal,
    currentValueCurrency: discovered.principalCurrency,
    note: "Nominal principal plus user-entered return.",
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  return {
    ...dataset,
    manualInvestments: hasPersistedInvestment
      ? dataset.manualInvestments
      : [...dataset.manualInvestments, manualInvestment],
    manualInvestmentValuations: [
      ...dataset.manualInvestmentValuations.filter(
        (valuation) =>
          !(
            valuation.manualInvestmentId === manualInvestmentId &&
            valuation.snapshotDate === referenceDate
          ),
      ),
      virtualValuation,
    ],
  };
}
