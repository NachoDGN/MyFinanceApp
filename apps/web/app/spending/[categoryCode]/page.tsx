import { notFound, redirect } from "next/navigation";

import { FlowCategoryDetailPage } from "../../../components/flow-category-detail-page";
import {
  formatFlowCategoryLabel,
  formatTransactionClassLabel,
} from "../../../lib/flow-page";
import { buildHref } from "../../../lib/navigation";
import {
  categoryAppliesToScope,
  getSpendingCategoryModel,
} from "../../../lib/queries";

type SpendingCategoryModel = Awaited<
  ReturnType<typeof getSpendingCategoryModel>
>;
type SpendingCategoryTransaction =
  SpendingCategoryModel["transactions"][number];

function formatCategoryLabel(
  categoryCode: string | null | undefined,
  transactionClass: string,
  model: SpendingCategoryModel,
) {
  if (!categoryCode) {
    if (transactionClass === "loan_principal_payment") {
      return "Loan Principal";
    }
    if (transactionClass === "loan_interest_payment") {
      return "Loan Interest";
    }
    if (transactionClass === "fee") {
      return "Fees";
    }
    if (transactionClass === "refund") {
      return "Refunds";
    }
    return model.category?.label ?? "Uncategorized";
  }

  return formatFlowCategoryLabel(
    model.dataset,
    categoryCode,
    categoryCode === model.category?.categoryCode
      ? model.category.label
      : categoryCode,
  );
}

function resolveMerchantLabel(transaction: SpendingCategoryTransaction) {
  return (
    transaction.merchantNormalized?.trim() ||
    transaction.counterpartyName?.trim() ||
    transaction.descriptionClean ||
    transaction.descriptionRaw
  );
}

function spendingContributionAmountEur(
  transaction: SpendingCategoryTransaction,
) {
  const amount = Number(transaction.amountBaseEur ?? 0);
  if (!Number.isFinite(amount)) {
    return 0;
  }

  return Math.max(
    transaction.transactionClass === "refund" ? -amount : Math.abs(amount),
    0,
  );
}

export default async function SpendingCategoryPage({
  params,
  searchParams,
}: {
  params: Promise<{ categoryCode: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { categoryCode: rawCategoryCode } = await params;
  const categoryCode = decodeURIComponent(rawCategoryCode);
  const model = await getSpendingCategoryModel(searchParams, categoryCode);

  if (!model.category) {
    notFound();
  }

  if (!categoryAppliesToScope(model.dataset, model.scope, categoryCode)) {
    redirect(buildHref("/spending", model.navigationState, {}));
  }

  return (
    <FlowCategoryDetailPage
      model={{ ...model, category: model.category }}
      categoryCode={categoryCode}
      pathnameBase="/spending"
      groups={model.merchantRows}
      topGroup={model.topMerchant}
      labels={{
        watermark: "SPENDING",
        watermarkClassName: "spending-editorial-watermark",
        titleSuffix: "Spend",
        flowNoun: "spend",
        flowLogicNoun: "spending",
        groupNoun: "merchant",
        groupPlural: "merchant buckets",
        chartDescription:
          "Merchant-level composition for this category in each month.",
        emptyChartLabel: "No category spending data",
        breakdownTitle: "Merchant Breakdown",
        breakdownDescription:
          "Merchant and counterparty buckets inside this category for the selected period.",
        breakdownHeader: "Merchant",
        emptyGroupLabel: "No merchant buckets are available for this category.",
        emptyTransactionsLabel:
          "No transactions are attached to this merchant bucket.",
        topGroupStatLabel: "Top Merchant",
        noTopGroupLabel: "No merchant totals available.",
        transactionSourceDescription:
          "Expand the merchant rows to inspect the transactions that add up to the category total.",
        backHref: buildHref("/spending", model.navigationState, {}),
        backLabel: "Back to Spending",
      }}
      deltaBadgeTone={(value) => (value > 0 ? "accent" : "neutral")}
      resolveGroupLabel={resolveMerchantLabel}
      contributionAmountEur={spendingContributionAmountEur}
      formatCategoryLabel={(transaction) =>
        formatCategoryLabel(
          transaction.categoryCode,
          transaction.transactionClass,
          model,
        )
      }
      largestTransactionBadge={(transaction) =>
        transaction?.merchantNormalized ??
        transaction?.counterpartyName ??
        transaction?.descriptionClean ??
        "No transactions"
      }
    />
  );
}
