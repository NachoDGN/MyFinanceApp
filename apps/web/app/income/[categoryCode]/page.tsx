import { notFound } from "next/navigation";

import { FlowCategoryDetailPage } from "../../../components/flow-category-detail-page";
import {
  formatFlowCategoryLabel,
  formatTransactionClassLabel,
} from "../../../lib/flow-page";
import { buildHref } from "../../../lib/navigation";
import { getIncomeCategoryModel } from "../../../lib/queries";

type IncomeCategoryModel = Awaited<ReturnType<typeof getIncomeCategoryModel>>;
type IncomeCategoryTransaction = IncomeCategoryModel["transactions"][number];

function formatCategoryLabel(
  categoryCode: string | null | undefined,
  transactionClass: string,
  model: IncomeCategoryModel,
) {
  const category = model.category;
  const fallbackLabel =
    category && categoryCode === category.categoryCode
      ? category.label
      : (category?.label ?? formatTransactionClassLabel(transactionClass));
  return formatFlowCategoryLabel(model.dataset, categoryCode, fallbackLabel);
}

function resolveIncomeSourceLabel(transaction: IncomeCategoryTransaction) {
  return (
    transaction.counterpartyName?.trim() ||
    transaction.merchantNormalized?.trim() ||
    transaction.descriptionClean ||
    transaction.descriptionRaw
  );
}

function incomeContributionAmountEur(transaction: IncomeCategoryTransaction) {
  const amount = Number(transaction.amountBaseEur ?? 0);
  return Number.isFinite(amount) ? Math.max(amount, 0) : 0;
}

export default async function IncomeCategoryPage({
  params,
  searchParams,
}: {
  params: Promise<{ categoryCode: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { categoryCode: rawCategoryCode } = await params;
  const categoryCode = decodeURIComponent(rawCategoryCode);
  const model = await getIncomeCategoryModel(searchParams, categoryCode);

  if (!model.category) {
    notFound();
  }

  return (
    <FlowCategoryDetailPage
      model={{ ...model, category: model.category }}
      categoryCode={categoryCode}
      pathnameBase="/income"
      groups={model.sourceRows}
      topGroup={model.topSource}
      labels={{
        watermark: "INCOME",
        titleSuffix: "Income",
        flowNoun: "income",
        flowLogicNoun: "income",
        groupNoun: "source",
        groupPlural: "source buckets",
        chartDescription:
          "Source-level composition for this category in each month.",
        emptyChartLabel: "No category income data",
        breakdownTitle: "Source Breakdown",
        breakdownDescription:
          "Payer and counterparty buckets inside this category for the selected period.",
        breakdownHeader: "Source",
        emptyGroupLabel: "No source buckets are available for this category.",
        emptyTransactionsLabel:
          "No transactions are attached to this source bucket.",
        topGroupStatLabel: "Top Source",
        noTopGroupLabel: "No source totals available.",
        transactionSourceDescription:
          "Expand the source rows to inspect the transactions that add up to the category total.",
        backHref: buildHref("/income", model.navigationState, {}),
        backLabel: "Back to Income",
      }}
      deltaBadgeTone={(value) => (value >= 0 ? "accent" : "neutral")}
      resolveGroupLabel={resolveIncomeSourceLabel}
      contributionAmountEur={incomeContributionAmountEur}
      formatCategoryLabel={(transaction) =>
        formatCategoryLabel(
          transaction.categoryCode,
          transaction.transactionClass,
          model,
        )
      }
      largestTransactionBadge={(transaction) =>
        transaction?.counterpartyName ??
        transaction?.merchantNormalized ??
        transaction?.descriptionClean ??
        "No transactions"
      }
    />
  );
}
