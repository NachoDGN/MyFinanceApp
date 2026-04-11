import { AppShell } from "../../components/app-shell";
import { SectionCard } from "../../components/primitives";
import { SettingsWorkbench } from "../../components/settings-workbench";
import { WorkspaceResetCard } from "../../components/workspace-reset-card";
import { getSettingsModel } from "../../lib/queries";

export default async function SettingsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getSettingsModel(searchParams);
  const managedEntities = model.dataset.entities.map((entity) => {
    const accountCount = model.dataset.accounts.filter(
      (account) => account.entityId === entity.id,
    ).length;
    const transactionCount = model.dataset.transactions.filter(
      (transaction) =>
        transaction.accountEntityId === entity.id ||
        transaction.economicEntityId === entity.id,
    ).length;
    const holdingCount = model.dataset.holdingAdjustments.filter(
      (adjustment) => adjustment.entityId === entity.id,
    ).length;
    const manualInvestmentIds = new Set(
      model.dataset.manualInvestments
        .filter((investment) => investment.entityId === entity.id)
        .map((investment) => investment.id),
    );
    const manualInvestmentCount = manualInvestmentIds.size;
    const manualValuationCount =
      model.dataset.manualInvestmentValuations.filter((valuation) =>
        manualInvestmentIds.has(valuation.manualInvestmentId),
      ).length;
    const snapshotCount =
      model.dataset.investmentPositions.filter(
        (position) => position.entityId === entity.id,
      ).length +
      model.dataset.dailyPortfolioSnapshots.filter(
        (snapshot) => snapshot.entityId === entity.id,
      ).length +
      manualInvestmentCount +
      manualValuationCount;
    const canDelete =
      entity.entityKind === "company" &&
      accountCount === 0 &&
      transactionCount === 0 &&
      holdingCount === 0 &&
      snapshotCount === 0;
    return {
      ...entity,
      accountCount,
      transactionCount,
      canDelete,
      deleteBlockedReason: canDelete
        ? null
        : entity.entityKind === "personal"
          ? "The personal entity stays in place as the root owner."
          : "Move or delete linked accounts and derived history before removing this entity.",
    };
  });
  const entityScopeOptions = model.scopeOptions.filter(
    (option) =>
      option.value === "consolidated" || !option.value.startsWith("account:"),
  );

  return (
    <AppShell
      pathname="/settings"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Settings</h1>
            <p className="page-subtitle">
              Configure entity ownership, timezone, and workspace defaults
              without breaking the rule that accounts remain children of an
              entity.
            </p>
          </div>
        </div>

        <SettingsWorkbench
          profile={model.dataset.profile}
          workspaceSettings={model.workspaceSettings}
          entities={managedEntities}
          scopeOptions={entityScopeOptions}
          timezones={model.timezones}
        />

        <SectionCard
          title="Data Management"
          subtitle="Reset imported data while keeping your workspace shape"
          span="span-12"
        >
          <WorkspaceResetCard />
        </SectionCard>
      </div>
    </AppShell>
  );
}
