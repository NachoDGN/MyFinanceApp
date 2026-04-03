"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import type { AccountType, Entity, ImportTemplate } from "@myfinance/domain";
import { accountTypeOptions } from "@myfinance/domain/template-config";
import { createAccountAction, deleteAccountAction } from "../app/actions";
import { SectionCard, SimpleTable } from "./primitives";

type ManagedAccount = {
  id: string;
  displayName: string;
  institutionName: string;
  entityName: string;
  accountType: string;
  currentBalance: string;
  currentBalanceCurrency: string;
  lastImport: string;
  staleThreshold: string;
  setupStatus: string;
  balanceMode: string;
  aliases: string;
  canDelete: boolean;
  deleteBlockedReason: string | null;
};

export function AccountsWorkbench({
  entities,
  templates,
  accounts,
}: {
  entities: Entity[];
  templates: ImportTemplate[];
  accounts: ManagedAccount[];
}) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);
  const [accountType, setAccountType] = useState<AccountType>("checking");
  const [selectedTemplateId, setSelectedTemplateId] = useState("");

  const compatibleTemplates = useMemo(
    () =>
      templates.filter(
        (template) => template.compatibleAccountType === accountType,
      ),
    [accountType, templates],
  );

  function handleCreate(formData: FormData, form: HTMLFormElement) {
    startTransition(async () => {
      setFeedback(null);
      try {
        await createAccountAction({
          entityId: String(formData.get("entityId") ?? ""),
          institutionName: String(formData.get("institutionName") ?? ""),
          displayName: String(formData.get("displayName") ?? ""),
          accountType,
          defaultCurrency: String(formData.get("defaultCurrency") ?? "EUR"),
          openingBalanceOriginal: String(
            formData.get("openingBalanceOriginal") ?? "",
          ),
          openingBalanceDate: String(formData.get("openingBalanceDate") ?? ""),
          includeInConsolidation:
            formData.get("includeInConsolidation") === "on",
          importTemplateDefaultId: selectedTemplateId,
          matchingAliasesText: String(
            formData.get("matchingAliasesText") ?? "",
          ),
          accountSuffix: String(formData.get("accountSuffix") ?? ""),
          balanceMode:
            String(formData.get("balanceMode") ?? "statement") === "computed"
              ? "computed"
              : "statement",
          staleAfterDays: String(formData.get("staleAfterDays") ?? "").trim()
            ? Number(formData.get("staleAfterDays"))
            : null,
        });
        form.reset();
        setAccountType("checking");
        setSelectedTemplateId("");
        setFeedback("Account created.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Account creation failed.",
        );
      }
    });
  }

  function handleDelete(account: ManagedAccount) {
    if (!account.canDelete) {
      setFeedback(
        account.deleteBlockedReason ?? "This account cannot be removed.",
      );
      return;
    }
    if (
      !window.confirm(`Remove ${account.displayName}? This cannot be undone.`)
    ) {
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      try {
        await deleteAccountAction(account.id);
        setFeedback(`Removed ${account.displayName}.`);
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Account removal failed.",
        );
      }
    });
  }

  return (
    <>
      <SectionCard
        title="Add Account"
        subtitle="Create a new cash or investment account"
        span="span-4"
      >
        <form
          className="form-grid"
          onSubmit={(event) => {
            event.preventDefault();
            handleCreate(
              new FormData(event.currentTarget),
              event.currentTarget,
            );
          }}
        >
          <label className="input-label">
            Entity
            <select
              className="input-select"
              name="entityId"
              defaultValue={entities[0]?.id}
            >
              {entities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.displayName}
                </option>
              ))}
            </select>
          </label>
          <label className="input-label">
            Account Type
            <select
              className="input-select"
              value={accountType}
              onChange={(event) => {
                const nextAccountType = event.target.value as AccountType;
                setAccountType(nextAccountType);
                setSelectedTemplateId((currentValue) =>
                  templates.some(
                    (template) =>
                      template.id === currentValue &&
                      template.compatibleAccountType === nextAccountType,
                  )
                    ? currentValue
                    : "",
                );
              }}
            >
              {accountTypeOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <label className="input-label">
            Display Name
            <input
              className="input-field"
              name="displayName"
              placeholder="Personal Checking"
              required
            />
          </label>
          <label className="input-label">
            Institution
            <input
              className="input-field"
              name="institutionName"
              placeholder="Santander"
              required
            />
          </label>
          <label className="input-label">
            Default Currency
            <input
              className="input-field"
              name="defaultCurrency"
              defaultValue="EUR"
              required
            />
          </label>
          <label className="input-label">
            Default Import Template
            <select
              className="input-select"
              value={selectedTemplateId}
              onChange={(event) => setSelectedTemplateId(event.target.value)}
            >
              <option value="">No default template</option>
              {compatibleTemplates.map((template) => (
                <option key={template.id} value={template.id}>
                  {template.name}
                </option>
              ))}
            </select>
          </label>
          <label className="input-label">
            Opening Balance
            <input
              className="input-field"
              name="openingBalanceOriginal"
              placeholder="Optional"
            />
          </label>
          <label className="input-label">
            Opening Balance Date
            <input
              className="input-field"
              name="openingBalanceDate"
              type="date"
            />
          </label>
          <label className="input-label">
            Account Suffix
            <input
              className="input-field"
              name="accountSuffix"
              placeholder="Last 4 digits"
            />
          </label>
          <label className="input-label">
            Stale Threshold (days)
            <input
              className="input-field"
              name="staleAfterDays"
              type="number"
              min="1"
              max="365"
            />
          </label>
          <label className="input-label">
            Balance Mode
            <select
              className="input-select"
              name="balanceMode"
              defaultValue="statement"
            >
              <option value="statement">statement</option>
              <option value="computed">computed</option>
            </select>
          </label>
          <label className="input-label">
            Matching Aliases
            <input
              className="input-field"
              name="matchingAliasesText"
              placeholder="Comma-separated text seen in statements"
            />
          </label>
          <label className="checkbox-row" style={{ gridColumn: "1 / -1" }}>
            <input
              name="includeInConsolidation"
              type="checkbox"
              defaultChecked
            />
            Include in consolidated totals
          </label>
          <div className="inline-actions" style={{ gridColumn: "1 / -1" }}>
            <button className="btn-pill" type="submit" disabled={isPending}>
              {isPending ? "Saving..." : "Create Account"}
            </button>
            <span className="muted">
              Deletion is only available for accounts with no imported or
              derived history.
            </span>
          </div>
          {feedback ? (
            <div
              className="status-note"
              style={{ gridColumn: "1 / -1", marginTop: 0 }}
            >
              {feedback}
            </div>
          ) : null}
        </form>
      </SectionCard>

      <SectionCard
        title="Account Registry"
        subtitle="Removal is guarded when history already exists"
        span="span-8"
      >
        <div className="legend-list">
          {accounts.map((account) => (
            <div key={account.id} className="draft-card">
              <div className="draft-meta">
                <div>
                  <span className="timeline-label">{account.displayName}</span>
                  <div className="metric-nominal">
                    {account.institutionName} · {account.entityName} ·{" "}
                    {account.accountType}
                  </div>
                </div>
                <button
                  className="btn-ghost"
                  type="button"
                  disabled={isPending || !account.canDelete}
                  onClick={() => handleDelete(account)}
                  title={account.deleteBlockedReason ?? "Remove account"}
                >
                  Remove
                </button>
              </div>
              <div className="split-grid" style={{ marginTop: 16 }}>
                <div>
                  <span className="label-sm">Current Balance</span>
                  <div className="timeline-label">{account.currentBalance}</div>
                  <div className="metric-nominal">
                    {account.currentBalanceCurrency}
                  </div>
                </div>
                <div>
                  <span className="label-sm">Setup</span>
                  <div className="timeline-label">{account.setupStatus}</div>
                </div>
                <div>
                  <span className="label-sm">Last Import</span>
                  <div className="metric-nominal">{account.lastImport}</div>
                </div>
                <div>
                  <span className="label-sm">Stale Threshold</span>
                  <div className="metric-nominal">{account.staleThreshold}</div>
                </div>
                <div>
                  <span className="label-sm">Balance Mode</span>
                  <div className="metric-nominal">{account.balanceMode}</div>
                </div>
                <div>
                  <span className="label-sm">Aliases</span>
                  <div className="metric-nominal">{account.aliases}</div>
                </div>
              </div>
              {!account.canDelete && account.deleteBlockedReason ? (
                <div className="status-note">{account.deleteBlockedReason}</div>
              ) : null}
            </div>
          ))}
        </div>
      </SectionCard>

      <SimpleTable
        span="span-12"
        headers={[
          "Account",
          "Entity",
          "Institution",
          "Current Balance",
          "Currency",
          "Last Import",
          "Mode",
          "Aliases",
          "Removal",
        ]}
        rows={accounts.map((account) => [
          account.displayName,
          account.entityName,
          account.institutionName,
          account.currentBalance,
          account.currentBalanceCurrency,
          account.lastImport,
          account.balanceMode,
          account.aliases,
          account.canDelete ? "Available" : "Locked",
        ])}
      />
    </>
  );
}
