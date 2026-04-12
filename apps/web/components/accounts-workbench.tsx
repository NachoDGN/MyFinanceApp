"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import type { AccountType, Entity, ImportTemplate } from "@myfinance/domain";
import { accountTypeOptions } from "@myfinance/domain/template-config";
import {
  createAccountAction,
  deleteAccountAction,
  updateAccountAction,
} from "../app/actions";
import { SectionCard, SimpleTable } from "./primitives";

type ManagedAccount = {
  id: string;
  displayName: string;
  institutionName: string;
  entityName: string;
  accountType: AccountType;
  currentBalance: string;
  currentBalanceCurrency: string;
  lastImport: string;
  staleThreshold: string;
  setupStatus: string;
  balanceMode: "statement" | "computed";
  aliases: string;
  defaultCurrency: string;
  openingBalanceOriginal: string | null;
  openingBalanceDate: string | null;
  includeInConsolidation: boolean;
  importTemplateDefaultId: string | null;
  matchingAliasesText: string;
  accountSuffix: string | null;
  staleAfterDays: number | null;
  workspaceDefaultStaleAfterDays: number;
  canDelete: boolean;
  deleteBlockedReason: string | null;
};

export function AccountsWorkbench({
  entities,
  templates,
  accounts,
  defaultCurrency,
  defaultCashStaleAfterDays,
  defaultInvestmentStaleAfterDays,
}: {
  entities: Entity[];
  templates: ImportTemplate[];
  accounts: ManagedAccount[];
  defaultCurrency: string;
  defaultCashStaleAfterDays: number;
  defaultInvestmentStaleAfterDays: number;
}) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);
  const [accountType, setAccountType] = useState<AccountType>("checking");
  const [selectedTemplateId, setSelectedTemplateId] = useState("");
  const [staleAfterDays, setStaleAfterDays] = useState(
    String(defaultCashStaleAfterDays),
  );
  const [editingAccountId, setEditingAccountId] = useState<string | null>(null);
  const hasEntities = entities.length > 0;

  const compatibleTemplates = useMemo(
    () =>
      templates.filter(
        (template) => template.compatibleAccountType === accountType,
      ),
    [accountType, templates],
  );

  function getDefaultStaleThreshold(nextAccountType: AccountType) {
    return nextAccountType === "brokerage_account"
      ? String(defaultInvestmentStaleAfterDays)
      : String(defaultCashStaleAfterDays);
  }

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
          staleAfterDays: String(formData.get("staleAfterDays") ?? "").trim(),
        });
        form.reset();
        setAccountType("checking");
        setSelectedTemplateId("");
        setStaleAfterDays(String(defaultCashStaleAfterDays));
        setFeedback("Account created.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Account creation failed.",
        );
      }
    });
  }

  function handleUpdate(formData: FormData, account: ManagedAccount) {
    startTransition(async () => {
      setFeedback(null);
      try {
        const nextDisplayName = String(
          formData.get("displayName") ?? account.displayName,
        );
        await updateAccountAction({
          accountId: account.id,
          institutionName: String(
            formData.get("institutionName") ?? account.institutionName,
          ),
          displayName: nextDisplayName,
          defaultCurrency: String(
            formData.get("defaultCurrency") ?? account.defaultCurrency,
          ),
          openingBalanceOriginal: String(
            formData.get("openingBalanceOriginal") ?? "",
          ),
          openingBalanceDate: String(formData.get("openingBalanceDate") ?? ""),
          includeInConsolidation:
            formData.get("includeInConsolidation") === "on",
          importTemplateDefaultId: String(
            formData.get("importTemplateDefaultId") ?? "",
          ),
          matchingAliasesText: String(
            formData.get("matchingAliasesText") ?? "",
          ),
          accountSuffix: String(formData.get("accountSuffix") ?? ""),
          balanceMode:
            String(formData.get("balanceMode") ?? account.balanceMode) ===
            "computed"
              ? "computed"
              : "statement",
          staleAfterDays: String(formData.get("staleAfterDays") ?? "").trim(),
        });
        setEditingAccountId(null);
        setFeedback(`Updated ${nextDisplayName}.`);
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Account update failed.",
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
        if (editingAccountId === account.id) {
          setEditingAccountId(null);
        }
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
            if (!hasEntities) {
              setFeedback(
                "Create an entity first in Settings before adding an account.",
              );
              return;
            }
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
              disabled={!hasEntities}
              defaultValue={entities[0]?.id}
            >
              {!hasEntities ? (
                <option value="">Create an entity first</option>
              ) : null}
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
                setStaleAfterDays(getDefaultStaleThreshold(nextAccountType));
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
              defaultValue={defaultCurrency}
              required
            />
          </label>
          <label className="input-label">
            Default Import Template
            <select
              className="input-select"
              name="importTemplateDefaultId"
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
              value={staleAfterDays}
              onChange={(event) => setStaleAfterDays(event.target.value)}
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
            <button
              className="btn-pill"
              type="submit"
              disabled={isPending || !hasEntities}
            >
              {isPending ? "Saving..." : "Create Account"}
            </button>
            <span className="muted">
              {hasEntities
                ? "Deletion is only available for accounts with no imported or derived history."
                : "Create an entity in Settings first, then come back here to add accounts."}
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
        subtitle="Edit opening balances and operational settings without changing ownership or type"
        span="span-8"
      >
        {feedback ? (
          <div className="status-note" style={{ marginBottom: 16 }}>
            {feedback}
          </div>
        ) : null}
        <div className="legend-list">
          {accounts.map((account) => {
            const isEditing = editingAccountId === account.id;
            const compatibleAccountTemplates = templates.filter(
              (template) =>
                template.compatibleAccountType === account.accountType,
            );

            return (
              <div key={account.id} className="draft-card">
                <div className="draft-meta">
                  <div>
                    <span className="timeline-label">{account.displayName}</span>
                    <div className="metric-nominal">
                      {account.institutionName} · {account.entityName} ·{" "}
                      {account.accountType}
                    </div>
                  </div>
                  <div className="inline-actions">
                    <button
                      className="btn-ghost"
                      type="button"
                      disabled={isPending}
                      onClick={() => {
                        setFeedback(null);
                        setEditingAccountId((currentAccountId) =>
                          currentAccountId === account.id ? null : account.id,
                        );
                      }}
                    >
                      {isEditing ? "Close" : "Edit"}
                    </button>
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
                </div>
                <div className="split-grid" style={{ marginTop: 16 }}>
                  <div>
                    <span className="label-sm">Current Balance</span>
                    <div className="timeline-label">
                      {account.currentBalance}
                    </div>
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
                {isEditing ? (
                  <form
                    className="form-grid"
                    style={{ marginTop: 16 }}
                    onSubmit={(event) => {
                      event.preventDefault();
                      handleUpdate(new FormData(event.currentTarget), account);
                    }}
                  >
                    <label className="input-label">
                      Display Name
                      <input
                        className="input-field"
                        name="displayName"
                        defaultValue={account.displayName}
                        required
                      />
                    </label>
                    <label className="input-label">
                      Institution
                      <input
                        className="input-field"
                        name="institutionName"
                        defaultValue={account.institutionName}
                        required
                      />
                    </label>
                    <label className="input-label">
                      Default Currency
                      <input
                        className="input-field"
                        name="defaultCurrency"
                        defaultValue={account.defaultCurrency}
                        required
                      />
                    </label>
                    <label className="input-label">
                      Default Import Template
                      <select
                        className="input-select"
                        name="importTemplateDefaultId"
                        defaultValue={account.importTemplateDefaultId ?? ""}
                      >
                        <option value="">No default template</option>
                        {compatibleAccountTemplates.map((template) => (
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
                        defaultValue={account.openingBalanceOriginal ?? ""}
                        placeholder="Optional"
                      />
                    </label>
                    <label className="input-label">
                      Opening Balance Date
                      <input
                        className="input-field"
                        name="openingBalanceDate"
                        type="date"
                        defaultValue={account.openingBalanceDate ?? ""}
                      />
                    </label>
                    <label className="input-label">
                      Account Suffix
                      <input
                        className="input-field"
                        name="accountSuffix"
                        defaultValue={account.accountSuffix ?? ""}
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
                        defaultValue={account.staleAfterDays ?? ""}
                        placeholder={`${account.workspaceDefaultStaleAfterDays}`}
                      />
                    </label>
                    <label className="input-label">
                      Balance Mode
                      <select
                        className="input-select"
                        name="balanceMode"
                        defaultValue={account.balanceMode}
                      >
                        <option value="statement">statement</option>
                        <option value="computed">computed</option>
                      </select>
                    </label>
                    <label className="input-label" style={{ gridColumn: "1 / -1" }}>
                      Matching Aliases
                      <input
                        className="input-field"
                        name="matchingAliasesText"
                        defaultValue={account.matchingAliasesText}
                        placeholder="Comma-separated text seen in statements"
                      />
                    </label>
                    <label className="checkbox-row" style={{ gridColumn: "1 / -1" }}>
                      <input
                        name="includeInConsolidation"
                        type="checkbox"
                        defaultChecked={account.includeInConsolidation}
                      />
                      Include in consolidated totals
                    </label>
                    <div className="status-note" style={{ gridColumn: "1 / -1" }}>
                      Entity ownership and account type stay fixed after
                      creation so historical attribution and template
                      compatibility do not drift.
                    </div>
                    <div className="inline-actions" style={{ gridColumn: "1 / -1" }}>
                      <button
                        className="btn-pill"
                        type="submit"
                        disabled={isPending}
                      >
                        {isPending ? "Saving..." : "Save Changes"}
                      </button>
                      <button
                        className="btn-ghost"
                        type="button"
                        disabled={isPending}
                        onClick={() => setEditingAccountId(null)}
                      >
                        Cancel
                      </button>
                      <span className="muted">
                        Leave stale threshold blank to fall back to the workspace
                        default of {account.workspaceDefaultStaleAfterDays} days.
                      </span>
                    </div>
                  </form>
                ) : null}
                {!account.canDelete && account.deleteBlockedReason ? (
                  <div className="status-note">{account.deleteBlockedReason}</div>
                ) : null}
              </div>
            );
          })}
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
