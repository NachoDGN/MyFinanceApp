"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import type { Entity, WorkspaceSettings } from "@myfinance/domain";
import {
  createEntityAction,
  deleteEntityAction,
  updateEntityAction,
  updateWorkspaceProfileAction,
} from "../app/actions";
import { SectionCard } from "./primitives";

type ManagedEntity = Pick<
  Entity,
  "id" | "slug" | "displayName" | "legalName" | "entityKind" | "baseCurrency"
> & {
  accountCount: number;
  transactionCount: number;
  canDelete: boolean;
  deleteBlockedReason: string | null;
};

export function SettingsWorkbench({
  profile,
  workspaceSettings,
  scopeOptions,
  entities,
  timezones,
}: {
  profile: {
    displayName: string;
    defaultBaseCurrency: string;
    timezone: string;
  };
  workspaceSettings: WorkspaceSettings;
  scopeOptions: Array<{ value: string; label: string }>;
  entities: ManagedEntity[];
  timezones: string[];
}) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);
  const hasPersonalEntity = entities.some(
    (entity) => entity.entityKind === "personal",
  );

  function handleWorkspaceSave(formData: FormData) {
    startTransition(async () => {
      setFeedback(null);
      try {
        const defaultBaseCurrency =
          formData.get("defaultBaseCurrency") === "USD" ? "USD" : "EUR";
        const defaultDisplayCurrency =
          formData.get("defaultDisplayCurrency") === "USD" ? "USD" : "EUR";
        const defaultPeriodPreset =
          formData.get("defaultPeriodPreset") === "ytd" ? "ytd" : "mtd";
        await updateWorkspaceProfileAction({
          displayName: String(formData.get("displayName") ?? ""),
          defaultBaseCurrency,
          timezone: String(formData.get("timezone") ?? ""),
          preferredScope: String(formData.get("preferredScope") ?? "consolidated"),
          defaultDisplayCurrency,
          defaultPeriodPreset,
          defaultCashStaleAfterDays: Number(
            formData.get("defaultCashStaleAfterDays") ?? 7,
          ),
          defaultInvestmentStaleAfterDays: Number(
            formData.get("defaultInvestmentStaleAfterDays") ?? 3,
          ),
        });
        setFeedback("Workspace defaults updated.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error
            ? error.message
            : "Workspace defaults could not be saved.",
        );
      }
    });
  }

  function handleCreateEntity(formData: FormData, form: HTMLFormElement) {
    startTransition(async () => {
      setFeedback(null);
      try {
        const entityKind =
          formData.get("entityKind") === "personal" ? "personal" : "company";
        const baseCurrency =
          formData.get("baseCurrency") === "USD" ? "USD" : "EUR";
        await createEntityAction({
          slug: String(formData.get("slug") ?? ""),
          displayName: String(formData.get("displayName") ?? ""),
          legalName: String(formData.get("legalName") ?? ""),
          entityKind,
          baseCurrency,
        });
        form.reset();
        setFeedback("Entity created.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Entity creation failed.",
        );
      }
    });
  }

  function handleUpdateEntity(entityId: string, formData: FormData) {
    startTransition(async () => {
      setFeedback(null);
      try {
        const baseCurrency =
          formData.get("baseCurrency") === "USD" ? "USD" : "EUR";
        await updateEntityAction({
          entityId,
          slug: String(formData.get("slug") ?? ""),
          displayName: String(formData.get("displayName") ?? ""),
          legalName: String(formData.get("legalName") ?? ""),
          baseCurrency,
        });
        setFeedback("Entity updated.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Entity update failed.",
        );
      }
    });
  }

  function handleDeleteEntity(entity: ManagedEntity) {
    if (!entity.canDelete) {
      setFeedback(
        entity.deleteBlockedReason ?? "This entity cannot be removed.",
      );
      return;
    }

    if (!window.confirm(`Remove ${entity.displayName}? This cannot be undone.`)) {
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      try {
        await deleteEntityAction(entity.id);
        setFeedback(`Removed ${entity.displayName}.`);
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Entity removal failed.",
        );
      }
    });
  }

  return (
    <>
      <SectionCard
        title="Workspace Defaults"
        subtitle="Profile, timezone, filters, and fallback thresholds"
        span="span-6"
      >
        <form
          className="form-grid"
          onSubmit={(event) => {
            event.preventDefault();
            handleWorkspaceSave(new FormData(event.currentTarget));
          }}
        >
          <label className="input-label">
            Profile Name
            <input
              className="input-field"
              name="displayName"
              defaultValue={profile.displayName}
              required
            />
          </label>
          <label className="input-label">
            Timezone
            <input
              className="input-field"
              name="timezone"
              defaultValue={profile.timezone}
              placeholder="Europe/Madrid"
              list="workspace-timezones"
              required
            />
            <datalist id="workspace-timezones">
              {timezones.map((timezone) => (
                <option key={timezone} value={timezone} />
              ))}
            </datalist>
          </label>
          <label className="input-label">
            Base Currency
            <select
              className="input-select"
              name="defaultBaseCurrency"
              defaultValue={profile.defaultBaseCurrency}
            >
              <option value="EUR">EUR</option>
              <option value="USD">USD</option>
            </select>
          </label>
          <label className="input-label">
            Default Display Currency
            <select
              className="input-select"
              name="defaultDisplayCurrency"
              defaultValue={workspaceSettings.defaultDisplayCurrency}
            >
              <option value="EUR">EUR</option>
              <option value="USD">USD</option>
            </select>
          </label>
          <label className="input-label">
            Default Landing Scope
            <select
              className="input-select"
              name="preferredScope"
              defaultValue={workspaceSettings.preferredScope}
            >
              {scopeOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="input-label">
            Default Period
            <select
              className="input-select"
              name="defaultPeriodPreset"
              defaultValue={workspaceSettings.defaultPeriodPreset}
            >
              <option value="mtd">Month to Date</option>
              <option value="ytd">Year to Date</option>
            </select>
          </label>
          <label className="input-label">
            Cash Stale Threshold (days)
            <input
              className="input-field"
              name="defaultCashStaleAfterDays"
              type="number"
              min="1"
              max="365"
              defaultValue={workspaceSettings.defaultCashStaleAfterDays}
              required
            />
          </label>
          <label className="input-label">
            Investment Stale Threshold (days)
            <input
              className="input-field"
              name="defaultInvestmentStaleAfterDays"
              type="number"
              min="1"
              max="365"
              defaultValue={workspaceSettings.defaultInvestmentStaleAfterDays}
              required
            />
          </label>
          <div className="inline-actions" style={{ gridColumn: "1 / -1" }}>
            <button className="btn-pill" type="submit" disabled={isPending}>
              {isPending ? "Saving..." : "Save Workspace Settings"}
            </button>
            <span className="muted">
              Account-level stale thresholds override these defaults when set.
            </span>
          </div>
        </form>
      </SectionCard>

      <SectionCard
        title="Add Entity"
        subtitle="Entities own accounts and drive scope filters"
        span="span-6"
      >
        <form
          className="form-grid"
          onSubmit={(event) => {
            event.preventDefault();
            handleCreateEntity(
              new FormData(event.currentTarget),
              event.currentTarget,
            );
          }}
        >
          <label className="input-label">
            Display Name
            <input
              className="input-field"
              name="displayName"
              placeholder="Company C"
              required
            />
          </label>
          <label className="input-label">
            Scope Slug
            <input
              className="input-field"
              name="slug"
              placeholder="company_c"
              autoCapitalize="none"
              autoCorrect="off"
              spellCheck={false}
              required
            />
          </label>
          <label className="input-label">
            Legal Name
            <input
              className="input-field"
              name="legalName"
              placeholder="Optional"
            />
          </label>
          <label className="input-label">
            Entity Kind
            <select className="input-select" name="entityKind" defaultValue="company">
              <option value="company">Company</option>
              <option value="personal" disabled={hasPersonalEntity}>
                Personal
              </option>
            </select>
          </label>
          <label className="input-label">
            Base Currency
            <select
              className="input-select"
              name="baseCurrency"
              defaultValue={profile.defaultBaseCurrency}
            >
              <option value="EUR">EUR</option>
              <option value="USD">USD</option>
            </select>
          </label>
          <div className="inline-actions" style={{ gridColumn: "1 / -1" }}>
            <button className="btn-pill" type="submit" disabled={isPending}>
              {isPending ? "Saving..." : "Create Entity"}
            </button>
            <span className="muted">
              Keep one personal entity and add companies around it.
            </span>
          </div>
        </form>
      </SectionCard>

      <SectionCard
        title="Entity Registry"
        subtitle="Accounts remain children of entities; deletion stays guarded"
        span="span-12"
      >
        <div className="legend-list">
          {entities.map((entity) => (
            <form
              key={entity.id}
              className="draft-card"
              onSubmit={(event) => {
                event.preventDefault();
                handleUpdateEntity(entity.id, new FormData(event.currentTarget));
              }}
            >
              <div className="draft-meta">
                <div className="inline-actions">
                  <span className="timeline-label">{entity.displayName}</span>
                  <span className="pill">{entity.entityKind}</span>
                  <span className="pill">{entity.accountCount} accounts</span>
                  <span className="pill">{entity.transactionCount} rows</span>
                </div>
                <div className="inline-actions">
                  <button className="btn-pill" type="submit" disabled={isPending}>
                    {isPending ? "Saving..." : "Save"}
                  </button>
                  <button
                    className="btn-ghost"
                    type="button"
                    disabled={isPending || !entity.canDelete}
                    onClick={() => handleDeleteEntity(entity)}
                    title={entity.deleteBlockedReason ?? "Remove entity"}
                  >
                    Remove
                  </button>
                </div>
              </div>

              <div className="form-grid" style={{ marginTop: 20 }}>
                <label className="input-label">
                  Display Name
                  <input
                    className="input-field"
                    name="displayName"
                    defaultValue={entity.displayName}
                    required
                  />
                </label>
                <label className="input-label">
                  Scope Slug
                  <input
                    className="input-field"
                    name="slug"
                    defaultValue={entity.slug}
                    autoCapitalize="none"
                    autoCorrect="off"
                    spellCheck={false}
                    required
                  />
                </label>
                <label className="input-label">
                  Legal Name
                  <input
                    className="input-field"
                    name="legalName"
                    defaultValue={entity.legalName ?? ""}
                  />
                </label>
                <label className="input-label">
                  Base Currency
                  <select
                    className="input-select"
                    name="baseCurrency"
                    defaultValue={entity.baseCurrency}
                  >
                    <option value="EUR">EUR</option>
                    <option value="USD">USD</option>
                  </select>
                </label>
              </div>

              {entity.deleteBlockedReason ? (
                <div className="status-note">{entity.deleteBlockedReason}</div>
              ) : null}
            </form>
          ))}
        </div>
      </SectionCard>

      {feedback ? (
        <div className="status-note" style={{ gridColumn: "1 / -1" }}>
          {feedback}
        </div>
      ) : null}
    </>
  );
}
