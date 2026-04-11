"use client";

import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import {
  createManualInvestmentAction,
  deleteManualInvestmentAction,
  recordManualInvestmentValuationAction,
} from "../app/actions";
import { SectionCard } from "./primitives";

type EntityOption = {
  id: string;
  label: string;
};

type CashAccountOption = {
  id: string;
  entityId: string;
  label: string;
};

type ManualInvestmentSummary = {
  id: string;
  entityId: string;
  entityName: string;
  fundingAccountId: string;
  fundingAccountName: string;
  label: string;
  matcherText: string;
  note: string | null;
  latestSnapshotDate: string | null;
  latestValueOriginal: string | null;
  latestValueCurrency: string | null;
  currentValueDisplay: string;
  investedAmountDisplay: string;
  unrealizedDisplay: string;
  unrealizedPercent: string | null;
  freshnessLabel: string;
};

export function ManualInvestmentWorkbench({
  entities,
  cashAccounts,
  manualInvestments,
  referenceDate,
}: {
  entities: EntityOption[];
  cashAccounts: CashAccountOption[];
  manualInvestments: ManualInvestmentSummary[];
  referenceDate: string;
}) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);
  const [selectedEntityId, setSelectedEntityId] = useState(
    entities[0]?.id ?? "",
  );
  const entityCashAccounts = cashAccounts.filter(
    (account) => account.entityId === selectedEntityId,
  );
  const [selectedFundingAccountId, setSelectedFundingAccountId] = useState(
    entityCashAccounts[0]?.id ?? "",
  );

  useEffect(() => {
    if (
      entityCashAccounts.some(
        (account) => account.id === selectedFundingAccountId,
      )
    ) {
      return;
    }
    setSelectedFundingAccountId(entityCashAccounts[0]?.id ?? "");
  }, [entityCashAccounts, selectedFundingAccountId]);

  function handleCreateInvestment(formData: FormData, form: HTMLFormElement) {
    startTransition(async () => {
      setFeedback(null);
      try {
        await createManualInvestmentAction({
          entityId: String(formData.get("entityId") ?? ""),
          fundingAccountId: String(formData.get("fundingAccountId") ?? ""),
          label: String(formData.get("label") ?? ""),
          matcherText: String(formData.get("matcherText") ?? ""),
          note: String(formData.get("note") ?? ""),
          snapshotDate: String(formData.get("snapshotDate") ?? ""),
          currentValueOriginal: String(
            formData.get("currentValueOriginal") ?? "",
          ),
          currentValueCurrency: String(
            formData.get("currentValueCurrency") ?? "",
          ),
          valuationNote: String(formData.get("valuationNote") ?? ""),
        });
        form.reset();
        setFeedback("Tracked investment created.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error
            ? error.message
            : "Tracked investment could not be created.",
        );
      }
    });
  }

  function handleRecordValuation(
    manualInvestmentId: string,
    formData: FormData,
  ) {
    startTransition(async () => {
      setFeedback(null);
      try {
        await recordManualInvestmentValuationAction({
          manualInvestmentId,
          snapshotDate: String(formData.get("snapshotDate") ?? ""),
          currentValueOriginal: String(
            formData.get("currentValueOriginal") ?? "",
          ),
          currentValueCurrency: String(
            formData.get("currentValueCurrency") ?? "",
          ),
          note: String(formData.get("note") ?? ""),
        });
        setFeedback("Valuation saved.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error
            ? error.message
            : "Valuation could not be saved.",
        );
      }
    });
  }

  function handleDeleteInvestment(investment: ManualInvestmentSummary) {
    if (
      !window.confirm(
        `Remove ${investment.label}? This also deletes its saved valuation history.`,
      )
    ) {
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      try {
        await deleteManualInvestmentAction(investment.id);
        setFeedback(`Removed ${investment.label}.`);
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error
            ? error.message
            : "Tracked investment could not be removed.",
        );
      }
    });
  }

  return (
    <>
      <SectionCard
        title="Manual Fund Valuations"
        subtitle="Track company fund value manually while cost basis comes from matched cash transfers"
        span="span-6"
      >
        <form
          className="form-grid"
          onSubmit={(event) => {
            event.preventDefault();
            handleCreateInvestment(
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
              value={selectedEntityId}
              onChange={(event) => setSelectedEntityId(event.target.value)}
            >
              {entities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.label}
                </option>
              ))}
            </select>
          </label>
          <label className="input-label">
            Funding Account
            <select
              className="input-select"
              name="fundingAccountId"
              value={selectedFundingAccountId}
              onChange={(event) =>
                setSelectedFundingAccountId(event.target.value)
              }
              disabled={entityCashAccounts.length === 0}
            >
              {entityCashAccounts.length === 0 ? (
                <option value="">No cash accounts for this entity</option>
              ) : (
                entityCashAccounts.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.label}
                  </option>
                ))
              )}
            </select>
          </label>
          <label className="input-label">
            Investment Label
            <input
              className="input-field"
              name="label"
              placeholder="Revolut Treasury Fund"
              required
            />
          </label>
          <label className="input-label">
            Snapshot Date
            <input
              className="input-field"
              name="snapshotDate"
              type="date"
              defaultValue={referenceDate}
              required
            />
          </label>
          <label className="input-label">
            Current Value
            <input
              className="input-field"
              name="currentValueOriginal"
              inputMode="decimal"
              placeholder="10000"
              required
            />
          </label>
          <label className="input-label">
            Value Currency
            <select
              className="input-select"
              name="currentValueCurrency"
              defaultValue="EUR"
            >
              <option value="EUR">EUR</option>
              <option value="USD">USD</option>
            </select>
          </label>
          <label className="input-label" style={{ gridColumn: "1 / -1" }}>
            Matcher Terms
            <textarea
              className="input-textarea"
              name="matcherText"
              placeholder="low-risk fund, treasury"
              rows={4}
              required
            />
          </label>
          <label className="input-label" style={{ gridColumn: "1 / -1" }}>
            Investment Note
            <input
              className="input-field"
              name="note"
              placeholder="Used for Revolut treasury-fund cash movements."
            />
          </label>
          <label className="input-label" style={{ gridColumn: "1 / -1" }}>
            Valuation Note
            <input
              className="input-field"
              name="valuationNote"
              placeholder="Manual mark-to-market from the Revolut app."
            />
          </label>
          <div className="inline-actions" style={{ gridColumn: "1 / -1" }}>
            <button
              className="btn-pill"
              type="submit"
              disabled={isPending || entityCashAccounts.length === 0}
            >
              {isPending ? "Saving..." : "Create Tracked Investment"}
            </button>
            <span className="muted">
              Use comma- or line-separated matcher terms taken from the bank
              transfer description.
            </span>
          </div>
        </form>
      </SectionCard>

      <SectionCard
        title="Tracked Company Funds"
        subtitle="Latest manual value plus derived invested capital and unrealized P/L"
        span="span-6"
      >
        <div className="investment-position-list">
          {manualInvestments.length === 0 ? (
            <article className="investment-position-card">
              <div className="investment-position-copy">
                <h3 className="investment-position-name">
                  No tracked company funds yet
                </h3>
                <p className="investment-position-symbol">
                  Create one to fold off-platform fund value into the portfolio
                  KPIs.
                </p>
              </div>
            </article>
          ) : (
            manualInvestments.map((investment) => (
              <article className="investment-position-card" key={investment.id}>
                <div className="investment-position-head">
                  <div className="investment-position-copy">
                    <h3 className="investment-position-name">
                      {investment.label}
                    </h3>
                    <p className="investment-position-symbol">
                      {investment.entityName} · {investment.fundingAccountName}{" "}
                      · {investment.freshnessLabel}
                    </p>
                  </div>
                  <div className="investment-position-values">
                    <strong>{investment.currentValueDisplay}</strong>
                    <span
                      className={`investment-return ${Number(investment.unrealizedPercent ?? "0") >= 0 ? "positive" : "negative"}`}
                    >
                      {investment.unrealizedDisplay}
                      {investment.unrealizedPercent
                        ? ` / ${investment.unrealizedPercent}%`
                        : ""}
                    </span>
                  </div>
                </div>
                <div
                  className="investment-summary-meta"
                  style={{
                    display: "grid",
                    gap: 6,
                    marginTop: 12,
                  }}
                >
                  <span>
                    Derived invested capital: {investment.investedAmountDisplay}
                  </span>
                  <span>
                    Latest snapshot: {investment.latestSnapshotDate ?? "None"}
                    {investment.latestValueOriginal &&
                    investment.latestValueCurrency
                      ? ` · ${investment.latestValueOriginal} ${investment.latestValueCurrency}`
                      : ""}
                  </span>
                  <span>Matcher terms: {investment.matcherText}</span>
                  {investment.note ? (
                    <span>Note: {investment.note}</span>
                  ) : null}
                </div>
                <form
                  className="form-grid"
                  style={{ marginTop: 16 }}
                  onSubmit={(event) => {
                    event.preventDefault();
                    handleRecordValuation(
                      investment.id,
                      new FormData(event.currentTarget),
                    );
                  }}
                >
                  <label className="input-label">
                    Snapshot Date
                    <input
                      className="input-field"
                      name="snapshotDate"
                      type="date"
                      defaultValue={referenceDate}
                      required
                    />
                  </label>
                  <label className="input-label">
                    Current Value
                    <input
                      className="input-field"
                      name="currentValueOriginal"
                      inputMode="decimal"
                      defaultValue={investment.latestValueOriginal ?? ""}
                      required
                    />
                  </label>
                  <label className="input-label">
                    Value Currency
                    <select
                      className="input-select"
                      name="currentValueCurrency"
                      defaultValue={investment.latestValueCurrency ?? "EUR"}
                    >
                      <option value="EUR">EUR</option>
                      <option value="USD">USD</option>
                    </select>
                  </label>
                  <label
                    className="input-label"
                    style={{ gridColumn: "1 / -1" }}
                  >
                    Valuation Note
                    <input
                      className="input-field"
                      name="note"
                      placeholder="Manual mark-to-market update."
                    />
                  </label>
                  <div
                    className="inline-actions"
                    style={{ gridColumn: "1 / -1" }}
                  >
                    <button
                      className="btn-pill"
                      type="submit"
                      disabled={isPending}
                    >
                      {isPending ? "Saving..." : "Save Valuation"}
                    </button>
                    <button
                      className="btn-pill"
                      type="button"
                      onClick={() => handleDeleteInvestment(investment)}
                      disabled={isPending}
                    >
                      Remove
                    </button>
                  </div>
                </form>
              </article>
            ))
          )}
        </div>
      </SectionCard>

      {feedback ? (
        <p className="muted" style={{ gridColumn: "1 / -1", marginTop: -8 }}>
          {feedback}
        </p>
      ) : null}
    </>
  );
}
