"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";

import type { ImportTemplate } from "@myfinance/domain";
import {
  accountTypeOptions,
  canonicalFieldOptions,
  createDefaultColumnMappings,
  fileKindOptions,
  signModeOptions,
  type TemplateColumnMapping,
  type TemplateSignMode,
} from "@myfinance/domain/template-config";
import { createTemplateAction } from "../app/actions";

const signModeLabels: Record<TemplateSignMode, string> = {
  signed_amount: "Signed amount column",
  amount_direction_column: "Amount plus direction column",
  debit_credit_columns: "Separate debit and credit columns",
};

const defaultDirectionValues = {
  debit: "debit, out, sell, withdrawal",
  credit: "credit, in, buy, deposit",
};

function updateMappingRow(
  rows: TemplateColumnMapping[],
  index: number,
  patch: Partial<TemplateColumnMapping>,
) {
  return rows.map((row, rowIndex) => (rowIndex === index ? { ...row, ...patch } : row));
}

export function TemplateWorkbench({ templates }: { templates: ImportTemplate[] }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<string | null>(null);
  const [columnMappings, setColumnMappings] = useState<TemplateColumnMapping[]>(() =>
    createDefaultColumnMappings(),
  );
  const [signMode, setSignMode] = useState<TemplateSignMode>("signed_amount");
  const [invertSign, setInvertSign] = useState(false);
  const [directionColumn, setDirectionColumn] = useState("");
  const [debitColumn, setDebitColumn] = useState("");
  const [creditColumn, setCreditColumn] = useState("");
  const [debitValuesText, setDebitValuesText] = useState(defaultDirectionValues.debit);
  const [creditValuesText, setCreditValuesText] = useState(defaultDirectionValues.credit);
  const [dateDayFirst, setDateDayFirst] = useState(true);

  async function handleSubmit(formData: FormData) {
    setMessage(null);

    const result = await createTemplateAction({
      name: String(formData.get("name") ?? ""),
      institutionName: String(formData.get("institutionName") ?? ""),
      compatibleAccountType: String(
        formData.get("compatibleAccountType") ?? "",
      ) as ImportTemplate["compatibleAccountType"],
      fileKind: String(formData.get("fileKind") ?? "csv") as ImportTemplate["fileKind"],
      sheetName: String(formData.get("sheetName") ?? "") || null,
      headerRowIndex: Number(formData.get("headerRowIndex") ?? 1),
      rowsToSkipBeforeHeader: Number(formData.get("rowsToSkipBeforeHeader") ?? 0),
      rowsToSkipAfterHeader: Number(formData.get("rowsToSkipAfterHeader") ?? 0),
      delimiter: String(formData.get("delimiter") ?? "") || null,
      encoding: String(formData.get("encoding") ?? "") || null,
      decimalSeparator: String(formData.get("decimalSeparator") ?? "") || null,
      thousandsSeparator: String(formData.get("thousandsSeparator") ?? "") || null,
      dateFormat: String(formData.get("dateFormat") ?? "%Y-%m-%d"),
      defaultCurrency: String(formData.get("defaultCurrency") ?? "EUR"),
      columnMappings,
      signMode,
      invertSign,
      directionColumn,
      debitColumn,
      creditColumn,
      debitValuesText,
      creditValuesText,
      dateDayFirst,
      active: true,
    });
    setMessage(`Template saved${result.templateId ? `: ${result.templateId}` : "."}`);
    router.refresh();
  }

  return (
    <div className="form-grid">
      <div className="legend-list" style={{ marginBottom: 20 }}>
        <span className="pill">{templates.length} template(s) configured</span>
        <span className="pill">Map source columns into canonical fields</span>
        <span className="pill">JSON is still stored, but no longer authored directly</span>
      </div>
      <form
        className="form-grid"
        onSubmit={(event) => {
          event.preventDefault();
          const formData = new FormData(event.currentTarget);
          startTransition(() => {
            void handleSubmit(formData).catch((error) => {
              setMessage(error instanceof Error ? error.message : "Template creation failed.");
            });
          });
        }}
      >
        <label className="input-label">
          Template Name
          <input className="input-field" name="name" defaultValue="IBKR equities CSV" required />
        </label>
        <label className="input-label">
          Institution
          <input className="input-field" name="institutionName" defaultValue="Interactive Brokers" required />
        </label>
        <label className="input-label">
          Account Type
          <select className="input-select" name="compatibleAccountType" defaultValue="brokerage_account">
            {accountTypeOptions.map((accountType) => (
              <option key={accountType} value={accountType}>
                {accountType}
              </option>
            ))}
          </select>
        </label>
        <label className="input-label">
          File Kind
          <select className="input-select" name="fileKind" defaultValue="csv">
            {fileKindOptions.map((fileKind) => (
              <option key={fileKind} value={fileKind}>
                {fileKind}
              </option>
            ))}
          </select>
        </label>
        <label className="input-label">
          Worksheet
          <input className="input-field" name="sheetName" placeholder="Optional for xlsx" />
        </label>
        <label className="input-label">
          Header Row
          <input className="input-field" name="headerRowIndex" type="number" min="1" defaultValue="1" />
        </label>
        <label className="input-label">
          Skip Before Header
          <input className="input-field" name="rowsToSkipBeforeHeader" type="number" min="0" defaultValue="0" />
        </label>
        <label className="input-label">
          Skip After Header
          <input className="input-field" name="rowsToSkipAfterHeader" type="number" min="0" defaultValue="0" />
        </label>
        <label className="input-label">
          Date Format
          <input className="input-field" name="dateFormat" defaultValue="%Y-%m-%d" />
        </label>
        <label className="input-label">
          Default Currency
          <input className="input-field" name="defaultCurrency" defaultValue="EUR" />
        </label>
        <label className="input-label">
          Delimiter
          <input className="input-field" name="delimiter" defaultValue="," />
        </label>
        <label className="input-label">
          Encoding
          <input className="input-field" name="encoding" defaultValue="utf-8" />
        </label>
        <label className="input-label">
          Decimal Separator
          <input className="input-field" name="decimalSeparator" defaultValue="." />
        </label>
        <label className="input-label">
          Thousands Separator
          <input className="input-field" name="thousandsSeparator" defaultValue="," />
        </label>

        <div className="builder-panel" style={{ gridColumn: "1 / -1" }}>
          <div>
            <span className="label-sm">Column Mappings</span>
            <h3 className="section-title">Source column to canonical field</h3>
            <p className="builder-copy">Use statement header names or Excel letters. Transaction date is required.</p>
          </div>
          <div className="mapping-list">
            {columnMappings.map((mapping, index) => (
              <div className="mapping-row" key={`${mapping.target}-${index}`}>
                <label className="input-label">
                  Source Column
                  <input
                    className="input-field"
                    value={mapping.source}
                    onChange={(event) =>
                      setColumnMappings((rows) =>
                        updateMappingRow(rows, index, { source: event.target.value }),
                      )
                    }
                    placeholder="Date or A"
                  />
                </label>
                <label className="input-label">
                  Purpose
                  <select
                    className="input-select"
                    value={mapping.target}
                    onChange={(event) =>
                      setColumnMappings((rows) =>
                        updateMappingRow(rows, index, {
                          target: event.target.value as TemplateColumnMapping["target"],
                        }),
                      )
                    }
                  >
                    {canonicalFieldOptions.map((field) => (
                      <option key={field.key} value={field.key}>
                        {field.label}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  className="btn-ghost"
                  type="button"
                  disabled={columnMappings.length === 1}
                  onClick={() =>
                    setColumnMappings((rows) => rows.filter((_, rowIndex) => rowIndex !== index))
                  }
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
          <div className="inline-actions">
            <button
              className="btn-ghost"
              type="button"
              onClick={() =>
                setColumnMappings((rows) => [...rows, { source: "", target: "description_raw" }])
              }
            >
              Add Mapping
            </button>
            <span className="muted">Optional fields can be added only when the source file exposes them.</span>
          </div>
        </div>

        <div className="builder-panel" style={{ gridColumn: "1 / -1" }}>
          <div>
            <span className="label-sm">Amount Parsing</span>
            <h3 className="section-title">How credits and debits are encoded</h3>
          </div>
          <label className="input-label">
            Sign Mode
            <select
              className="input-select"
              value={signMode}
              onChange={(event) => setSignMode(event.target.value as TemplateSignMode)}
            >
              {signModeOptions.map((option) => (
                <option key={option} value={option}>
                  {signModeLabels[option]}
                </option>
              ))}
            </select>
          </label>

          {signMode === "signed_amount" ? (
            <label className="checkbox-row">
              <input
                type="checkbox"
                checked={invertSign}
                onChange={(event) => setInvertSign(event.target.checked)}
              />
              Invert sign after parsing
            </label>
          ) : null}

          {signMode === "amount_direction_column" ? (
            <div className="form-grid">
              <label className="input-label">
                Direction Column
                <input
                  className="input-field"
                  value={directionColumn}
                  onChange={(event) => setDirectionColumn(event.target.value)}
                  placeholder="Debit/Credit"
                />
              </label>
              <label className="input-label">
                Debit Values
                <input
                  className="input-field"
                  value={debitValuesText}
                  onChange={(event) => setDebitValuesText(event.target.value)}
                  placeholder={defaultDirectionValues.debit}
                />
              </label>
              <label className="input-label">
                Credit Values
                <input
                  className="input-field"
                  value={creditValuesText}
                  onChange={(event) => setCreditValuesText(event.target.value)}
                  placeholder={defaultDirectionValues.credit}
                />
              </label>
            </div>
          ) : null}

          {signMode === "debit_credit_columns" ? (
            <div className="form-grid">
              <label className="input-label">
                Debit Column
                <input
                  className="input-field"
                  value={debitColumn}
                  onChange={(event) => setDebitColumn(event.target.value)}
                  placeholder="Debit"
                />
              </label>
              <label className="input-label">
                Credit Column
                <input
                  className="input-field"
                  value={creditColumn}
                  onChange={(event) => setCreditColumn(event.target.value)}
                  placeholder="Credit"
                />
              </label>
            </div>
          ) : null}
        </div>

        <div className="builder-panel" style={{ gridColumn: "1 / -1" }}>
          <div>
            <span className="label-sm">Normalization</span>
            <h3 className="section-title">Date parsing defaults</h3>
          </div>
          <label className="checkbox-row">
            <input
              type="checkbox"
              checked={dateDayFirst}
              onChange={(event) => setDateDayFirst(event.target.checked)}
            />
            Parse ambiguous dates as day-first
          </label>
        </div>

        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <button className="btn-pill" type="submit" disabled={isPending}>
            {isPending ? "Saving..." : "Save Template"}
          </button>
          {message ? <span className="label-sm">{message}</span> : null}
        </div>
      </form>
    </div>
  );
}
