"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";

import type { ImportTemplate } from "@myfinance/domain";

const defaultColumnMap = `{
  "transaction_date": "Date",
  "posted_date": "Posted Date",
  "description_raw": "Description",
  "amount_original_signed": "Amount",
  "currency_original": "Currency",
  "external_reference": "Reference",
  "balance_original": "Balance",
  "security_symbol": "Symbol",
  "security_name": "Security",
  "quantity": "Quantity",
  "unit_price_original": "Price",
  "transaction_type_raw": "Type"
}`;

const defaultSignLogic = `{
  "mode": "signed_amount"
}`;

const defaultNormalizationRules = `{
  "date_day_first": true
}`;

export function TemplateWorkbench({ templates }: { templates: ImportTemplate[] }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<string | null>(null);

  async function handleSubmit(formData: FormData) {
    setMessage(null);

    const payload = {
      template: {
        name: String(formData.get("name") ?? ""),
        institutionName: String(formData.get("institutionName") ?? ""),
        compatibleAccountType: String(formData.get("compatibleAccountType") ?? ""),
        fileKind: String(formData.get("fileKind") ?? "csv"),
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
        columnMapJson: JSON.parse(String(formData.get("columnMapJson") ?? "{}")),
        signLogicJson: JSON.parse(String(formData.get("signLogicJson") ?? "{}")),
        normalizationRulesJson: JSON.parse(String(formData.get("normalizationRulesJson") ?? "{}")),
        active: true,
      },
      apply: true,
    };

    const response = await fetch("/api/templates", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(errorText || "Template creation failed.");
    }

    const result = (await response.json()) as { templateId?: string };
    setMessage(`Template saved${result.templateId ? `: ${result.templateId}` : "."}`);
    router.refresh();
  }

  return (
    <div className="form-grid">
      <div className="legend-list" style={{ marginBottom: 20 }}>
        <span className="pill">{templates.length} template(s) configured</span>
        <span className="pill">Cash + investment mappings supported</span>
        <span className="pill">Column values can be header names or Excel letters</span>
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
            <option value="checking">checking</option>
            <option value="savings">savings</option>
            <option value="company_bank">company_bank</option>
            <option value="brokerage_cash">brokerage_cash</option>
            <option value="brokerage_account">brokerage_account</option>
            <option value="credit_card">credit_card</option>
            <option value="other">other</option>
          </select>
        </label>
        <label className="input-label">
          File Kind
          <select className="input-select" name="fileKind" defaultValue="csv">
            <option value="csv">csv</option>
            <option value="xlsx">xlsx</option>
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
        <label className="input-label" style={{ gridColumn: "1 / -1" }}>
          Column Map JSON
          <textarea className="input-textarea" name="columnMapJson" defaultValue={defaultColumnMap} rows={14} />
        </label>
        <label className="input-label" style={{ gridColumn: "1 / -1" }}>
          Sign Logic JSON
          <textarea className="input-textarea" name="signLogicJson" defaultValue={defaultSignLogic} rows={4} />
        </label>
        <label className="input-label" style={{ gridColumn: "1 / -1" }}>
          Normalization Rules JSON
          <textarea
            className="input-textarea"
            name="normalizationRulesJson"
            defaultValue={defaultNormalizationRules}
            rows={4}
          />
        </label>
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
