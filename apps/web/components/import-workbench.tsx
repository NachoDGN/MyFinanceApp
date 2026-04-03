"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";

import type { Account, ImportBatch, ImportCommitResult, ImportPreviewResult, ImportTemplate } from "@myfinance/domain";

type ImportResult = ImportPreviewResult | ImportCommitResult;

function isCommitResult(value: ImportResult): value is ImportCommitResult {
  return "importBatchId" in value;
}

export function ImportWorkbench({
  accounts,
  templates,
  importBatches,
}: {
  accounts: Account[];
  templates: ImportTemplate[];
  importBatches: ImportBatch[];
}) {
  const router = useRouter();
  const [selectedAccountId, setSelectedAccountId] = useState(accounts[0]?.id ?? "");
  const [selectedTemplateId, setSelectedTemplateId] = useState(templates[0]?.id ?? "");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  async function submit(mode: "preview" | "commit") {
    if (!selectedFile) {
      throw new Error("Select a file before running the import.");
    }

    const formData = new FormData();
    formData.append("accountId", selectedAccountId);
    formData.append("templateId", selectedTemplateId);
    formData.append("file", selectedFile);

    const response = await fetch(`/api/imports/${mode}`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(errorText || `${mode} import failed.`);
    }

    const payload = (await response.json()) as ImportResult;
    setResult(payload);
    setMessage(
      mode === "commit"
        ? "Import committed. Classification and rebuild jobs were queued."
        : "Preview generated from the uploaded file.",
    );

    if (mode === "commit") {
      router.refresh();
    }
  }

  const latestBatch = importBatches[0];

  return (
    <div className="form-grid">
      <div className="legend-list" style={{ marginBottom: 20 }}>
        <span className="pill">Browser upload only</span>
        <span className="pill">Template-driven normalization</span>
        <span className="pill">
          Last batch: {latestBatch ? `${latestBatch.originalFilename} | ${latestBatch.status}` : "none yet"}
        </span>
      </div>
      <div className="form-grid">
        <label className="input-label">
          Account
          <select
            className="input-select"
            value={selectedAccountId}
            onChange={(event) => setSelectedAccountId(event.target.value)}
          >
            {accounts.map((account) => (
              <option key={account.id} value={account.id}>
                {account.displayName}
              </option>
            ))}
          </select>
        </label>
        <label className="input-label">
          Template
          <select
            className="input-select"
            value={selectedTemplateId}
            onChange={(event) => setSelectedTemplateId(event.target.value)}
          >
            {templates.map((template) => (
              <option key={template.id} value={template.id}>
                {template.name}
              </option>
            ))}
          </select>
        </label>
        <label className="input-label" style={{ gridColumn: "1 / -1" }}>
          Spreadsheet File
          <input
            className="input-field"
            type="file"
            accept=".csv,.xlsx"
            onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)}
          />
        </label>
        <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
          <button
            className="btn-pill"
            type="button"
            disabled={isPending}
            onClick={() =>
              startTransition(() => {
                void submit("preview").catch((error) => {
                  setMessage(error instanceof Error ? error.message : "Preview failed.");
                });
              })
            }
          >
            {isPending ? "Working..." : "Preview Import"}
          </button>
          <button
            className="btn-ghost"
            type="button"
            disabled={isPending}
            onClick={() =>
              startTransition(() => {
                void submit("commit").catch((error) => {
                  setMessage(error instanceof Error ? error.message : "Commit failed.");
                });
              })
            }
          >
            Commit Import
          </button>
          {message ? <span className="label-sm">{message}</span> : null}
        </div>
      </div>

      {result ? (
        <div className="details-section" style={{ marginTop: 24 }}>
          <div className="section-header">
            <div>
              <span className="label-sm">Latest Run</span>
              <h2 className="section-title">
                {result.originalFilename} | {result.rowCountParsed} parsed / {result.rowCountDuplicates} duplicate(s)
              </h2>
            </div>
          </div>
          <div className="legend-list" style={{ marginBottom: 20 }}>
            <span className="pill">Detected rows: {result.rowCountDetected}</span>
            <span className="pill">Failed rows: {result.rowCountFailed}</span>
            <span className="pill">
              Date range: {result.dateRange ? `${result.dateRange.start} -> ${result.dateRange.end}` : "n/a"}
            </span>
            {isCommitResult(result) ? (
              <span className="pill">Inserted: {result.rowCountInserted}</span>
            ) : null}
          </div>
          {result.sampleRows.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table className="data-table">
                <thead>
                  <tr>
                    {Object.keys(result.sampleRows[0] ?? {}).map((key) => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.sampleRows.map((row, index) => (
                    <tr key={`${result.originalFilename}-${index}`}>
                      {Object.keys(result.sampleRows[0] ?? {}).map((key) => (
                        <td key={key}>{String(row[key] ?? "-")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
          {result.parseErrors.length > 0 ? (
            <pre
              style={{
                marginTop: 16,
                padding: 16,
                background: "rgba(12, 18, 28, 0.92)",
                color: "white",
                borderRadius: 16,
                overflowX: "auto",
                fontSize: 12,
              }}
            >
              {JSON.stringify(result.parseErrors, null, 2)}
            </pre>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
