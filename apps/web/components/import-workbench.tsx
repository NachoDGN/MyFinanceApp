"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState, useTransition } from "react";

import type {
  Account,
  ImportBatch,
  ImportCommitResult,
  ImportPreviewResult,
  ImportTemplate,
} from "@myfinance/domain";
import { commitImportAction, previewImportAction } from "../app/actions";
import { NEW_SPREADSHEET_TEMPLATE_ID } from "../app/import-constants";

type ImportResult = ImportPreviewResult | ImportCommitResult;
type TemplateOption = Pick<ImportTemplate, "id" | "name">;

function isCommitResult(value: ImportResult): value is ImportCommitResult {
  return "importBatchId" in value;
}

function PreviewTable({
  headers,
  rows,
}: {
  headers: string[];
  rows: Array<Record<string, unknown>>;
}) {
  if (headers.length === 0 || rows.length === 0) {
    return null;
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <table className="data-table">
        <thead>
          <tr>
            {headers.map((header) => (
              <th key={header}>{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`preview-row-${index}`}>
              {headers.map((header) => (
                <td key={`${header}-${index}`}>{String(row[header] ?? "-")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
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
  const [availableTemplates, setAvailableTemplates] = useState<
    TemplateOption[]
  >(() => templates.map(({ id, name }) => ({ id, name })));
  const [selectedAccountId, setSelectedAccountId] = useState(
    accounts[0]?.id ?? "",
  );
  const [selectedTemplateId, setSelectedTemplateId] = useState(
    templates[0]?.id ?? NEW_SPREADSHEET_TEMPLATE_ID,
  );
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    setAvailableTemplates(templates.map(({ id, name }) => ({ id, name })));
  }, [templates]);

  async function submit(mode: "preview" | "commit") {
    if (!selectedFile) {
      throw new Error("Select a file before running the import.");
    }

    const usedNewTemplateSelection =
      selectedTemplateId === NEW_SPREADSHEET_TEMPLATE_ID;
    const formData = new FormData();
    formData.append("accountId", selectedAccountId);
    formData.append("templateId", selectedTemplateId);
    formData.append("file", selectedFile);

    const payload = (await (mode === "commit"
      ? commitImportAction(formData)
      : previewImportAction(formData))) as ImportResult;
    setResult(payload);
    if (usedNewTemplateSelection && payload.resolvedTemplateName) {
      const resolvedTemplateName = payload.resolvedTemplateName;
      setAvailableTemplates((current) =>
        current.some((template) => template.id === payload.templateId)
          ? current
          : [
              ...current,
              { id: payload.templateId, name: resolvedTemplateName },
            ],
      );
      setSelectedTemplateId(payload.templateId);
    }
    setMessage(
      mode === "commit"
        ? usedNewTemplateSelection && payload.resolvedTemplateName
          ? `Import committed. Template saved as ${payload.resolvedTemplateName}. Classification and rebuild jobs were queued.`
          : "Import committed. Classification and rebuild jobs were queued."
        : usedNewTemplateSelection && payload.resolvedTemplateName
          ? `Preview generated and template saved as ${payload.resolvedTemplateName}.`
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
          Use New spreadsheet to infer and save a template with AI
        </span>
        <span className="pill">
          Last batch:{" "}
          {latestBatch
            ? `${latestBatch.originalFilename} | ${latestBatch.status}`
            : "none yet"}
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
            <option value={NEW_SPREADSHEET_TEMPLATE_ID}>
              New spreadsheet (infer template with AI)
            </option>
            {availableTemplates.map((template) => (
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
            onChange={(event) =>
              setSelectedFile(event.target.files?.[0] ?? null)
            }
          />
        </label>
        <div
          style={{
            display: "flex",
            gap: 12,
            alignItems: "center",
            flexWrap: "wrap",
          }}
        >
          <button
            className="btn-pill"
            type="button"
            disabled={isPending}
            onClick={() =>
              startTransition(() => {
                void submit("preview").catch((error) => {
                  setMessage(
                    error instanceof Error ? error.message : "Preview failed.",
                  );
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
                  setMessage(
                    error instanceof Error ? error.message : "Commit failed.",
                  );
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
                {result.originalFilename} | {result.rowCountParsed} parsed /{" "}
                {result.rowCountDuplicates} duplicate(s)
              </h2>
            </div>
          </div>
          <div className="legend-list" style={{ marginBottom: 20 }}>
            <span className="pill">
              Detected rows: {result.rowCountDetected}
            </span>
            <span className="pill">Failed rows: {result.rowCountFailed}</span>
            <span className="pill">
              Date range:{" "}
              {result.dateRange
                ? `${result.dateRange.start} -> ${result.dateRange.end}`
                : "n/a"}
            </span>
            {isCommitResult(result) ? (
              <span className="pill">Inserted: {result.rowCountInserted}</span>
            ) : null}
          </div>
          {result.sourceTablePreview &&
          result.sourceTablePreview.rows.length > 0 ? (
            <div style={{ marginBottom: 20 }}>
              <span className="label-sm">Detected Source Table</span>
              <h3
                className="section-title"
                style={{ marginTop: 8, marginBottom: 12 }}
              >
                Spreadsheet slice used for parsing
              </h3>
              <PreviewTable
                headers={result.sourceTablePreview.headers}
                rows={result.sourceTablePreview.rows}
              />
            </div>
          ) : null}
          {result.sampleRows.length > 0 ? (
            <div>
              <span className="label-sm">Canonical Preview</span>
              <h3
                className="section-title"
                style={{ marginTop: 8, marginBottom: 12 }}
              >
                Normalized rows that will be ingested
              </h3>
              <PreviewTable
                headers={Object.keys(result.sampleRows[0] ?? {})}
                rows={result.sampleRows}
              />
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
