"use client";

import { useRouter } from "next/navigation";
import { useRef, useState, useTransition } from "react";

import { commitCreditCardStatementImportAction } from "../app/actions";
import { NEW_SPREADSHEET_TEMPLATE_ID } from "../app/import-constants";

type TemplateOption = {
  id: string;
  name: string;
};

type CreditCardStatementUploadCellProps = {
  settlementTransactionId: string;
  statementStatus: "not_applicable" | "upload_required" | "uploaded";
  linkedCreditCardAccountName?: string | null;
  linkedImportFilename?: string | null;
  templateOptions: TemplateOption[];
  variant?: "default" | "statement";
};

export function CreditCardStatementUploadCell({
  settlementTransactionId,
  statementStatus,
  linkedCreditCardAccountName,
  linkedImportFilename,
  templateOptions,
  variant = "default",
}: CreditCardStatementUploadCellProps) {
  const router = useRouter();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [selectedTemplateId, setSelectedTemplateId] = useState(
    NEW_SPREADSHEET_TEMPLATE_ID,
  );
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const panelClassName =
    variant === "statement"
      ? "statement-upload-panel"
      : "statement-upload-panel statement-upload-panel-compact";

  if (statementStatus === "not_applicable") {
    return <span className="muted">—</span>;
  }

  if (statementStatus === "uploaded") {
    return (
      <div className={panelClassName}>
        <span className="statement-alert statement-alert-success">
          Statement linked
        </span>
        {linkedCreditCardAccountName ? (
          <p className="statement-helper-text">
            Card ledger: {linkedCreditCardAccountName}
          </p>
        ) : null}
        {linkedImportFilename ? (
          <p className="statement-helper-text">
            Imported from {linkedImportFilename}
          </p>
        ) : null}
      </div>
    );
  }

  async function handleSubmit() {
    if (!selectedFile) {
      setMessage("Choose the full credit-card statement first.");
      return;
    }

    setMessage(null);
    const formData = new FormData();
    formData.append("settlementTransactionId", settlementTransactionId);
    formData.append("templateId", selectedTemplateId);
    formData.append("file", selectedFile);

    const result = (await commitCreditCardStatementImportAction(formData)) as {
      linkedCreditCardAccountName?: string;
      rowCountInserted?: number;
      statementNetAmountBaseEur?: string;
      resolvedTemplateName?: string;
    };

    const details = [
      result.rowCountInserted
        ? `${result.rowCountInserted} statement rows`
        : null,
      result.statementNetAmountBaseEur
        ? `${Number(result.statementNetAmountBaseEur).toFixed(2)} EUR validated`
        : null,
      result.linkedCreditCardAccountName
        ? `linked to ${result.linkedCreditCardAccountName}`
        : null,
      result.resolvedTemplateName
        ? `template saved as ${result.resolvedTemplateName}`
        : null,
    ]
      .filter(Boolean)
      .join(" · ");

    setSelectedFile(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
    setMessage(details || "Statement imported.");
    router.refresh();
  }

  return (
    <div className={panelClassName}>
      <span className="statement-alert statement-alert-warning">
        Statement needed
      </span>
      <label className="statement-field">
        <span className="statement-field-label">Template</span>
        <div className="statement-select-shell">
          <select
            className="statement-select"
            value={selectedTemplateId}
            onChange={(event) => setSelectedTemplateId(event.target.value)}
            disabled={isPending}
          >
            <option value={NEW_SPREADSHEET_TEMPLATE_ID}>
              New statement parser with AI
            </option>
            {templateOptions.map((template) => (
              <option key={template.id} value={template.id}>
                {template.name}
              </option>
            ))}
          </select>
        </div>
      </label>

      <div className="statement-field">
        <span className="statement-field-label">Statement file</span>
        <input
          ref={fileInputRef}
          className="statement-hidden-file-input"
          type="file"
          accept=".csv,.xls,.xlsx,.xlsm,.xltx,.xltm,.pdf"
          disabled={isPending}
          onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)}
        />
        <div className="statement-file-picker">
          <button
            className="statement-file-button"
            type="button"
            disabled={isPending}
            onClick={() => fileInputRef.current?.click()}
          >
            Choose File
          </button>
          <span className="statement-file-name">
            {selectedFile?.name ?? "No file chosen"}
          </span>
        </div>
      </div>

      <button
        className="statement-primary-button"
        type="button"
        disabled={isPending || !selectedFile}
        onClick={() =>
          startTransition(() => {
            void handleSubmit().catch((error) => {
              setMessage(
                error instanceof Error
                  ? error.message
                  : "Credit-card statement import failed.",
              );
            });
          })
        }
      >
        {isPending ? "Uploading…" : "Upload statement"}
      </button>

      <p className="statement-helper-text">
        The imported statement must net exactly to this settlement payment.
      </p>
      {message ? <p className="statement-helper-text">{message}</p> : null}
    </div>
  );
}
