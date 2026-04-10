"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";

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
};

export function CreditCardStatementUploadCell({
  settlementTransactionId,
  statementStatus,
  linkedCreditCardAccountName,
  linkedImportFilename,
  templateOptions,
}: CreditCardStatementUploadCellProps) {
  const router = useRouter();
  const [selectedTemplateId, setSelectedTemplateId] = useState(
    templateOptions[0]?.id ?? NEW_SPREADSHEET_TEMPLATE_ID,
  );
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  if (statementStatus === "not_applicable") {
    return <span className="muted">—</span>;
  }

  if (statementStatus === "uploaded") {
    return (
      <div style={{ display: "grid", gap: 6, minWidth: 220 }}>
        <span className="pill">Statement linked</span>
        {linkedCreditCardAccountName ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            Card ledger: {linkedCreditCardAccountName}
          </span>
        ) : null}
        {linkedImportFilename ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            Imported from {linkedImportFilename}
          </span>
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
      result.rowCountInserted ? `${result.rowCountInserted} statement rows` : null,
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
    setMessage(details || "Statement imported.");
    router.refresh();
  }

  return (
    <div style={{ display: "grid", gap: 8, minWidth: 260 }}>
      <span className="pill warning">Statement needed</span>
      <label className="input-label" style={{ gap: 6 }}>
        Template
        <select
          className="input-select"
          value={selectedTemplateId}
          onChange={(event) => setSelectedTemplateId(event.target.value)}
          disabled={isPending}
        >
          <option value={NEW_SPREADSHEET_TEMPLATE_ID}>
            New spreadsheet (infer template with AI)
          </option>
          {templateOptions.map((template) => (
            <option key={template.id} value={template.id}>
              {template.name}
            </option>
          ))}
        </select>
      </label>
      <label className="input-label" style={{ gap: 6 }}>
        Statement file
        <input
          className="input-field"
          type="file"
          accept=".csv,.xls,.xlsx,.xlsm,.xltx,.xltm"
          disabled={isPending}
          onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)}
        />
      </label>
      <button
        className="btn-pill"
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
        {isPending ? "Importing…" : "Upload statement"}
      </button>
      <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
        The imported statement must net exactly to this settlement payment.
      </span>
      {message ? (
        <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
          {message}
        </span>
      ) : null}
    </div>
  );
}
