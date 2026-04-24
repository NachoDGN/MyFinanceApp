import type {
  FileKind,
  ImportCommitResult,
  ImportFileValidationIssue,
  ImportPreviewResult,
} from "@myfinance/domain";

export type CanonicalImportRow = {
  transaction_date: string;
  posted_date?: string | null;
  description_raw: string;
  amount_original_signed: string;
  currency_original?: string | null;
  balance_original?: string | null;
  external_reference?: string | null;
  transaction_type_raw?: string | null;
  security_isin?: string | null;
  security_symbol?: string | null;
  security_name?: string | null;
  quantity?: string | null;
  unit_price_original?: string | null;
  fees_original?: string | null;
  fx_rate?: string | null;
  raw_row_json?: string | null;
};

export type DeterministicImportResult = (
  | ImportPreviewResult
  | ImportCommitResult
) & {
  normalizedRows?: CanonicalImportRow[];
};

export interface SpreadsheetSheetPreview {
  sheetName: string | null;
  previewCsv: string;
}

export interface SpreadsheetWorkbookPreview {
  fileKind: FileKind;
  delimiter?: string | null;
  encoding?: string | null;
  sheetPreviews: SpreadsheetSheetPreview[];
}

export interface SpreadsheetTablePreview {
  sheetName: string | null;
  previewCsv: string;
  headers: string[];
}

export interface SpreadsheetFileValidationResult {
  fileKind: FileKind;
  issues: ImportFileValidationIssue[];
}
