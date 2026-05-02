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

export type PortfolioStatementPosition = {
  symbol?: string | null;
  security_symbol?: string | null;
  securityName?: string | null;
  security_name?: string | null;
  isin?: string | null;
  conid?: string | null;
  exchange?: string | null;
  assetType?: string | null;
  asset_type?: string | null;
  currency?: string | null;
  quantity?: string | null;
  costPrice?: string | null;
  cost_price?: string | null;
  costBasis?: string | null;
  cost_basis?: string | null;
  closePrice?: string | null;
  close_price?: string | null;
  marketValue?: string | null;
  market_value?: string | null;
  unrealizedPnl?: string | null;
  unrealized_pnl?: string | null;
};

export type PortfolioStatementSnapshot = {
  brokerName?: string | null;
  broker_name?: string | null;
  accountNumber?: string | null;
  account_number?: string | null;
  statementDate?: string | null;
  statement_date?: string | null;
  periodStart?: string | null;
  period_start?: string | null;
  periodEnd?: string | null;
  period_end?: string | null;
  generatedAt?: string | null;
  generated_at?: string | null;
  baseCurrency?: string | null;
  base_currency?: string | null;
  netAssetValue?: string | null;
  net_asset_value?: string | null;
  cashBalance?: string | null;
  cash_balance?: string | null;
  dividendAccruals?: string | null;
  dividend_accruals?: string | null;
  cashBalanceIncludingAccruals?: string | null;
  cash_balance_including_accruals?: string | null;
  openPositions?: PortfolioStatementPosition[];
  open_positions?: PortfolioStatementPosition[];
};

export type DeterministicImportResult = (
  | ImportPreviewResult
  | ImportCommitResult
) & {
  normalizedRows?: CanonicalImportRow[];
  portfolioStatementSnapshot?: PortfolioStatementSnapshot | null;
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
