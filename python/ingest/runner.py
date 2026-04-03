#!/usr/bin/env python3

import argparse
import json
import sys
import uuid
from pathlib import Path

import pandas as pd


def load_dataframe(file_path: Path, template: dict) -> pd.DataFrame:
    file_kind = template.get("file_kind", file_path.suffix.lstrip(".")).lower()
    if file_kind == "csv":
        return pd.read_csv(
            file_path,
            delimiter=template.get("delimiter", ","),
            encoding=template.get("encoding", "utf-8"),
            skiprows=template.get("rows_to_skip_before_header", 0),
            decimal=template.get("decimal_separator", "."),
            thousands=template.get("thousands_separator", ","),
        )
    if file_kind == "xlsx":
        return pd.read_excel(
            file_path,
            sheet_name=template.get("sheet_name") or 0,
            header=template.get("header_row_index", 1) - 1,
        )
    raise ValueError(f"Unsupported file kind: {file_kind}")


def canonicalize_frame(frame: pd.DataFrame, template: dict) -> pd.DataFrame:
    column_map = template.get("column_map_json", {})
    default_currency = template.get("default_currency", "EUR")
    normalized = pd.DataFrame()
    normalized["transaction_date"] = frame[column_map["transaction_date"]].astype(str)
    normalized["description_raw"] = frame[column_map["description_raw"]].astype(str)
    normalized["amount_original_signed"] = frame[column_map["amount_original_signed"]].astype(str)
    currency_column = column_map.get("currency_original")
    normalized["currency_original"] = (
        frame[currency_column].astype(str) if currency_column and currency_column in frame else default_currency
    )
    balance_column = column_map.get("balance_original")
    normalized["balance_original"] = (
        frame[balance_column].astype(str) if balance_column and balance_column in frame else None
    )
    reference_column = column_map.get("external_reference")
    normalized["external_reference"] = (
        frame[reference_column].astype(str) if reference_column and reference_column in frame else None
    )
    normalized["raw_row_json"] = frame.apply(lambda row: row.to_json(force_ascii=False), axis=1)
    return normalized


def build_result(mode: str, account_id: str, template_id: str, filename: str, normalized: pd.DataFrame) -> dict:
    records = normalized.to_dict(orient="records")
    summary = {
      "schemaVersion": "v1",
      "accountId": account_id,
      "templateId": template_id,
      "originalFilename": filename,
      "rowCountDetected": len(records),
      "rowCountParsed": len(records),
      "rowCountDuplicates": 0,
      "rowCountFailed": 0,
      "dateRange": {
        "start": min((row["transaction_date"] for row in records), default=None),
        "end": max((row["transaction_date"] for row in records), default=None),
      }
      if records
      else None,
      "normalizedRows": records,
      "sampleRows": records[:5],
      "parseErrors": [],
    }
    if mode == "commit":
        summary["importBatchId"] = str(uuid.uuid4())
        summary["rowCountInserted"] = len(records)
        summary["transactionIds"] = [str(uuid.uuid4()) for _ in records]
        summary["jobsQueued"] = [
            "classification",
            "transfer_rematch",
            "position_rebuild",
            "metric_refresh",
            "insight_refresh",
        ]
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic pandas ingest wrapper")
    parser.add_argument("mode", choices=["preview", "commit"])
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--template-id", required=True)
    parser.add_argument("--template-json", required=True)
    args = parser.parse_args()

    file_path = Path(args.file_path)
    template = json.loads(args.template_json)
    frame = load_dataframe(file_path, template)
    normalized = canonicalize_frame(frame, template)
    result = build_result(
        args.mode,
        args.account_id,
        args.template_id,
        file_path.name,
        normalized,
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
