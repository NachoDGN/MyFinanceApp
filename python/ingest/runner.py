#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd


TEXT_JOIN_SEPARATOR = " "
EXCEL_COLUMN_PATTERN = re.compile(r"^[A-Z]+$")


@dataclass
class CanonicalizationResult:
    normalized: pd.DataFrame
    row_count_detected: int
    parse_errors: list[dict[str, Any]]


def normalize_header(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", text)


def is_blank(value: Any) -> bool:
    return normalize_text(value) == ""


def column_letter_to_index(column_letter: str) -> int:
    result = 0
    for character in column_letter:
        result = result * 26 + (ord(character) - ord("A") + 1)
    return result - 1


def resolve_column_name(frame: pd.DataFrame, spec: Any) -> Any | None:
    if spec is None:
        return None

    if isinstance(spec, float) and spec.is_integer():
        spec = int(spec)

    if isinstance(spec, int):
        if 0 <= spec < len(frame.columns):
            return frame.columns[spec]
        if 1 <= spec <= len(frame.columns):
            return frame.columns[spec - 1]
        return None

    candidate = normalize_text(spec)
    if not candidate:
        return None

    if candidate in frame.columns:
        return candidate

    normalized_candidate = normalize_header(candidate)
    header_lookup = {normalize_header(column): column for column in frame.columns}
    if normalized_candidate in header_lookup:
        return header_lookup[normalized_candidate]

    if candidate.isdigit():
        numeric = int(candidate)
        if 1 <= numeric <= len(frame.columns):
            return frame.columns[numeric - 1]
        if 0 <= numeric < len(frame.columns):
            return frame.columns[numeric]

    if EXCEL_COLUMN_PATTERN.fullmatch(candidate.upper()):
        index = column_letter_to_index(candidate.upper())
        if 0 <= index < len(frame.columns):
            return frame.columns[index]

    return None


def resolve_series(frame: pd.DataFrame, spec: Any) -> pd.Series | None:
    if spec is None:
        return None

    if isinstance(spec, list):
        resolved_columns = [resolve_column_name(frame, item) for item in spec]
        resolved_columns = [column for column in resolved_columns if column is not None]
        if not resolved_columns:
            return None
        return frame[resolved_columns].apply(
            lambda row: TEXT_JOIN_SEPARATOR.join(
                [piece for piece in (normalize_text(value) for value in row.tolist()) if piece]
            ).strip(),
            axis=1,
        )

    resolved_name = resolve_column_name(frame, spec)
    if resolved_name is None:
        return None
    return frame[resolved_name]


def decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def sanitize_numeric_text(text: str, decimal_hint: str | None, thousands_hint: str | None) -> str:
    clean = text.strip().replace("\u00a0", "").replace(" ", "")
    negative = clean.startswith("(") and clean.endswith(")")
    if negative:
        clean = clean[1:-1]

    clean = re.sub(r"[^\d,.\-+]", "", clean)

    if thousands_hint:
        clean = clean.replace(thousands_hint, "")
    if decimal_hint and decimal_hint != ".":
        clean = clean.replace(decimal_hint, ".")

    if not decimal_hint:
        if "," in clean and "." in clean:
            if clean.rfind(",") > clean.rfind("."):
                clean = clean.replace(".", "").replace(",", ".")
            else:
                clean = clean.replace(",", "")
        elif "," in clean:
            head, tail = clean.rsplit(",", 1)
            clean = f"{head.replace(',', '')}.{tail}" if len(tail) in (1, 2) else clean.replace(",", "")
        elif "." in clean:
            head, tail = clean.rsplit(".", 1)
            clean = f"{head.replace('.', '')}.{tail}" if len(tail) in (1, 2) else clean.replace(".", "")

    if negative and not clean.startswith("-"):
        clean = f"-{clean}"

    return clean


def parse_decimal_value(
    value: Any,
    decimal_hint: str | None = None,
    thousands_hint: str | None = None,
) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))

    text = normalize_text(value)
    if not text:
        return None

    sanitized = sanitize_numeric_text(text, decimal_hint, thousands_hint)
    if not sanitized or sanitized in {"-", "+", ".", ","}:
        return None

    try:
        return Decimal(sanitized)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid numeric value: {text}") from exc


def parse_date_value(
    value: Any,
    *,
    format_hint: str | None,
    dayfirst: bool,
) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = normalize_text(value)
    if not text:
        return None

    parsed = pd.NaT
    if format_hint:
        parsed = pd.to_datetime(text, format=format_hint, errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, dayfirst=dayfirst, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid date value: {text}")
    return parsed.date().isoformat()


def to_serializable(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return decimal_to_string(value)
    if pd.isna(value):
        return None
    return value


def load_dataframe(file_path: Path, template: dict[str, Any]) -> pd.DataFrame:
    file_kind = str(template.get("file_kind", file_path.suffix.lstrip("."))).lower()
    header_row_index = int(template.get("header_row_index", 1))
    skip_before_header = int(template.get("rows_to_skip_before_header", 0))
    rows_to_skip_after_header = int(template.get("rows_to_skip_after_header", 0))
    header_zero_index = max(header_row_index - 1 - skip_before_header, 0)

    if file_kind == "csv":
        frame = pd.read_csv(
            file_path,
            delimiter=template.get("delimiter", ","),
            encoding=template.get("encoding", "utf-8"),
            skiprows=skip_before_header,
            header=header_zero_index,
            dtype=object,
        )
    elif file_kind == "xlsx":
        frame = pd.read_excel(
            file_path,
            sheet_name=template.get("sheet_name") or 0,
            skiprows=skip_before_header,
            header=header_zero_index,
            dtype=object,
        )
    else:
        raise ValueError(f"Unsupported file kind: {file_kind}")

    if rows_to_skip_after_header > 0:
        frame = frame.iloc[rows_to_skip_after_header:]

    return frame.reset_index(drop=True)


def resolve_amount_series(
    frame: pd.DataFrame,
    column_map: dict[str, Any],
    sign_logic: dict[str, Any],
    *,
    decimal_hint: str | None,
    thousands_hint: str | None,
) -> pd.Series:
    mode = str(sign_logic.get("mode", "signed_amount"))

    if mode == "debit_credit_columns":
        debit_series = resolve_series(
            frame,
            sign_logic.get("debit_column", column_map.get("debit_amount")),
        )
        credit_series = resolve_series(
            frame,
            sign_logic.get("credit_column", column_map.get("credit_amount")),
        )
        if debit_series is None and credit_series is None:
            raise ValueError("Template sign logic requires debit and/or credit columns.")

        return pd.Series(
            [
                decimal_to_string(
                    (
                        parse_decimal_value(
                            credit,
                            decimal_hint=decimal_hint,
                            thousands_hint=thousands_hint,
                        )
                        or Decimal("0")
                    )
                    - (
                        parse_decimal_value(
                            debit,
                            decimal_hint=decimal_hint,
                            thousands_hint=thousands_hint,
                        )
                        or Decimal("0")
                    )
                )
                for debit, credit in zip(
                    debit_series.tolist() if debit_series is not None else [None] * len(frame),
                    credit_series.tolist() if credit_series is not None else [None] * len(frame),
                    strict=False,
                )
            ]
        )

    amount_series = resolve_series(
        frame,
        sign_logic.get("amount_column", column_map.get("amount_original_signed")),
    )
    if amount_series is None:
        raise ValueError("Template is missing an amount column mapping.")

    if mode == "amount_direction_column":
        direction_series = resolve_series(frame, sign_logic.get("direction_column"))
        if direction_series is None:
            raise ValueError("Template sign logic requires a direction column.")

        debit_values = {
            normalize_text(value).upper()
            for value in (sign_logic.get("debit_values") or ["debit", "out", "sell", "withdrawal"])
        }
        credit_values = {
            normalize_text(value).upper()
            for value in (sign_logic.get("credit_values") or ["credit", "in", "buy", "deposit"])
        }

        def resolve_directional_amount(amount_value: Any, direction_value: Any) -> str | None:
            amount = parse_decimal_value(
                amount_value,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
            if amount is None:
                return None
            direction = normalize_text(direction_value).upper()
            if direction in debit_values:
                amount = -abs(amount)
            elif direction in credit_values:
                amount = abs(amount)
            return decimal_to_string(amount)

        return pd.Series(
            [
                resolve_directional_amount(amount_value, direction_value)
                for amount_value, direction_value in zip(
                    amount_series.tolist(),
                    direction_series.tolist(),
                    strict=False,
                )
            ]
        )

    invert_sign = bool(sign_logic.get("invert_sign", False))
    return amount_series.apply(
        lambda value: decimal_to_string(
            -(
                parse_decimal_value(
                    value,
                    decimal_hint=decimal_hint,
                    thousands_hint=thousands_hint,
                )
                or Decimal("0")
            )
            if invert_sign
            else parse_decimal_value(
                value,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
        )
    )


def canonicalize_frame(frame: pd.DataFrame, template: dict[str, Any]) -> CanonicalizationResult:
    column_map = template.get("column_map_json", {}) or {}
    sign_logic = template.get("sign_logic_json", {}) or {}
    normalization_rules = template.get("normalization_rules_json", {}) or {}
    default_currency = str(template.get("default_currency", "EUR")).upper()
    date_format = template.get("date_format")
    decimal_hint = template.get("decimal_separator") or None
    thousands_hint = template.get("thousands_separator") or None
    dayfirst = bool(normalization_rules.get("date_day_first", True))

    detected_rows = 0
    normalized_rows: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    source_row_offset = int(template.get("header_row_index", 1)) + int(
        template.get("rows_to_skip_after_header", 0)
    )

    if frame.empty:
        return CanonicalizationResult(
            normalized=pd.DataFrame(normalized_rows),
            row_count_detected=0,
            parse_errors=parse_errors,
        )

    amount_series = resolve_amount_series(
        frame,
        column_map,
        sign_logic,
        decimal_hint=decimal_hint,
        thousands_hint=thousands_hint,
    )
    transaction_date_series = resolve_series(frame, column_map.get("transaction_date"))
    if transaction_date_series is None:
        raise ValueError("Template is missing a transaction_date mapping.")

    description_series = resolve_series(frame, column_map.get("description_raw"))
    posted_date_series = resolve_series(frame, column_map.get("posted_date"))
    currency_series = resolve_series(frame, column_map.get("currency_original"))
    balance_series = resolve_series(frame, column_map.get("balance_original"))
    external_reference_series = resolve_series(frame, column_map.get("external_reference"))
    transaction_type_series = resolve_series(frame, column_map.get("transaction_type_raw"))
    security_symbol_series = resolve_series(frame, column_map.get("security_symbol"))
    security_name_series = resolve_series(frame, column_map.get("security_name"))
    quantity_series = resolve_series(frame, column_map.get("quantity"))
    unit_price_series = resolve_series(frame, column_map.get("unit_price_original"))
    fees_series = resolve_series(frame, column_map.get("fees_original"))
    fx_rate_series = resolve_series(frame, column_map.get("fx_rate"))

    for index, (_, row) in enumerate(frame.iterrows()):
        raw_row = {str(column): to_serializable(value) for column, value in row.to_dict().items()}
        if all(is_blank(value) for value in raw_row.values()):
            continue

        detected_rows += 1
        source_row = source_row_offset + index + 1

        try:
            transaction_date = parse_date_value(
                transaction_date_series.iloc[index],
                format_hint=date_format,
                dayfirst=dayfirst,
            )
            if not transaction_date:
                raise ValueError("Missing transaction date.")

            posted_date = parse_date_value(
                posted_date_series.iloc[index] if posted_date_series is not None else None,
                format_hint=date_format,
                dayfirst=dayfirst,
            )
            amount = parse_decimal_value(
                amount_series.iloc[index],
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
            if amount is None:
                raise ValueError("Missing amount.")

            description = normalize_text(description_series.iloc[index] if description_series is not None else None)
            transaction_type_raw = normalize_text(
                transaction_type_series.iloc[index] if transaction_type_series is not None else None
            )
            security_symbol = normalize_text(
                security_symbol_series.iloc[index] if security_symbol_series is not None else None
            )
            security_name = normalize_text(
                security_name_series.iloc[index] if security_name_series is not None else None
            )
            external_reference = normalize_text(
                external_reference_series.iloc[index] if external_reference_series is not None else None
            )

            if not description:
                description = TEXT_JOIN_SEPARATOR.join(
                    [piece for piece in [transaction_type_raw, security_symbol, security_name, external_reference] if piece]
                ).strip()
            if not description:
                raise ValueError("Missing description.")

            quantity = parse_decimal_value(
                quantity_series.iloc[index] if quantity_series is not None else None,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
            unit_price = parse_decimal_value(
                unit_price_series.iloc[index] if unit_price_series is not None else None,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
            balance = parse_decimal_value(
                balance_series.iloc[index] if balance_series is not None else None,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
            fees = parse_decimal_value(
                fees_series.iloc[index] if fees_series is not None else None,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )
            fx_rate = parse_decimal_value(
                fx_rate_series.iloc[index] if fx_rate_series is not None else None,
                decimal_hint=decimal_hint,
                thousands_hint=thousands_hint,
            )

            normalized_rows.append(
                {
                    "transaction_date": transaction_date,
                    "posted_date": posted_date,
                    "description_raw": description,
                    "amount_original_signed": decimal_to_string(amount),
                    "currency_original": normalize_text(
                        currency_series.iloc[index] if currency_series is not None else default_currency
                    ).upper()
                    or default_currency,
                    "balance_original": decimal_to_string(balance),
                    "external_reference": external_reference or None,
                    "transaction_type_raw": transaction_type_raw or None,
                    "security_symbol": security_symbol or None,
                    "security_name": security_name or None,
                    "quantity": decimal_to_string(quantity),
                    "unit_price_original": decimal_to_string(unit_price),
                    "fees_original": decimal_to_string(fees),
                    "fx_rate": decimal_to_string(fx_rate),
                    "raw_row_json": json.dumps(
                        {
                            **raw_row,
                            "_source_row": source_row,
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                }
            )
        except Exception as exc:
            parse_errors.append({"row": source_row, "message": str(exc)})

    normalized = pd.DataFrame(normalized_rows)
    return CanonicalizationResult(
        normalized=normalized,
        row_count_detected=detected_rows,
        parse_errors=parse_errors,
    )


def build_result(
    mode: str,
    account_id: str,
    template_id: str,
    filename: str,
    canonical: CanonicalizationResult,
) -> dict[str, Any]:
    normalized = canonical.normalized
    records = normalized.to_dict(orient="records")
    summary: dict[str, Any] = {
        "schemaVersion": "v1",
        "accountId": account_id,
        "templateId": template_id,
        "originalFilename": filename,
        "rowCountDetected": canonical.row_count_detected,
        "rowCountParsed": len(records),
        "rowCountDuplicates": 0,
        "rowCountFailed": len(canonical.parse_errors),
        "dateRange": {
            "start": min((row["transaction_date"] for row in records), default=None),
            "end": max((row["transaction_date"] for row in records), default=None),
        }
        if records
        else None,
        "normalizedRows": records,
        "sampleRows": records[:5],
        "parseErrors": canonical.parse_errors,
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
    parser = argparse.ArgumentParser(description="Template-driven pandas ingest wrapper")
    parser.add_argument("mode", choices=["preview", "commit"])
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--template-id", required=True)
    parser.add_argument("--template-json", required=True)
    args = parser.parse_args()

    file_path = Path(args.file_path)
    template = json.loads(args.template_json)
    frame = load_dataframe(file_path, template)
    canonical = canonicalize_frame(frame, template)
    result = build_result(
        args.mode,
        args.account_id,
        args.template_id,
        file_path.name,
        canonical,
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
