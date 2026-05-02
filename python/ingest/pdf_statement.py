from __future__ import annotations

import base64
import json
import os
import re
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any


ADE_PARSE_EU_URL = "https://api.va.eu-west-1.landing.ai/v1/ade/parse"
DEFAULT_ADE_MODEL = "dpt-2-mini"
DEFAULT_GEMINI_MODEL = (
    os.getenv("LLM_PDF_STATEMENT_MODEL")
    or os.getenv("GEMINI_PDF_STATEMENT_MODEL")
    or os.getenv("LLM_TRANSACTION_MODEL")
    or "gemini-3-flash-preview"
)
PDF_MAX_VISION_PAGES = 10
PDF_RENDER_DPI = 200
PDF_JPEG_QUALITY = 85
MAX_IMAGE_PIXELS_FOR_API = 20_000_000
RETRY_DELAYS_SECONDS = (1, 2, 4)

OCR_FALLBACK_PROMPT = """You are a document OCR and layout-preserving transcription assistant.
Read all provided page images and return a faithful markdown transcription.
Do not summarize. Do not classify. Do not infer missing text.
Preserve headings, lists, tables, labels, and reading order as closely as possible.
Return only markdown."""


@dataclass
class PdfTextExtractionResult:
    markdown: str
    method: str


@dataclass
class PdfStatementParseResult:
    rows: list[dict[str, Any]]
    markdown: str
    extraction_method: str
    parser_model: str
    statement_net_total: str | None
    portfolio_statement_snapshot: dict[str, Any] | None = None


def _read_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _extract_outer_json(value: str) -> str:
    text = value.strip()
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        return text[first_brace : last_brace + 1]

    first_bracket = text.find("[")
    last_bracket = text.rfind("]")
    if first_bracket >= 0 and last_bracket > first_bracket:
        return text[first_bracket : last_bracket + 1]

    return text


def _normalise_json_payload(value: str) -> str:
    text = (
        value.replace("```json", "```")
        .replace("```", "")
        .replace("“", '"')
        .replace("”", '"')
        .replace("‘", "'")
        .replace("’", "'")
        .strip()
    )
    text = _extract_outer_json(text)
    text = re.sub(r'([{,]\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*:)', r'\1"\2"\3', text)
    text = re.sub(
        r"([{,]\s*)'([^']+)'(\s*:)",
        lambda match: f'{match.group(1)}"{match.group(2)}"{match.group(3)}',
        text,
    )
    text = re.sub(
        r":\s*'([^']*)'",
        lambda match: ': "' + match.group(1).replace('"', '\\"') + '"',
        text,
    )
    return text


def _parse_json_payload(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = json.loads(_normalise_json_payload(value))

    if not isinstance(parsed, dict):
        raise RuntimeError("The PDF statement parser did not return a JSON object.")

    return parsed


def _extract_gemini_text(payload: dict[str, Any]) -> str | None:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return None

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()

    return None


def _dump_markdown(markdown: str, document_path: str, provider: str) -> None:
    dump_setting = (os.getenv("MYFINANCE_DUMP_PDF_TEXT", "1") or "1").strip().lower()
    if dump_setting in {"0", "false", "no", "off"}:
        return

    dump_dir = Path("Logs") / "ade_dumps"
    dump_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(document_path).name)
    dump_path = dump_dir / f"{safe_name}.{provider}.md"
    dump_path.write_text(markdown, encoding="utf-8")


def ade_parse_document(
    document_path: str,
    model: str = DEFAULT_ADE_MODEL,
    split: str | None = None,
    api_key: str | None = None,
) -> dict[str, Any]:
    import requests

    resolved_api_key = (
        api_key or _read_env("ADE_API_KEY") or _read_env("api_key") or ""
    ).strip()
    if not resolved_api_key:
        raise RuntimeError("ADE_API_KEY is required to parse PDF statements with ADE.")

    headers = {"Authorization": f"Bearer {resolved_api_key}"}
    data: dict[str, Any] = {"model": model}
    if split:
        data["split"] = split

    with open(document_path, "rb") as document_file:
        response = requests.post(
            ADE_PARSE_EU_URL,
            headers=headers,
            data=data,
            files={"document": document_file},
            timeout=120,
        )

    if response.status_code != 200:
        raise RuntimeError(f"ADE API HTTP {response.status_code}: {response.text[:500]}")

    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"ADE API error: {payload.get('error')}")
    if payload.get("status") == "error":
        raise RuntimeError(
            f"ADE API error: {payload.get('message', 'Unknown error')}",
        )

    return payload


def pdf_to_base64_images(
    pdf_path: str,
    *,
    max_pages: int = PDF_MAX_VISION_PAGES,
    dpi: int = PDF_RENDER_DPI,
    jpeg_quality: int = PDF_JPEG_QUALITY,
) -> list[str]:
    from pdf2image import convert_from_path
    from PIL import Image

    original_max_pixels = Image.MAX_IMAGE_PIXELS
    Image.MAX_IMAGE_PIXELS = None

    try:
        pages = convert_from_path(
            pdf_path,
            dpi=dpi,
            first_page=1,
            last_page=max_pages,
        )
        encoded_images: list[str] = []
        for page in pages:
            rgb_page = page.convert("RGB")
            pixel_count = rgb_page.width * rgb_page.height
            if pixel_count > MAX_IMAGE_PIXELS_FOR_API:
                scale = (MAX_IMAGE_PIXELS_FOR_API / pixel_count) ** 0.5
                new_size = (
                    max(1, int(rgb_page.width * scale)),
                    max(1, int(rgb_page.height * scale)),
                )
                resampling = getattr(Image, "Resampling", Image).LANCZOS
                rgb_page = rgb_page.resize(new_size, resampling)
            buffer = BytesIO()
            rgb_page.save(buffer, format="JPEG", quality=jpeg_quality)
            encoded_images.append(base64.b64encode(buffer.getvalue()).decode("utf-8"))
        return encoded_images
    finally:
        Image.MAX_IMAGE_PIXELS = original_max_pixels


def _call_gemini_generate_content(
    *,
    parts: list[dict[str, Any]],
    system_prompt: str,
    response_mime_type: str,
    model_name: str,
    temperature: float = 0.1,
    max_output_tokens: int = 32768,
) -> str:
    import requests

    api_key = _read_env("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required for Gemini PDF parsing.")

    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model_name}:generateContent"
    )
    request_body = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [
            {
                "role": "user",
                "parts": parts,
            }
        ],
        "generationConfig": {
            "responseMimeType": response_mime_type,
            "temperature": temperature,
            "maxOutputTokens": max_output_tokens,
        },
    }

    last_error: str | None = None
    for attempt, delay in enumerate(RETRY_DELAYS_SECONDS, start=1):
        try:
            response = requests.post(
                endpoint,
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                },
                json=request_body,
                timeout=120,
            )
            if response.status_code != 200:
                last_error = f"Gemini API HTTP {response.status_code}: {response.text[:500]}"
                raise RuntimeError(last_error)

            payload = response.json()
            text = _extract_gemini_text(payload)
            if text:
                return text

            last_error = "Gemini returned an empty response body."
            raise RuntimeError(last_error)
        except Exception as exc:
            last_error = str(exc)
            if attempt == len(RETRY_DELAYS_SECONDS):
                break
            time.sleep(delay)

    raise RuntimeError(last_error or "Gemini PDF parsing failed.")


def gemini_extract_text_from_images(
    images: list[str],
    *,
    model_name: str = DEFAULT_GEMINI_MODEL,
) -> str:
    parts: list[dict[str, Any]] = [{"text": OCR_FALLBACK_PROMPT}]
    for encoded_image in images:
        parts.append(
            {
                "inlineData": {
                    "mimeType": "image/jpeg",
                    "data": encoded_image,
                }
            }
        )

    return _call_gemini_generate_content(
        parts=parts,
        system_prompt="Return only markdown.",
        response_mime_type="text/plain",
        model_name=model_name,
        temperature=0,
    )


def extract_text_from_pdf(
    pdf_path: str,
    *,
    ade_model: str = DEFAULT_ADE_MODEL,
    gemini_model: str = DEFAULT_GEMINI_MODEL,
) -> PdfTextExtractionResult:
    ade_error: str | None = None
    try:
        payload = ade_parse_document(pdf_path, model=ade_model)
        markdown = str(payload.get("markdown", "") or "").strip()
        if markdown:
            _dump_markdown(markdown, pdf_path, "ade")
            return PdfTextExtractionResult(markdown=markdown, method="ade")
        ade_error = "ADE returned empty markdown."
    except Exception as exc:
        ade_error = str(exc)

    images = pdf_to_base64_images(
        pdf_path,
        max_pages=PDF_MAX_VISION_PAGES,
        dpi=PDF_RENDER_DPI,
        jpeg_quality=PDF_JPEG_QUALITY,
    )
    if not images:
        raise RuntimeError(
            f"ADE parsing failed ({ade_error or 'unknown error'}) and the PDF could not be rendered for Gemini fallback.",
        )

    markdown = gemini_extract_text_from_images(images, model_name=gemini_model).strip()
    if not markdown:
        raise RuntimeError(
            f"ADE parsing failed ({ade_error or 'unknown error'}) and Gemini fallback returned empty text.",
        )

    _dump_markdown(markdown, pdf_path, "gemini_vision")
    return PdfTextExtractionResult(markdown=markdown, method="gemini_vision")


def extract_credit_card_statement_rows(
    pdf_path: str,
    *,
    default_currency: str = "EUR",
    reference_date: str | None = None,
    model_name: str = DEFAULT_GEMINI_MODEL,
) -> PdfStatementParseResult:
    extraction = extract_text_from_pdf(pdf_path, gemini_model=model_name)
    system_prompt = """You extract individual card-statement ledger rows from markdown.
Return only JSON.

Required JSON shape:
{
  "transactions": [
    {
      "transaction_date": "YYYY-MM-DD",
      "posted_date": null,
      "description_raw": "Merchant or narrative",
      "amount_original_signed": "-12.34",
      "currency_original": "EUR",
      "balance_original": null,
      "external_reference": null,
      "transaction_type_raw": null
    }
  ],
  "statement_net_total": "-123.45"
}

Rules:
- Include only individual transaction rows that make up the billed statement amount.
- Exclude account summaries, previous balances, monthly payment settlements, opening balances, closing balances, minimum due blocks, and duplicated totals.
- Purchases, fees, interest, and cash withdrawals are negative signed amounts.
- Refunds, charge reversals, and credits are positive signed amounts.
- Preserve useful merchant wording in description_raw.
- Use ISO dates. If the statement shows both operation and posting dates, put them in transaction_date and posted_date.
- If currency is missing on a row, use the provided default currency.
- statement_net_total should equal the net sum of the returned rows whenever the statement shows enough information.
"""
    user_prompt = (
        f"Reference date: {reference_date or 'unknown'}\n"
        f"Default currency: {default_currency}\n\n"
        "Extract the transaction rows from this credit-card statement markdown:\n\n"
        f"{extraction.markdown}"
    )
    payload = _call_gemini_generate_content(
        parts=[{"text": user_prompt}],
        system_prompt=system_prompt,
        response_mime_type="application/json",
        model_name=model_name,
        temperature=0,
    )
    parsed = _parse_json_payload(payload)
    transactions = parsed.get("transactions")
    if not isinstance(transactions, list):
        raise RuntimeError("The PDF statement parser did not return a transactions array.")

    normalized_rows: list[dict[str, Any]] = []
    for transaction in transactions:
        if not isinstance(transaction, dict):
            continue
        normalized_rows.append(
            {
                "transaction_date": str(transaction.get("transaction_date", "") or "").strip(),
                "posted_date": str(transaction.get("posted_date", "") or "").strip() or None,
                "description_raw": str(transaction.get("description_raw", "") or "").strip(),
                "amount_original_signed": str(
                    transaction.get("amount_original_signed", "") or ""
                ).strip(),
                "currency_original": str(
                    transaction.get("currency_original", default_currency) or default_currency
                )
                .strip()
                .upper()
                or default_currency,
                "balance_original": str(transaction.get("balance_original", "") or "").strip()
                or None,
                "external_reference": str(
                    transaction.get("external_reference", "") or ""
                ).strip()
                or None,
                "transaction_type_raw": str(
                    transaction.get("transaction_type_raw", "") or ""
                ).strip()
                or None,
            }
        )

    if not normalized_rows:
        raise RuntimeError("The PDF statement parser did not produce any transaction rows.")

    statement_net_total = str(parsed.get("statement_net_total", "") or "").strip() or None
    return PdfStatementParseResult(
        rows=normalized_rows,
        markdown=extraction.markdown,
        extraction_method=extraction.method,
        parser_model=model_name,
        statement_net_total=statement_net_total,
    )


def extract_interactive_brokers_statement_rows(
    pdf_path: str,
    *,
    default_currency: str = "EUR",
    reference_date: str | None = None,
    model_name: str = DEFAULT_GEMINI_MODEL,
) -> PdfStatementParseResult:
    extraction = extract_text_from_pdf(pdf_path, gemini_model=model_name)
    system_prompt = """You extract transaction ledger rows from Interactive Brokers activity statement markdown.
Return only JSON.

Required JSON shape:
{
  "transactions": [
    {
      "transaction_date": "YYYY-MM-DD",
      "posted_date": null,
      "description_raw": "Provider narrative",
      "amount_original_signed": "-12.34",
      "currency_original": "EUR",
      "balance_original": null,
      "external_reference": null,
      "transaction_type_raw": "Trade",
      "security_isin": null,
      "security_symbol": null,
      "security_name": null,
      "quantity": null,
      "unit_price_original": null,
      "fees_original": null,
      "fx_rate": null
    }
  ],
  "statement_net_total": null,
  "portfolio_statement_snapshot": {
    "broker_name": "Interactive Brokers Ireland Limited",
    "account_number": "U0000000",
    "statement_date": "YYYY-MM-DD",
    "period_start": "YYYY-MM-DD",
    "period_end": "YYYY-MM-DD",
    "generated_at": "ISO-8601 timestamp or null",
    "base_currency": "EUR",
    "net_asset_value": "123.45",
    "cash_balance": "12.34",
    "dividend_accruals": "0.00",
    "cash_balance_including_accruals": "12.34",
    "open_positions": [
      {
        "symbol": "HY9H",
        "security_name": "SK HYNIX INC-GDS",
        "isin": "US78392B1070",
        "conid": "517397504",
        "exchange": "FWB2",
        "asset_type": "stock",
        "currency": "EUR",
        "quantity": "9",
        "cost_price": "380.555555556",
        "cost_basis": "3425",
        "close_price": "760",
        "market_value": "6840",
        "unrealized_pnl": "3415"
      }
    ]
  }
}

Rules:
- Include only individual ledger events: trades, deposits, withdrawals, dividends, interest, fees, withholding tax, corporate-action cash rows, and FX conversions.
- Exclude NAV summaries, Cash Report summary lines, Mark-to-Market summaries, Realized/Unrealized summaries, Open Positions, Financial Instrument Information, Base Currency Exchange Rate tables, code legends, notes, totals, and subtotal rows.
- If the statement has no individual ledger events for the statement period, return an empty transactions array instead of inventing rows from summaries or positions.
- Sign amounts from the account cash perspective: deposits, dividends, interest, sell proceeds, and credits are positive; buys, withdrawals, fees, commissions, and taxes are negative.
- For trades, include security_symbol, security_name, security_isin when shown, quantity, unit_price_original, and fees_original when available.
- Use ISO dates. If the statement has trade and settle dates, put trade date in transaction_date and settlement date in posted_date.
- If currency is missing on a row, use the provided default currency.
- Always extract the portfolio_statement_snapshot from the account summary,
  Cash Report, Net Asset Value, Open Positions, and Financial Instrument
  Information sections when present, even when there are no transaction rows.
- Use null for missing snapshot values and an empty open_positions array when
  no open positions are shown.
"""
    user_prompt = (
        f"Reference date: {reference_date or 'unknown'}\n"
        f"Default currency: {default_currency}\n\n"
        "Extract individual transaction ledger rows from this Interactive Brokers "
        "activity statement markdown:\n\n"
        f"{extraction.markdown}"
    )
    payload = _call_gemini_generate_content(
        parts=[{"text": user_prompt}],
        system_prompt=system_prompt,
        response_mime_type="application/json",
        model_name=model_name,
        temperature=0,
    )
    parsed = _parse_json_payload(payload)
    transactions = parsed.get("transactions")
    if not isinstance(transactions, list):
        raise RuntimeError("The IBKR PDF parser did not return a transactions array.")
    portfolio_statement_snapshot = parsed.get("portfolio_statement_snapshot")
    if not isinstance(portfolio_statement_snapshot, dict):
        portfolio_statement_snapshot = None

    normalized_rows: list[dict[str, Any]] = []
    for transaction in transactions:
        if not isinstance(transaction, dict):
            continue
        normalized_rows.append(
            {
                "transaction_date": str(transaction.get("transaction_date", "") or "").strip(),
                "posted_date": str(transaction.get("posted_date", "") or "").strip() or None,
                "description_raw": str(transaction.get("description_raw", "") or "").strip(),
                "amount_original_signed": str(
                    transaction.get("amount_original_signed", "") or ""
                ).strip(),
                "currency_original": str(
                    transaction.get("currency_original", default_currency) or default_currency
                )
                .strip()
                .upper()
                or default_currency,
                "balance_original": str(transaction.get("balance_original", "") or "").strip()
                or None,
                "external_reference": str(
                    transaction.get("external_reference", "") or ""
                ).strip()
                or None,
                "transaction_type_raw": str(
                    transaction.get("transaction_type_raw", "") or ""
                ).strip()
                or None,
                "security_isin": str(transaction.get("security_isin", "") or "").strip()
                or None,
                "security_symbol": str(transaction.get("security_symbol", "") or "").strip()
                or None,
                "security_name": str(transaction.get("security_name", "") or "").strip()
                or None,
                "quantity": str(transaction.get("quantity", "") or "").strip() or None,
                "unit_price_original": str(
                    transaction.get("unit_price_original", "") or ""
                ).strip()
                or None,
                "fees_original": str(transaction.get("fees_original", "") or "").strip()
                or None,
                "fx_rate": str(transaction.get("fx_rate", "") or "").strip() or None,
            }
        )

    statement_net_total = str(parsed.get("statement_net_total", "") or "").strip() or None
    return PdfStatementParseResult(
        rows=normalized_rows,
        markdown=extraction.markdown,
        extraction_method=extraction.method,
        parser_model=model_name,
        statement_net_total=statement_net_total,
        portfolio_statement_snapshot=portfolio_statement_snapshot,
    )
