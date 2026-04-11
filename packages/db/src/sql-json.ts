import type { SqlClient } from "./sql-runtime";

const DATE_ONLY_KEYS = new Set([
  "openingBalanceDate",
  "transactionDate",
  "postedDate",
  "asOfDate",
  "priceDate",
  "effectiveDate",
  "lastTradeDate",
  "snapshotDate",
  "month",
]);

function camelizeKey(value: string) {
  return value.replace(/_([a-z])/g, (_, character: string) =>
    character.toUpperCase(),
  );
}

export function camelizeValue<T>(value: T, key?: string): T {
  if (Array.isArray(value)) {
    return value.map((item) => camelizeValue(item, key)) as T;
  }
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Date) {
    const iso = value.toISOString();
    return (key && DATE_ONLY_KEYS.has(key) ? iso.slice(0, 10) : iso) as T;
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(
        ([rawKey, nested]) => {
          const nextKey = camelizeKey(rawKey);
          return [nextKey, camelizeValue(nested, nextKey)];
        },
      ),
    ) as T;
  }
  return value;
}

export function mapFromSql<T>(value: unknown): T {
  return camelizeValue(value as T);
}

export function serializeJson(sql: SqlClient, value: unknown) {
  return sql.json((value ?? {}) as Parameters<SqlClient["json"]>[0]);
}

export function parseJsonColumn<T>(value: unknown): T {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return value as T;
    }
  }
  return value as T;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function readOptionalRecord(value: unknown) {
  return isRecord(value) ? value : null;
}

export function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

export function readOptionalNumberAsString(value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? String(value)
    : null;
}

export function readRawOutputField(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  if (!rawOutput) {
    return null;
  }

  if (key in rawOutput) {
    return rawOutput[key];
  }

  const camelizedKey = camelizeKey(key);
  if (camelizedKey in rawOutput) {
    return rawOutput[camelizedKey];
  }

  return null;
}

export function readRawOutputString(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  return readOptionalString(readRawOutputField(rawOutput, key));
}

export function readRawOutputNumberAsString(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  const value = readRawOutputField(rawOutput, key);
  return readOptionalNumberAsString(value) ?? readOptionalString(value);
}
