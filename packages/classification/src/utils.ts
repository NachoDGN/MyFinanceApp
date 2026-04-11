export function normalizeOptionalText(value: string | null | undefined) {
  const text = value?.trim() ?? "";
  return text || null;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function readOptionalBoolean(value: unknown) {
  return typeof value === "boolean" ? value : null;
}

export function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

export function readOptionalRecord(value: unknown) {
  return isRecord(value) ? value : null;
}

export function readUnknownArray(value: unknown) {
  return Array.isArray(value) ? value : null;
}

function camelizeJsonKey(value: string) {
  return value.replace(/_([a-z])/g, (_, character: string) =>
    character.toUpperCase(),
  );
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

  const camelizedKey = camelizeJsonKey(key);
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
