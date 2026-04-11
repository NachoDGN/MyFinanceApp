export function normalizeUppercaseText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

export function normalizeSecurityText(value: string | null | undefined) {
  return normalizeUppercaseText(
    String(value ?? "")
      .trim()
      .replace(/\s+/g, " "),
  );
}

export function normalizeSecurityIdentifier(
  value: string | null | undefined,
) {
  return normalizeSecurityText(value).replace(/\s+/g, "");
}

export function normalizeDescription(input: string): {
  raw: string;
  clean: string;
  comparison: string;
} {
  const clean = input
    .trim()
    .replace(/\s+/g, " ")
    .replace(/\bSEPA\b/gi, "")
    .replace(/\bCARD ENDING \d{4}\b/gi, "")
    .trim();

  return {
    raw: input,
    clean,
    comparison: clean.toUpperCase(),
  };
}

export function normalizeMatcherText(value: string) {
  return normalizeUppercaseText(value);
}

export function extractIsinFromText(
  ...values: Array<string | null | undefined>
) {
  const isinPattern = /\b[A-Z]{2}[A-Z0-9]{9}\d\b/i;
  for (const value of values) {
    const match = String(value ?? "").toUpperCase().match(isinPattern);
    if (match?.[0]) {
      return normalizeSecurityIdentifier(match[0]);
    }
  }
  return null;
}

export function normalizeInvestmentMatchingText(
  value: string | null | undefined,
) {
  return normalizeDescription(value ?? "")
    .comparison.replace(/\bGLOB\b/g, "GLOBAL")
    .replace(/\bSM[\s-]?CAP\b/g, "SMALL CAP")
    .replace(/\bSMALLCAP\b/g, "SMALL CAP")
    .replace(/\bIDX\b/g, "INDEX")
    .replace(/\bU\s*S\b/g, "US")
    .replace(/\bS\s*&\s*P\b/g, "SP")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
