const BLOCKED_SQL_KEYWORDS =
  /\b(insert|update|delete|drop|alter|create|grant|revoke|copy|truncate|comment|merge|call|do|execute|vacuum|analyze)\b/iu;

const ALLOWED_LEDGER_RELATIONS = new Set([
  "agent_ledger_accounts",
  "agent_ledger_audit_events",
  "agent_ledger_categories",
  "agent_ledger_entities",
  "agent_ledger_fx_rates",
  "agent_ledger_import_batches",
  "agent_ledger_search_rows",
  "agent_ledger_transactions",
]);

function stripSqlComments(query: string) {
  return query
    .replace(/--.*$/gmu, "")
    .replace(/\/\*[\s\S]*?\*\//gu, "")
    .trim();
}

function hasTopLevelLimit(query: string) {
  return /\blimit\s+\d+\s*$/iu.test(query);
}

function extractRelationNames(query: string) {
  const relationNames = new Set<string>();
  const relationPattern =
    /\b(?:from|join)\s+((?:public\.)?[a-zA-Z_][a-zA-Z0-9_]*)/giu;
  let match: RegExpExecArray | null;

  while ((match = relationPattern.exec(query))) {
    const relationName = match[1].split(".").at(-1)?.toLowerCase();
    if (relationName) {
      relationNames.add(relationName);
    }
  }

  return [...relationNames];
}

function extractCteNames(query: string) {
  if (!/^\s*with\b/iu.test(query)) {
    return new Set<string>();
  }

  const cteNames = new Set<string>();
  const ctePattern = /(?:with|,)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s*\(/giu;
  let match: RegExpExecArray | null;
  while ((match = ctePattern.exec(query))) {
    cteNames.add(match[1].toLowerCase());
  }
  return cteNames;
}

export function sanitizeReadOnlyLedgerSql(
  query: string,
  options: { limit?: number } = {},
) {
  const limit = Math.max(1, Math.min(options.limit ?? 25, 100));
  const trimmed = stripSqlComments(query).replace(/;+$/u, "").trim();
  const lowered = trimmed.toLowerCase();

  if (!trimmed) {
    throw new Error("SQL query is required.");
  }

  if (!/^(select|with)\b/u.test(lowered)) {
    throw new Error("SQL tool only accepts SELECT or WITH queries.");
  }

  if (/[;]/u.test(trimmed)) {
    throw new Error("Only a single SQL statement is allowed.");
  }

  if (BLOCKED_SQL_KEYWORDS.test(lowered)) {
    throw new Error("SQL tool received a blocked mutating keyword.");
  }

  const relationNames = extractRelationNames(trimmed);
  const cteNames = extractCteNames(trimmed);
  const blockedRelation = relationNames.find(
    (relationName) =>
      !ALLOWED_LEDGER_RELATIONS.has(relationName) &&
      !cteNames.has(relationName),
  );
  if (blockedRelation) {
    throw new Error(
      `SQL tool can only read agent ledger views, not ${blockedRelation}.`,
    );
  }

  if (hasTopLevelLimit(trimmed)) {
    return trimmed;
  }

  return `${trimmed}\nlimit ${limit}`;
}

export function normalizeAgentSearchQuery(query: string) {
  return query
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

export function isDuplicateAgentSearchQuery(
  query: string,
  previousQueries: readonly string[],
) {
  const normalizedQuery = normalizeAgentSearchQuery(query);
  if (!normalizedQuery) {
    return false;
  }

  return previousQueries.some(
    (candidate) => normalizeAgentSearchQuery(candidate) === normalizedQuery,
  );
}
