import type { LLMProviderName } from "./types";

export type ProviderApiErrorPayload = Record<string, unknown>;

function isRecord(value: unknown): value is ProviderApiErrorPayload {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function formatProviderName(provider: LLMProviderName) {
  return provider === "openai" ? "OpenAI" : "Gemini";
}

function readProviderErrorNode(payload: ProviderApiErrorPayload | null) {
  const error = payload?.error;
  return isRecord(error) ? error : null;
}

function readProviderErrorMessage(error: ProviderApiErrorPayload | null) {
  return typeof error?.message === "string" && error.message.trim()
    ? error.message.trim()
    : null;
}

function readProviderErrorStatus(error: ProviderApiErrorPayload | null) {
  return typeof error?.status === "string" && error.status.trim()
    ? error.status.trim()
    : null;
}

function readProviderErrorCode(error: ProviderApiErrorPayload | null) {
  const code = error?.code;
  if (typeof code === "number" && Number.isFinite(code)) {
    return code;
  }
  if (typeof code === "string" && code.trim()) {
    return code.trim();
  }
  return null;
}

export function truncateForLog(
  value: string | null | undefined,
  maxLength = 240,
) {
  if (!value) return null;
  const collapsed = value.replace(/\s+/g, " ").trim();
  return collapsed.length <= maxLength
    ? collapsed
    : `${collapsed.slice(0, maxLength)}...`;
}

export function parseProviderResponseJson(
  responseBody: string | null | undefined,
) {
  if (!responseBody) {
    return null;
  }

  try {
    const parsed = JSON.parse(responseBody) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function buildProviderApiErrorMessage(input: {
  provider: LLMProviderName;
  statusCode?: number;
  responseBody?: string | null;
  providerError: ProviderApiErrorPayload | null;
}) {
  const prefix = `${formatProviderName(input.provider)} request failed with status ${input.statusCode ?? "unknown"}.`;
  const providerMessage = readProviderErrorMessage(input.providerError);
  if (providerMessage) {
    const qualifiers: string[] = [];
    const providerStatus = readProviderErrorStatus(input.providerError);
    const providerCode = readProviderErrorCode(input.providerError);
    if (providerStatus) {
      qualifiers.push(providerStatus);
    }
    if (
      providerCode !== null &&
      `${providerCode}` !== `${input.statusCode ?? ""}`
    ) {
      qualifiers.push(`code ${providerCode}`);
    }
    return qualifiers.length > 0
      ? `${prefix} ${qualifiers.join(", ")}: ${providerMessage}`
      : `${prefix} ${providerMessage}`;
  }

  const responseSummary = truncateForLog(input.responseBody, 600);
  return responseSummary ? `${prefix} ${responseSummary}` : prefix;
}

export class ProviderApiError extends Error {
  readonly provider: LLMProviderName;
  readonly statusCode?: number;
  readonly responseBody: string | null;
  readonly responseJson: ProviderApiErrorPayload | null;
  readonly providerError: ProviderApiErrorPayload | null;
  readonly providerErrorCode: number | string | null;
  readonly providerErrorStatus: string | null;

  constructor(input: {
    provider: LLMProviderName;
    statusCode?: number;
    responseBody?: string | null;
  }) {
    const responseJson = parseProviderResponseJson(input.responseBody);
    const providerError = readProviderErrorNode(responseJson);
    super(
      buildProviderApiErrorMessage({
        provider: input.provider,
        statusCode: input.statusCode,
        responseBody: input.responseBody,
        providerError,
      }),
    );
    this.name = "ProviderApiError";
    this.provider = input.provider;
    this.statusCode = input.statusCode;
    this.responseBody = input.responseBody ?? null;
    this.responseJson = responseJson;
    this.providerError = providerError;
    this.providerErrorCode = readProviderErrorCode(providerError);
    this.providerErrorStatus = readProviderErrorStatus(providerError);
  }
}
