import { LLMUnavailableError, type LLMProviderName } from "./types";

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const OPENAI_PREFIXES = ["gpt-", "o1", "o3", "o4", "chatgpt", "ft:gpt", "ft:o"];

function readEnv(name: string) {
  const value = process.env[name]?.trim();
  return value ? value : null;
}

export function resolveModelProvider(modelName: string): LLMProviderName {
  const normalized = modelName.trim().toLowerCase();
  if (normalized.startsWith("gemini")) {
    return "gemini";
  }
  if (OPENAI_PREFIXES.some((prefix) => normalized.startsWith(prefix))) {
    return "openai";
  }
  return "openai";
}

export function maskSecret(secret: string | null | undefined) {
  if (!secret) return "missing";
  if (secret.length <= 8) {
    return `${secret.slice(0, 2)}***`;
  }
  return `${secret.slice(0, 4)}...${secret.slice(-4)}`;
}

export function getRequestTimeoutMs() {
  const parsed = Number(process.env.LLM_REQUEST_TIMEOUT_MS ?? `${DEFAULT_REQUEST_TIMEOUT_MS}`);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_REQUEST_TIMEOUT_MS;
}

export function getProviderApiKey(provider: LLMProviderName) {
  if (provider === "gemini") {
    return readEnv("GEMINI_API_KEY");
  }
  return readEnv("OPENAI_API_KEY");
}

export function isModelConfigured(modelName: string) {
  return Boolean(getProviderApiKey(resolveModelProvider(modelName)));
}

export function getProviderRequestConfig(modelName: string) {
  const provider = resolveModelProvider(modelName);
  const apiKey = getProviderApiKey(provider);
  if (!apiKey) {
    throw new LLMUnavailableError(
      `${provider.toUpperCase()} credentials are not configured for model ${modelName}.`,
      {
        provider,
        modelName,
        kind: "missing_credentials",
        attempts: 0,
      },
    );
  }

  return {
    provider,
    apiKey,
    timeoutMs: getRequestTimeoutMs(),
  };
}
