import { z } from "zod";

import { getProviderRequestConfig } from "./config";
import type {
  GenerateJsonParams,
  GenerateTextParams,
  JsonRepairMode,
  LLMFailureKind,
  LoggerLike,
  LLMProviderName,
} from "./types";
import { LLMUnavailableError, LLMValidationError } from "./types";

class ProviderApiError extends Error {
  constructor(
    message: string,
    readonly statusCode?: number,
    readonly responseBody?: string | null,
  ) {
    super(message);
    this.name = "ProviderApiError";
  }
}

class EmptyTextError extends Error {
  constructor(message = "The model returned an empty text response.") {
    super(message);
    this.name = "EmptyTextError";
  }
}

class JsonOutputError extends Error {
  constructor(
    readonly kind: "malformed_json" | "schema_validation",
    message: string,
    readonly rawOutput: string,
  ) {
    super(message);
    this.name = "JsonOutputError";
  }
}

type ProviderRequest = {
  modelName: string;
  systemPrompt: string;
  userPrompt: string;
  temperature?: number;
  maxTokens?: number;
  responseJsonSchema?: Record<string, unknown>;
  schemaName?: string;
  tools?: Array<Record<string, unknown>>;
  toolChoice?: string;
  include?: string[];
};

interface ProviderAdapter {
  readonly provider: LLMProviderName;
  generateText(request: ProviderRequest): Promise<string>;
  generateJson(request: ProviderRequest): Promise<string>;
}

function getAbortSignal(timeoutMs: number) {
  if (
    typeof AbortSignal !== "undefined" &&
    typeof AbortSignal.timeout === "function"
  ) {
    return AbortSignal.timeout(timeoutMs);
  }

  const controller = new AbortController();
  setTimeout(() => controller.abort(), timeoutMs);
  return controller.signal;
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function truncateForLog(value: string | null | undefined, maxLength = 240) {
  if (!value) return null;
  const collapsed = value.replace(/\s+/g, " ").trim();
  return collapsed.length <= maxLength
    ? collapsed
    : `${collapsed.slice(0, maxLength)}...`;
}

function toErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown LLM failure.";
}

function normalizeStringFragment(value: string) {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\r/g, "\\r")
    .replace(/\n/g, "\\n")
    .replace(/\t/g, "\\t");
}

function extractOuterJson(payload: string) {
  const firstBrace = payload.indexOf("{");
  const lastBrace = payload.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    return payload.slice(firstBrace, lastBrace + 1);
  }

  const firstBracket = payload.indexOf("[");
  const lastBracket = payload.lastIndexOf("]");
  if (firstBracket >= 0 && lastBracket > firstBracket) {
    return payload.slice(firstBracket, lastBracket + 1);
  }

  return payload;
}

function quoteLocaleNumberLiterals(payload: string) {
  return payload.replace(
    /(:\s*|\[\s*|,\s*)([-+]?\d{1,3}(?:\.\d{3})+,\d+|[-+]?\d+,\d+)(?=\s*[,}\]])/g,
    (_, prefix: string, numberLiteral: string) => `${prefix}"${numberLiteral}"`,
  );
}

export function normaliseJsonPayload(
  rawPayload: string,
  options: { allowLocaleNumberStrings?: boolean } = {},
) {
  let normalized = rawPayload
    .replace(/```json/gi, "```")
    .replace(/```/g, "")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .trim();

  normalized = extractOuterJson(normalized);
  normalized = normalized.replace(
    /([{,]\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*:)/g,
    '$1"$2"$3',
  );
  normalized = normalized.replace(
    /([{,]\s*)'([^']+)'(\s*:)/g,
    (_, prefix: string, key: string, suffix: string) => {
      return `${prefix}"${normalizeStringFragment(key)}"${suffix}`;
    },
  );
  normalized = normalized.replace(/:\s*'([^']*)'/g, (_, value: string) => {
    return `: "${normalizeStringFragment(value)}"`;
  });

  if (options.allowLocaleNumberStrings) {
    normalized = quoteLocaleNumberLiterals(normalized);
  }

  return normalized;
}

function parseJsonWithSchema<T>(rawPayload: string, schema: z.ZodType<T>) {
  let parsedJson: unknown;
  try {
    parsedJson = JSON.parse(rawPayload);
  } catch (error) {
    throw new JsonOutputError(
      "malformed_json",
      toErrorMessage(error),
      rawPayload,
    );
  }

  const parsed = schema.safeParse(parsedJson);
  if (!parsed.success) {
    const issueSummary = parsed.error.issues
      .map((issue) => {
        const path = issue.path.join(".");
        return path ? `${path}: ${issue.message}` : issue.message;
      })
      .join("; ");
    throw new JsonOutputError(
      "schema_validation",
      issueSummary || parsed.error.message,
      rawPayload,
    );
  }

  return parsed.data;
}

function extractOpenAIResponseText(payload: Record<string, unknown>) {
  if (typeof payload.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  const output = Array.isArray(payload.output) ? payload.output : [];
  for (const item of output) {
    if (!item || typeof item !== "object") continue;
    const content = Array.isArray((item as { content?: unknown[] }).content)
      ? (item as { content: unknown[] }).content
      : [];
    for (const chunk of content) {
      if (!chunk || typeof chunk !== "object") continue;
      const textValue = (chunk as { text?: unknown }).text;
      if (typeof textValue === "string" && textValue.trim()) {
        return textValue.trim();
      }
    }
  }

  return null;
}

function extractGeminiResponseText(payload: Record<string, unknown>) {
  const candidates = Array.isArray(payload.candidates)
    ? payload.candidates
    : [];
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== "object") continue;
    const content = (candidate as { content?: { parts?: unknown[] } }).content;
    const parts = Array.isArray(content?.parts) ? content.parts : [];
    for (const part of parts) {
      if (!part || typeof part !== "object") continue;
      const text = (part as { text?: unknown }).text;
      if (typeof text === "string" && text.trim()) {
        return text.trim();
      }
    }
  }

  return null;
}

class OpenAIProvider implements ProviderAdapter {
  readonly provider = "openai" as const;

  constructor(
    private readonly apiKey: string,
    private readonly timeoutMs: number,
  ) {}

  private async request(request: ProviderRequest, mode: "text" | "json") {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.apiKey}`,
      },
      signal: getAbortSignal(this.timeoutMs),
      body: JSON.stringify({
        model: request.modelName,
        input: [
          {
            role: "system",
            content: [{ type: "input_text", text: request.systemPrompt }],
          },
          {
            role: "user",
            content: [{ type: "input_text", text: request.userPrompt }],
          },
        ],
        ...(typeof request.temperature === "number"
          ? { temperature: request.temperature }
          : {}),
        ...(typeof request.maxTokens === "number"
          ? { max_output_tokens: request.maxTokens }
          : {}),
        ...(request.tools?.length ? { tools: request.tools } : {}),
        ...(request.toolChoice ? { tool_choice: request.toolChoice } : {}),
        ...(request.include?.length ? { include: request.include } : {}),
        ...(mode === "json"
          ? request.responseJsonSchema
            ? {
                text: {
                  format: {
                    type: "json_schema",
                    name: request.schemaName ?? "structured_output",
                    schema: request.responseJsonSchema,
                    strict: true,
                  },
                },
              }
            : {
                text: {
                  format: {
                    type: "json_object",
                  },
                },
              }
          : {}),
      }),
    });

    if (!response.ok) {
      const responseBody = await response.text();
      throw new ProviderApiError(
        `OpenAI request failed with status ${response.status}.${responseBody ? ` ${truncateForLog(responseBody, 600)}` : ""}`,
        response.status,
        responseBody,
      );
    }

    const payload = (await response.json()) as Record<string, unknown>;
    const text = extractOpenAIResponseText(payload);
    if (!text) {
      throw new EmptyTextError();
    }
    return text;
  }

  generateText(request: ProviderRequest) {
    return this.request(request, "text");
  }

  generateJson(request: ProviderRequest) {
    return this.request(request, "json");
  }
}

class GeminiProvider implements ProviderAdapter {
  readonly provider = "gemini" as const;

  constructor(
    private readonly apiKey: string,
    private readonly timeoutMs: number,
  ) {}

  private async request(request: ProviderRequest, mode: "text" | "json") {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${request.modelName}:generateContent`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": this.apiKey,
        },
        signal: getAbortSignal(this.timeoutMs),
        body: JSON.stringify({
          system_instruction: {
            parts: [{ text: request.systemPrompt }],
          },
          contents: [
            {
              role: "user",
              parts: [{ text: request.userPrompt }],
            },
          ],
          generationConfig: {
            responseMimeType:
              mode === "json" ? "application/json" : "text/plain",
            ...(typeof request.temperature === "number"
              ? { temperature: request.temperature }
              : {}),
            ...(typeof request.maxTokens === "number"
              ? { maxOutputTokens: request.maxTokens }
              : {}),
          },
        }),
      },
    );

    if (!response.ok) {
      const responseBody = await response.text();
      throw new ProviderApiError(
        `Gemini request failed with status ${response.status}.${responseBody ? ` ${truncateForLog(responseBody, 600)}` : ""}`,
        response.status,
        responseBody,
      );
    }

    const payload = (await response.json()) as Record<string, unknown>;
    const text = extractGeminiResponseText(payload);
    if (!text) {
      throw new EmptyTextError();
    }
    return text;
  }

  generateText(request: ProviderRequest) {
    return this.request(request, "text");
  }

  generateJson(request: ProviderRequest) {
    return this.request(request, "json");
  }
}

function createProviderAdapter(modelName: string): ProviderAdapter {
  const { provider, apiKey, timeoutMs } = getProviderRequestConfig(modelName);
  if (provider === "gemini") {
    return new GeminiProvider(apiKey, timeoutMs);
  }
  return new OpenAIProvider(apiKey, timeoutMs);
}

function getRetryDelay(attempt: number) {
  if (attempt <= 1) return 0;
  return 1000 * 2 ** (attempt - 2);
}

export class LLMClient {
  constructor(private readonly logger: LoggerLike = console) {}

  private logFailure(
    provider: LLMProviderName,
    modelName: string,
    attempt: number,
    kind: LLMFailureKind,
    error: unknown,
    rawOutput?: string | null,
  ) {
    const log =
      kind === "api_exception" || kind === "missing_credentials"
        ? this.logger.error
        : this.logger.warn;
    log?.("LLM request failed", {
      provider,
      modelName,
      attempt,
      kind,
      message: toErrorMessage(error),
      sample: truncateForLog(rawOutput),
    });
  }

  async generateText(params: GenerateTextParams) {
    const adapter = createProviderAdapter(params.modelName);
    const maxRetries = Math.max(1, params.maxRetries ?? 3);
    let lastError: unknown = null;
    let lastKind: LLMFailureKind = "api_exception";

    for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
      const delay = getRetryDelay(attempt);
      if (delay > 0) {
        await sleep(delay);
      }

      try {
        return await adapter.generateText({
          modelName: params.modelName,
          systemPrompt: params.systemPrompt,
          userPrompt: params.userPrompt,
          temperature: params.temperature,
          maxTokens: params.maxTokens,
          tools: params.tools,
          toolChoice: params.toolChoice,
          include: params.include,
        });
      } catch (error) {
        lastError = error;
        lastKind =
          error instanceof EmptyTextError ? "empty_text" : "api_exception";
        this.logFailure(
          adapter.provider,
          params.modelName,
          attempt,
          lastKind,
          error,
        );
      }
    }

    throw new LLMUnavailableError(toErrorMessage(lastError), {
      provider: adapter.provider,
      modelName: params.modelName,
      kind: lastKind,
      attempts: maxRetries,
      statusCode:
        lastError instanceof ProviderApiError
          ? lastError.statusCode
          : undefined,
    });
  }

  async generateJson<T>(params: GenerateJsonParams<T>) {
    const adapter = createProviderAdapter(params.modelName);
    const maxRetries = Math.max(1, params.maxRetries ?? 3);
    const repairMode: JsonRepairMode = params.repairMode ?? "json";
    let lastError: unknown = null;
    let lastKind: LLMFailureKind = "api_exception";
    let lastRawOutput: string | null = null;

    for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
      const delay = getRetryDelay(attempt);
      if (delay > 0) {
        await sleep(delay);
      }

      try {
        const rawOutput = await adapter.generateJson({
          modelName: params.modelName,
          systemPrompt: params.systemPrompt,
          userPrompt: params.userPrompt,
          temperature: params.temperature,
          maxTokens: params.maxTokens,
          responseJsonSchema: params.responseJsonSchema,
          schemaName: params.schemaName,
          tools: params.tools,
          toolChoice: params.toolChoice,
          include: params.include,
        });

        try {
          return parseJsonWithSchema(rawOutput, params.responseSchema);
        } catch (error) {
          if (!(error instanceof JsonOutputError)) {
            throw error;
          }

          lastRawOutput = error.rawOutput;

          if (repairMode === "json") {
            const normalizedPayload = normaliseJsonPayload(rawOutput, {
              allowLocaleNumberStrings: params.allowLocaleNumberStrings,
            });
            if (normalizedPayload !== rawOutput) {
              return parseJsonWithSchema(
                normalizedPayload,
                params.responseSchema,
              );
            }
          }

          throw error;
        }
      } catch (error) {
        lastError = error;
        lastKind =
          error instanceof EmptyTextError
            ? "empty_text"
            : error instanceof JsonOutputError
              ? error.kind
              : "api_exception";
        if (error instanceof JsonOutputError) {
          lastRawOutput = error.rawOutput;
        }
        this.logFailure(
          adapter.provider,
          params.modelName,
          attempt,
          lastKind,
          error,
          lastRawOutput,
        );
      }
    }

    if (lastKind === "malformed_json" || lastKind === "schema_validation") {
      throw new LLMValidationError(toErrorMessage(lastError), {
        provider: adapter.provider,
        modelName: params.modelName,
        kind: lastKind,
        attempts: maxRetries,
        rawOutput: lastRawOutput,
      });
    }

    throw new LLMUnavailableError(toErrorMessage(lastError), {
      provider: adapter.provider,
      modelName: params.modelName,
      kind: lastKind,
      attempts: maxRetries,
      statusCode:
        lastError instanceof ProviderApiError
          ? lastError.statusCode
          : undefined,
      rawOutput: lastRawOutput,
    });
  }
}

export function createLLMClient(logger?: LoggerLike) {
  return new LLMClient(logger);
}
