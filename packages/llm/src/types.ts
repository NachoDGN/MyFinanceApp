import type { ZodType } from "zod";

export type LLMProviderName = "openai" | "gemini";
export type LLMFailureKind =
  | "api_exception"
  | "empty_text"
  | "malformed_json"
  | "schema_validation"
  | "missing_credentials";
export type JsonRepairMode = "off" | "json";

export interface LoggerLike {
  info?: (message: string, metadata?: Record<string, unknown>) => void;
  warn?: (message: string, metadata?: Record<string, unknown>) => void;
  error?: (message: string, metadata?: Record<string, unknown>) => void;
}

export interface GenerateTextParams {
  systemPrompt: string;
  userPrompt: string;
  modelName: string;
  maxRetries?: number;
  temperature?: number;
  maxTokens?: number;
}

export interface GenerateJsonParams<T> extends GenerateTextParams {
  responseSchema: ZodType<T>;
  responseJsonSchema?: Record<string, unknown>;
  schemaName?: string;
  repairMode?: JsonRepairMode;
  allowLocaleNumberStrings?: boolean;
}

export interface TextLLM {
  generateText(params: GenerateTextParams): Promise<string>;
}

export interface StructuredLLM {
  generateJson<T>(params: GenerateJsonParams<T>): Promise<T>;
}

export type LLMTaskClient = TextLLM & StructuredLLM;

export interface LLMErrorMetadata {
  provider: LLMProviderName;
  modelName: string;
  kind: LLMFailureKind;
  attempts: number;
  statusCode?: number;
  rawOutput?: string | null;
}

export class LLMError extends Error {
  readonly provider: LLMProviderName;
  readonly modelName: string;
  readonly kind: LLMFailureKind;
  readonly attempts: number;
  readonly statusCode?: number;
  readonly rawOutput?: string | null;

  constructor(message: string, metadata: LLMErrorMetadata) {
    super(message);
    this.name = "LLMError";
    this.provider = metadata.provider;
    this.modelName = metadata.modelName;
    this.kind = metadata.kind;
    this.attempts = metadata.attempts;
    this.statusCode = metadata.statusCode;
    this.rawOutput = metadata.rawOutput ?? null;
  }
}

export class LLMUnavailableError extends LLMError {
  constructor(message: string, metadata: LLMErrorMetadata) {
    super(message, metadata);
    this.name = "LLMUnavailableError";
  }
}

export class LLMValidationError extends LLMError {
  constructor(message: string, metadata: LLMErrorMetadata) {
    super(message, metadata);
    this.name = "LLMValidationError";
  }
}
