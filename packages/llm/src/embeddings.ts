import { getProviderApiKey, getRequestTimeoutMs } from "./config";
import {
  parseProviderResponseJson,
  ProviderApiError,
} from "./provider-api-error";

export type GeminiEmbeddingTaskType =
  | "TASK_TYPE_UNSPECIFIED"
  | "RETRIEVAL_QUERY"
  | "RETRIEVAL_DOCUMENT"
  | "SEMANTIC_SIMILARITY"
  | "CLASSIFICATION";

export interface TextEmbeddingClient {
  embedTexts(input: {
    texts: string[];
    taskType?: GeminiEmbeddingTaskType;
    outputDimensionality?: number;
  }): Promise<number[][]>;
}

const DEFAULT_EMBEDDING_MODEL = "gemini-embedding-001";
const DEFAULT_OUTPUT_DIMENSIONALITY = 768;
const MAX_BATCH_SIZE = 32;

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

function normalizeVector(values: number[]) {
  const magnitude = Math.sqrt(
    values.reduce((sum, value) => sum + value * value, 0),
  );
  if (!Number.isFinite(magnitude) || magnitude <= 0) {
    return values;
  }
  return values.map((value) => value / magnitude);
}

function chunkArray<T>(items: T[], chunkSize: number) {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += chunkSize) {
    chunks.push(items.slice(index, index + chunkSize));
  }
  return chunks;
}

function normalizeModelName(modelName: string) {
  return modelName.startsWith("models/") ? modelName : `models/${modelName}`;
}

class GeminiTextEmbeddingClient implements TextEmbeddingClient {
  constructor(
    private readonly apiKey: string,
    private readonly modelName: string,
    private readonly timeoutMs: number,
  ) {}

  async embedTexts(input: {
    texts: string[];
    taskType?: GeminiEmbeddingTaskType;
    outputDimensionality?: number;
  }) {
    if (input.texts.length === 0) {
      return [];
    }

    const requests = input.texts.map((text) => ({
      model: normalizeModelName(this.modelName),
      content: {
        parts: [
          {
            text: text.trim() || " ",
          },
        ],
      },
      taskType: input.taskType ?? "SEMANTIC_SIMILARITY",
      outputDimensionality:
        input.outputDimensionality ?? DEFAULT_OUTPUT_DIMENSIONALITY,
    }));

    const embeddings: number[][] = [];
    for (const batch of chunkArray(requests, MAX_BATCH_SIZE)) {
      const response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/${normalizeModelName(this.modelName)}:batchEmbedContents`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-goog-api-key": this.apiKey,
          },
          signal: getAbortSignal(this.timeoutMs),
          body: JSON.stringify({
            requests: batch,
          }),
        },
      );

      const responseBody = await response.text();
      const payload = parseProviderResponseJson(responseBody) as {
        embeddings?: Array<{ values?: number[] }>;
      } | null;
      if (!response.ok) {
        throw new ProviderApiError({
          provider: "gemini",
          statusCode: response.status,
          responseBody,
        });
      }

      if (!payload) {
        throw new Error("Gemini embeddings response was not valid JSON.");
      }

      const batchEmbeddings = payload.embeddings ?? [];
      if (batchEmbeddings.length !== batch.length) {
        throw new Error("Gemini embeddings response size did not match request size.");
      }

      for (const embedding of batchEmbeddings) {
        const values = Array.isArray(embedding.values)
          ? embedding.values.filter((value): value is number =>
              typeof value === "number" && Number.isFinite(value),
            )
          : [];
        if (values.length === 0) {
          throw new Error("Gemini embeddings response did not include vector values.");
        }
        embeddings.push(normalizeVector(values));
      }
    }

    return embeddings;
  }
}

export function isTextEmbeddingConfigured() {
  return Boolean(getProviderApiKey("gemini"));
}

export function createTextEmbeddingClient(
  modelName = DEFAULT_EMBEDDING_MODEL,
): TextEmbeddingClient {
  const apiKey = getProviderApiKey("gemini");
  if (!apiKey) {
    throw new Error("GEMINI_API_KEY is required for text embeddings.");
  }

  return new GeminiTextEmbeddingClient(apiKey, modelName, getRequestTimeoutMs());
}
