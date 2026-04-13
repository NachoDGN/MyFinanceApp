import assert from "node:assert/strict";
import test from "node:test";

import {
  createLLMClient,
  createTextEmbeddingClient,
  LLMUnavailableError,
  ProviderApiError,
} from "../packages/llm/src/index.ts";

const quotaExceededResponse = {
  error: {
    code: 429,
    message: "Your project has exceeded its monthly spending cap.",
    status: "RESOURCE_EXHAUSTED",
    details: [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        reason: "BILLING_DISABLED",
      },
    ],
  },
};

test("gemini embeddings throw a parseable provider api error", async () => {
  const previousGeminiKey = process.env.GEMINI_API_KEY;
  const previousFetch = globalThis.fetch;

  process.env.GEMINI_API_KEY = "test-gemini-key";
  globalThis.fetch = async () =>
    new Response(JSON.stringify(quotaExceededResponse), {
      status: 429,
      headers: { "Content-Type": "application/json" },
    });

  try {
    const embeddingClient = createTextEmbeddingClient("gemini-embedding-001");

    await assert.rejects(
      async () =>
        embeddingClient.embedTexts({
          texts: ["uber madrid trip"],
        }),
      (error: unknown) => {
        assert.ok(error instanceof ProviderApiError);
        assert.equal(error.provider, "gemini");
        assert.equal(error.statusCode, 429);
        assert.equal(error.providerErrorStatus, "RESOURCE_EXHAUSTED");
        assert.equal(error.providerErrorCode, 429);
        assert.deepEqual(error.responseJson, quotaExceededResponse);
        assert.deepEqual(error.providerError, quotaExceededResponse.error);
        assert.match(error.message, /RESOURCE_EXHAUSTED/);
        assert.match(error.message, /monthly spending cap/i);
        return true;
      },
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousGeminiKey === undefined) {
      delete process.env.GEMINI_API_KEY;
    } else {
      process.env.GEMINI_API_KEY = previousGeminiKey;
    }
  }
});

test("llm unavailable errors preserve parsed gemini provider responses", async () => {
  const previousGeminiKey = process.env.GEMINI_API_KEY;
  const previousFetch = globalThis.fetch;

  process.env.GEMINI_API_KEY = "test-gemini-key";
  globalThis.fetch = async () =>
    new Response(JSON.stringify(quotaExceededResponse), {
      status: 429,
      headers: { "Content-Type": "application/json" },
    });

  try {
    const client = createLLMClient();

    await assert.rejects(
      async () =>
        client.generateText({
          systemPrompt: "You are a test system.",
          userPrompt: "Say hello.",
          modelName: "gemini-2.5-flash",
          maxRetries: 1,
        }),
      (error: unknown) => {
        assert.ok(error instanceof LLMUnavailableError);
        assert.equal(error.provider, "gemini");
        assert.equal(error.kind, "api_exception");
        assert.equal(error.statusCode, 429);
        assert.equal(
          error.rawOutput,
          JSON.stringify(quotaExceededResponse),
        );
        assert.deepEqual(error.providerError, quotaExceededResponse.error);
        assert.match(error.message, /RESOURCE_EXHAUSTED/);
        assert.match(error.message, /monthly spending cap/i);
        return true;
      },
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousGeminiKey === undefined) {
      delete process.env.GEMINI_API_KEY;
    } else {
      process.env.GEMINI_API_KEY = previousGeminiKey;
    }
  }
});
