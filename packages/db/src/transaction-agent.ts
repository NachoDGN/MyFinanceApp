import {
  createLLMClient,
  type LLMTaskClient,
} from "@myfinance/llm";
import type {
  TransactionQuestionAnswerResponse,
  TransactionQuestionEvidence,
  TransactionQuestionTraceEvent,
} from "@myfinance/domain";

import type { SqlClient } from "./sql-runtime";
import {
  buildTransactionAgentDecisionPrompt,
  buildTransactionAgentSystemPrompt,
  transactionAgentDecisionJsonSchema,
  transactionAgentDecisionSchema,
} from "./transaction-agent-prompts";
import { isDuplicateAgentSearchQuery } from "./transaction-agent-sql-guard";
import {
  appendTransactionAgentEvent,
  appendTransactionAgentEvidence,
  completeTransactionAgentRun,
  createTransactionAgentRun,
} from "./transaction-agent-trace";
import {
  executeTransactionAgentTool,
  formatTransactionAgentObservation,
  summarizeTransactionAgentToolResult,
} from "./transaction-agent-tools";
import type {
  TransactionAgentDecision,
  TransactionAgentRunInput,
  TransactionAgentSettings,
  TransactionAgentState,
  TransactionAgentToolName,
  TransactionAgentToolResult,
} from "./transaction-agent-types";

const DEFAULT_AGENT_SETTINGS: TransactionAgentSettings = {
  maxSteps: 8,
  modelName:
    process.env.TRANSACTION_AGENT_MODEL?.trim() ||
    process.env.TRANSACTION_SEARCH_AGENT_MODEL?.trim() ||
    process.env.TRANSACTION_SEARCH_GENERATION_MODEL?.trim() ||
    "gpt-5.4-mini",
  tracePayloads: false,
};

function mergeSettings(
  settings: Partial<TransactionAgentSettings> | undefined,
): TransactionAgentSettings {
  return {
    ...DEFAULT_AGENT_SETTINGS,
    ...settings,
    maxSteps: Math.max(
      1,
      Math.min(settings?.maxSteps ?? DEFAULT_AGENT_SETTINGS.maxSteps, 10),
    ),
  };
}

function addTrace(
  state: TransactionAgentState,
  event: TransactionQuestionTraceEvent,
) {
  state.trace.push(event);
}

function recordEvidence(
  state: TransactionAgentState,
  evidence: TransactionQuestionEvidence[],
) {
  for (const item of evidence) {
    if (!state.evidenceById.has(item.id)) {
      state.evidenceOrder.push(item.id);
    }
    state.evidenceById.set(item.id, item);
  }
}

function trimInPlace(values: string[], limit: number) {
  while (values.length > limit) {
    values.shift();
  }
}

async function decideNextAction(input: {
  llm: LLMTaskClient;
  question: string;
  settings: TransactionAgentSettings;
  state: TransactionAgentState;
  completedSteps: number;
  runInput: TransactionAgentRunInput;
}): Promise<TransactionAgentDecision> {
  const prompt = buildTransactionAgentDecisionPrompt({
    question: input.question,
    settings: input.settings,
    dataset: input.runInput.dataset,
    displayCurrency: input.runInput.currency,
    referenceDate: input.runInput.referenceDate,
    state: input.state,
    completedSteps: input.completedSteps,
    toolNames: [
      "answer",
      "hybrid_transaction_search",
      "sql_ledger_query",
      "get_transaction",
      "get_related_entries",
    ],
  });

  return input.llm.generateJson({
    modelName: input.settings.modelName,
    systemPrompt: buildTransactionAgentSystemPrompt(),
    userPrompt: prompt,
    responseSchema: transactionAgentDecisionSchema,
    responseJsonSchema: transactionAgentDecisionJsonSchema,
    schemaName: "transaction_agent_decision",
    temperature: 0.1,
    maxTokens: 1600,
    maxRetries: 1,
  });
}

function updateStateFromToolResult(input: {
  state: TransactionAgentState;
  toolName: TransactionAgentToolName;
  toolResult: TransactionAgentToolResult;
}) {
  recordEvidence(input.state, input.toolResult.evidence);
  if (input.toolResult.retrieval) {
    input.state.lastRetrieval = input.toolResult.retrieval;
    for (const row of input.toolResult.retrieval.rows) {
      input.state.retrievalByTransactionId.set(row.transaction.id, row);
    }
  }

  const warnings = input.toolResult.payload.warnings;
  if (Array.isArray(warnings)) {
    for (const warning of warnings) {
      if (typeof warning === "string" && warning.trim()) {
        input.state.warnings.add(warning);
      }
    }
  }

  input.state.priorEvents.push(`${input.toolName}: ${input.toolResult.summary}`);
  input.state.evidenceSummary.push(
    ...summarizeTransactionAgentToolResult(
      input.toolName,
      input.toolResult.payload,
    ),
  );
  trimInPlace(input.state.priorEvents, 18);
  trimInPlace(input.state.evidenceSummary, 18);
  input.state.transientObservation = formatTransactionAgentObservation(
    input.toolName,
    input.toolResult.payload,
  );
}

async function executeAndTraceTool(input: {
  sql: SqlClient;
  runId: string | null;
  userId: string;
  stepIndex: number;
  toolName: TransactionAgentToolName;
  args: Record<string, unknown>;
  runInput: TransactionAgentRunInput;
  state: TransactionAgentState;
  tracePayloads: boolean;
}) {
  addTrace(input.state, {
    stepIndex: input.stepIndex,
    actor: "system",
    eventType: "tool_call",
    summary: `Calling ${input.toolName}.`,
  });
  await appendTransactionAgentEvent(input.sql, {
    runId: input.runId,
    stepIndex: input.stepIndex,
    actor: "system",
    eventType: "tool_call",
    summary: `Calling ${input.toolName}.`,
    payload: input.tracePayloads
      ? { toolName: input.toolName, arguments: input.args }
      : { toolName: input.toolName },
  });

  const start = Date.now();
  let toolResult: TransactionAgentToolResult;
  try {
    toolResult = await executeTransactionAgentTool({
      sql: input.sql,
      toolName: input.toolName,
      arguments: input.args,
      context: {
        userId: input.userId,
        runId: input.runId,
        question: input.runInput.question,
        input: input.runInput,
        state: input.state,
      },
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown tool execution error.";
    toolResult = {
      summary: `${input.toolName} failed: ${message}`,
      payload: {
        toolName: input.toolName,
        error: message,
      },
      evidence: [],
    };
  }
  const latencyMs = Date.now() - start;

  addTrace(input.state, {
    stepIndex: input.stepIndex,
    actor: "tool",
    eventType: toolResult.payload.error ? "error" : "tool_result",
    summary: toolResult.summary,
    latencyMs,
  });
  const eventId = await appendTransactionAgentEvent(input.sql, {
    runId: input.runId,
    stepIndex: input.stepIndex,
    actor: "tool",
    eventType: toolResult.payload.error ? "error" : "tool_result",
    summary: toolResult.summary,
    latencyMs,
    payload: input.tracePayloads
      ? {
          toolName: input.toolName,
          arguments: input.args,
          result: toolResult.payload,
        }
      : { toolName: input.toolName },
  });
  await appendTransactionAgentEvidence(input.sql, {
    runId: input.runId,
    eventId,
    evidence: toolResult.evidence,
  });

  updateStateFromToolResult({
    state: input.state,
    toolName: input.toolName,
    toolResult,
  });

  return toolResult;
}

function initialState(): TransactionAgentState {
  return {
    priorSearchQueries: [],
    priorEvents: [],
    evidenceSummary: [],
    transientObservation: null,
    initialRetrieval: null,
    lastRetrieval: null,
    retrievalByTransactionId: new Map(),
    evidenceById: new Map(),
    evidenceOrder: [],
    trace: [],
    warnings: new Set(),
  };
}

export async function runTransactionQuestionAgent(
  sql: SqlClient,
  userId: string,
  input: TransactionAgentRunInput,
): Promise<TransactionQuestionAnswerResponse> {
  const question = input.question.trim();
  if (!question) {
    throw new Error("Transaction question is required.");
  }

  const settings = mergeSettings(input.settings);
  const state = initialState();
  let toolCallCount = 0;
  const runId = await createTransactionAgentRun(sql, {
    userId,
    question,
    executorModel: settings.modelName,
    settingsJson: settings,
  });

  try {
    toolCallCount += 1;
    const initialResult = await executeAndTraceTool({
      sql,
      runId,
      userId,
      stepIndex: 0,
      toolName: "hybrid_transaction_search",
      args: { query: question, limit: 3 },
      runInput: input,
      state,
      tracePayloads: settings.tracePayloads,
    });
    if (initialResult.retrieval) {
      state.initialRetrieval = initialResult.retrieval;
      state.lastRetrieval = initialResult.retrieval;
    }
    state.priorSearchQueries.push(question);

    const llm = createLLMClient();
    for (let stepIndex = 1; stepIndex <= settings.maxSteps; stepIndex += 1) {
      const decisionStart = Date.now();
      const decision = await decideNextAction({
        llm,
        question,
        settings,
        state,
        completedSteps: stepIndex - 1,
        runInput: input,
      });
      const decisionLatencyMs = Date.now() - decisionStart;

      state.transientObservation = null;
      state.priorEvents.push(
        `Step ${stepIndex} decision: ${decision.reasoningSummary}`,
      );
      trimInPlace(state.priorEvents, 18);
      addTrace(state, {
        stepIndex,
        actor: "executor",
        eventType: "decision",
        summary: decision.reasoningSummary,
        latencyMs: decisionLatencyMs,
      });
      await appendTransactionAgentEvent(sql, {
        runId,
        stepIndex,
        actor: "executor",
        eventType: "decision",
        summary: decision.reasoningSummary,
        latencyMs: decisionLatencyMs,
        payload: settings.tracePayloads ? { decision } : {},
      });

      const toolName = decision.action.toolName;
      const args = decision.action.arguments ?? {};
      if (toolName === "hybrid_transaction_search") {
        const query = String(args.query ?? "");
        if (isDuplicateAgentSearchQuery(query, state.priorSearchQueries)) {
          const duplicateSummary =
            "Duplicate search query blocked. Reformulate materially or switch tools.";
          state.priorEvents.push(`hybrid_transaction_search: ${duplicateSummary}`);
          state.evidenceSummary.push(duplicateSummary);
          state.transientObservation = duplicateSummary;
          trimInPlace(state.priorEvents, 18);
          trimInPlace(state.evidenceSummary, 18);
          state.warnings.add(duplicateSummary);
          continue;
        }
        state.priorSearchQueries.push(query);
        trimInPlace(state.priorSearchQueries, 8);
      }

      toolCallCount += 1;
      const toolResult = await executeAndTraceTool({
        sql,
        runId,
        userId,
        stepIndex,
        toolName,
        args,
        runInput: input,
        state,
        tracePayloads: settings.tracePayloads,
      });

      if (toolResult.finalAnswer) {
        addTrace(state, {
          stepIndex,
          actor: "executor",
          eventType: "final_answer",
          summary: "Finalized through answer tool.",
        });
        await appendTransactionAgentEvent(sql, {
          runId,
          stepIndex,
          actor: "executor",
          eventType: "final_answer",
          summary: "Finalized through answer tool.",
          payload: {
            citationIds: toolResult.finalAnswer.citations.map(
              (citation) => citation.id,
            ),
          },
        });
        await completeTransactionAgentRun(sql, {
          runId,
          finalAnswer: toolResult.finalAnswer.answer,
          citationIds: toolResult.finalAnswer.citations.map(
            (citation) => citation.id,
          ),
          stepCount: stepIndex,
          toolCallCount,
          status: "succeeded",
          metadataJson: {
            answerType: toolResult.finalAnswer.answerType,
            confidence: toolResult.finalAnswer.confidence,
          },
        });

        return {
          schemaVersion: "v1",
          runId,
          question,
          answer: toolResult.finalAnswer.answer,
          answerType: toolResult.finalAnswer.answerType,
          confidence: toolResult.finalAnswer.confidence,
          insufficiencyReason: toolResult.finalAnswer.insufficiencyReason,
          citations: toolResult.finalAnswer.citations,
          evidence: [...state.evidenceById.values()],
          trace: state.trace,
          warnings: [...state.warnings],
          generatedAt: new Date().toISOString(),
        };
      }
    }

    const budgetEvidence = state.evidenceOrder
      .slice(-4)
      .flatMap((id) => {
        const evidence = state.evidenceById.get(id);
        return evidence ? [evidence] : [];
      });
    const budgetAnswer =
      "I could not gather enough grounded ledger evidence within the current search budget.";
    await completeTransactionAgentRun(sql, {
      runId,
      finalAnswer: budgetAnswer,
      citationIds: budgetEvidence.map((evidence) => evidence.id),
      stepCount: settings.maxSteps,
      toolCallCount,
      status: "succeeded",
      metadataJson: { exhaustedBudget: true },
    });

    return {
      schemaVersion: "v1",
      runId,
      question,
      answer: budgetAnswer,
      answerType: "no_result",
      confidence: 0.2,
      insufficiencyReason: "Step budget exhausted before sufficient evidence.",
      citations: budgetEvidence,
      evidence: [...state.evidenceById.values()],
      trace: state.trace,
      warnings: [...state.warnings],
      generatedAt: new Date().toISOString(),
    };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown transaction agent error.";
    addTrace(state, {
      stepIndex: 0,
      actor: "system",
      eventType: "error",
      summary: message,
    });
    await appendTransactionAgentEvent(sql, {
      runId,
      stepIndex: 0,
      actor: "system",
      eventType: "error",
      summary: message,
      payload: { error: message },
    });
    await completeTransactionAgentRun(sql, {
      runId,
      finalAnswer: "",
      citationIds: [],
      stepCount: state.trace.length,
      toolCallCount,
      status: "failed",
      failureMessage: message,
    });
    throw error;
  }
}
