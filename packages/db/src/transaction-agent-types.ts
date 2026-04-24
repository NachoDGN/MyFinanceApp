import type {
  AnswerTransactionQuestionInput,
  DomainDataset,
  TransactionQuestionAnswerResponse,
  TransactionQuestionEvidence,
  TransactionQuestionTraceEvent,
} from "@myfinance/domain";

import type { SearchTransactionsResult } from "./transaction-search";

export type TransactionAgentToolName =
  | "answer"
  | "hybrid_transaction_search"
  | "sql_ledger_query"
  | "get_transaction"
  | "get_related_entries";

export type TransactionAgentAnswerType =
  TransactionQuestionAnswerResponse["answerType"];

export type TransactionAgentDecision = {
  reasoningSummary: string;
  action: {
    type: "tool";
    toolName: TransactionAgentToolName;
    arguments?: Record<string, unknown>;
  };
};

export type TransactionAgentSettings = {
  maxSteps: number;
  modelName: string;
  tracePayloads: boolean;
};

export type TransactionAgentRunInput = AnswerTransactionQuestionInput & {
  dataset: DomainDataset;
  settings?: Partial<TransactionAgentSettings>;
};

export type TransactionAgentState = {
  priorSearchQueries: string[];
  priorEvents: string[];
  evidenceSummary: string[];
  transientObservation: string | null;
  initialRetrieval: SearchTransactionsResult | null;
  lastRetrieval: SearchTransactionsResult | null;
  retrievalByTransactionId: Map<string, SearchTransactionsResult["rows"][number]>;
  evidenceById: Map<string, TransactionQuestionEvidence>;
  evidenceOrder: string[];
  trace: TransactionQuestionTraceEvent[];
  warnings: Set<string>;
};

export type TransactionAgentToolContext = {
  userId: string;
  runId: string | null;
  question: string;
  input: TransactionAgentRunInput;
  state: TransactionAgentState;
};

export type TransactionAgentToolResult = {
  summary: string;
  payload: Record<string, unknown>;
  evidence: TransactionQuestionEvidence[];
  retrieval?: SearchTransactionsResult | null;
  finalAnswer?: {
    answer: string;
    answerType: TransactionAgentAnswerType;
    citations: TransactionQuestionEvidence[];
    confidence: number | null;
    insufficiencyReason: string | null;
  };
};

export type TransactionAgentExecutor = (input: {
  prompt: string;
  settings: TransactionAgentSettings;
}) => Promise<TransactionAgentDecision>;
