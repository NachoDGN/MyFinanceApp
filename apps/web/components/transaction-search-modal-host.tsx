"use client";

import { useMemo, useState } from "react";

type NavigationState = {
  scopeParam: string;
  currency: string;
  period: string;
  referenceDate?: string;
  latestReferenceDate?: string;
  start?: string;
  end?: string;
};

type TransactionQuestionEvidence = {
  id: string;
  type:
    | "transaction"
    | "ledger_query"
    | "source_batch"
    | "import_batch"
    | "audit_event";
  title: string;
  summary: string;
  transactionId?: string | null;
  sourceId?: string | null;
  metadata: Record<string, unknown>;
};

type TransactionQuestionTraceEvent = {
  stepIndex: number;
  actor: "executor" | "tool" | "system";
  eventType:
    | "decision"
    | "tool_call"
    | "tool_result"
    | "final_answer"
    | "error";
  summary: string;
  latencyMs?: number | null;
};

type TransactionQuestionAnswerResponse = {
  schemaVersion: "v1";
  runId: string | null;
  question: string;
  answer: string;
  answerType:
    | "single_transaction"
    | "aggregate"
    | "comparison"
    | "clarification"
    | "no_result";
  confidence: number | null;
  insufficiencyReason: string | null;
  citations: TransactionQuestionEvidence[];
  evidence: TransactionQuestionEvidence[];
  trace: TransactionQuestionTraceEvent[];
  warnings: string[];
  generatedAt: string;
};

const exampleQuestions = [
  "What was my last paid invoice value to Cardisa?",
  "What's my overall yearly spend for Custodio?",
  "Show the most recent unresolved card charge",
];

function formatConfidence(value: number | null) {
  if (value === null) {
    return "Not scored";
  }
  return `${Math.round(value * 100)}%`;
}

function formatEvidenceMeta(evidence: TransactionQuestionEvidence) {
  if (evidence.type === "transaction") {
    const amount = [
      evidence.metadata.amountOriginal,
      evidence.metadata.currencyOriginal,
    ]
      .filter((value) => typeof value === "string" && value)
      .join(" ");
    const date =
      typeof evidence.metadata.transactionDate === "string"
        ? evidence.metadata.transactionDate
        : null;
    return [date, amount].filter(Boolean).join(" · ");
  }

  if (evidence.type === "ledger_query") {
    const rowCount = evidence.metadata.rowCount;
    return typeof rowCount === "number" ? `${rowCount} rows` : "Query result";
  }

  return evidence.type.replace(/_/g, " ");
}

function isAnswerPayload(
  payload: TransactionQuestionAnswerResponse | { error?: string } | null,
): payload is TransactionQuestionAnswerResponse {
  return Boolean(
    payload &&
      "schemaVersion" in payload &&
      payload.schemaVersion === "v1" &&
      "answer" in payload,
  );
}

function buildTransactionsHref(
  state: NavigationState,
  transactionId: string | null | undefined,
) {
  const query = new URLSearchParams({
    scope: state.scopeParam,
    currency: state.currency,
    period: state.period,
  });
  if (state.referenceDate) {
    query.set("asOf", state.referenceDate);
  }
  if (state.period === "custom" && state.start && state.end) {
    query.set("start", state.start);
    query.set("end", state.end);
  }
  return `/transactions?${query.toString()}${
    transactionId ? `#transaction-${transactionId}` : ""
  }`;
}

export function TransactionSearchModalHost({
  state,
}: {
  state: NavigationState;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const [question, setQuestion] = useState("");
  const [answer, setAnswer] =
    useState<TransactionQuestionAnswerResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const canSubmit = question.trim().length > 0 && !isSubmitting;
  const traceSummary = useMemo(() => {
    if (!answer) {
      return [];
    }
    return answer.trace.slice(-8);
  }, [answer]);

  async function submitQuestion(nextQuestion = question) {
    const trimmed = nextQuestion.trim();
    if (!trimmed) {
      return;
    }

    setQuestion(trimmed);
    setIsSubmitting(true);
    setError(null);
    setAnswer(null);

    try {
      const response = await fetch("/api/transaction-search/ask", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          question: trimmed,
          scope: state.scopeParam,
          currency: state.currency,
          period: state.period,
          asOf: state.referenceDate,
          start: state.start,
          end: state.end,
        }),
      });
      const payload = (await response.json().catch(() => null)) as
        | TransactionQuestionAnswerResponse
        | { error?: string }
        | null;
      if (!response.ok || !isAnswerPayload(payload)) {
        throw new Error(
          payload && "error" in payload && payload.error
            ? payload.error
            : "Transaction search failed.",
        );
      }
      setAnswer(payload);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Transaction search failed.",
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <>
      <button
        type="button"
        className="filter-pill transaction-agent-open-button"
        onClick={() => setIsOpen(true)}
      >
        Ask Ledger
      </button>

      {isOpen ? (
        <div className="transaction-agent-backdrop">
          <section
            className="transaction-agent-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="transaction-agent-title"
          >
            <div className="transaction-agent-header">
              <div>
                <span className="label-sm">Transaction Agent</span>
                <h2 className="section-title" id="transaction-agent-title">
                  Ask the Ledger
                </h2>
              </div>
              <button
                type="button"
                className="btn-ghost"
                onClick={() => setIsOpen(false)}
              >
                Close
              </button>
            </div>

            <form
              className="transaction-agent-form"
              onSubmit={(event) => {
                event.preventDefault();
                void submitQuestion();
              }}
            >
              <label className="input-label">
                <span>Question</span>
                <textarea
                  className="input-textarea transaction-agent-question"
                  value={question}
                  onChange={(event) => setQuestion(event.target.value)}
                  placeholder="Ask about a transaction, vendor, invoice, or spend total"
                />
              </label>
              <div className="transaction-agent-actions">
                <button className="btn-pill" type="submit" disabled={!canSubmit}>
                  {isSubmitting ? "Searching..." : "Ask"}
                </button>
              </div>
            </form>

            <div className="transaction-agent-examples">
              {exampleQuestions.map((example) => (
                <button
                  key={example}
                  type="button"
                  className="transaction-agent-example"
                  onClick={() => {
                    setQuestion(example);
                    void submitQuestion(example);
                  }}
                  disabled={isSubmitting}
                >
                  {example}
                </button>
              ))}
            </div>

            {error ? <div className="status-note warning">{error}</div> : null}

            {answer ? (
              <div className="transaction-agent-result">
                <div className="transaction-agent-answer-card">
                  <div className="transaction-agent-answer-meta">
                    <span className="pill">{answer.answerType}</span>
                    <span className="pill">
                      Confidence {formatConfidence(answer.confidence)}
                    </span>
                  </div>
                  <p>{answer.answer}</p>
                  {answer.insufficiencyReason ? (
                    <div className="status-note warning">
                      {answer.insufficiencyReason}
                    </div>
                  ) : null}
                </div>

                {answer.warnings.length > 0 ? (
                  <div className="transaction-agent-warning-list">
                    {answer.warnings.map((warning) => (
                      <div className="status-note warning" key={warning}>
                        {warning}
                      </div>
                    ))}
                  </div>
                ) : null}

                <div className="transaction-agent-evidence-grid">
                  <div className="transaction-agent-panel">
                    <h3>Citations</h3>
                    {answer.citations.length === 0 ? (
                      <p className="muted">No citation was available.</p>
                    ) : (
                      <div className="transaction-agent-evidence-list">
                        {answer.citations.map((evidence) => (
                          <a
                            key={evidence.id}
                            className="transaction-agent-evidence-row"
                            href={buildTransactionsHref(
                              state,
                              evidence.transactionId,
                            )}
                          >
                            <span>{evidence.title}</span>
                            <small>{formatEvidenceMeta(evidence)}</small>
                            <small>{evidence.id}</small>
                          </a>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="transaction-agent-panel">
                    <h3>Trace</h3>
                    <div className="transaction-agent-trace">
                      {traceSummary.map((event, index) => (
                        <div
                          className="transaction-agent-trace-row"
                          key={`${event.stepIndex}-${event.eventType}-${index}`}
                        >
                          <span>
                            {event.stepIndex}. {event.actor}
                          </span>
                          <p>{event.summary}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            ) : null}
          </section>
        </div>
      ) : null}
    </>
  );
}
