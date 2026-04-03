"use client";

import { useRouter } from "next/navigation";
import { useMemo, useState, useTransition } from "react";

import { SectionCard, SimpleTable } from "./primitives";

function formatTimestamp(value: string | null | undefined) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

type RulesWorkbenchModel = {
  rules: Array<{
    id: string;
    priority: number;
    scopeJson: Record<string, unknown>;
    conditionsJson: Record<string, unknown>;
    outputsJson: Record<string, unknown>;
    hitCount: number;
    lastHitAt?: string | null;
    active: boolean;
  }>;
  drafts: {
    parserConfigured: boolean;
    drafts: Array<{
      id: string;
      requestText: string;
      status: string;
      attempts: number;
      createdAt: string;
      finishedAt?: string | null;
      lastError?: string | null;
      parsedRule?: {
        title: string;
        summary: string;
        priority: number;
        scopeJson: Record<string, unknown>;
        conditionsJson: Record<string, unknown>;
        outputsJson: Record<string, unknown>;
        confidence: string;
        explanation: string[];
        parseSource: string;
        model?: string | null;
        generatedAt: string;
      } | null;
      appliedRuleId?: string | null;
    }>;
  };
  deterministicSummaries: ReadonlyArray<{
    id: string;
    title: string;
    summary: string;
    evidence: readonly string[];
  }>;
};

export function RulesWorkbench({ model }: { model: RulesWorkbenchModel }) {
  const router = useRouter();
  const [requestText, setRequestText] = useState("");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const draftStats = useMemo(() => {
    const queued = model.drafts.drafts.filter((draft) => draft.status === "queued").length;
    const completed = model.drafts.drafts.filter((draft) => draft.status === "completed").length;
    const failed = model.drafts.drafts.filter((draft) => draft.status === "failed").length;
    return { queued, completed, failed };
  }, [model.drafts.drafts]);

  function queueDraft() {
    if (requestText.trim().length < 8) {
      setFeedback("Provide a more specific rule request so the parser has enough context.");
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      const response = await fetch("/api/rules/drafts", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requestText,
          apply: true,
        }),
      });

      if (!response.ok) {
        setFeedback("Failed to queue the draft. Check the worker and parser configuration.");
        return;
      }

      setRequestText("");
      setFeedback("Draft queued. The worker will parse it in the background.");
      router.refresh();
    });
  }

  function applyDraft(jobId: string) {
    startTransition(async () => {
      setFeedback(null);
      const response = await fetch(`/api/rules/drafts/${jobId}/apply`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apply: true }),
      });

      if (!response.ok) {
        setFeedback("Failed to apply the parsed draft.");
        return;
      }

      setFeedback("Parsed draft promoted into the deterministic rule table.");
      router.refresh();
    });
  }

  return (
    <>
      <SectionCard
        title="Rule Composer"
        subtitle="Natural-language draft"
        span="span-8"
        actions={
          <button className="btn-ghost" type="button" onClick={() => router.refresh()}>
            Refresh Drafts
          </button>
        }
      >
        <div className="form-grid">
          <label className="input-label" style={{ gridColumn: "1 / -1" }}>
            Describe the rule you want
            <textarea
              className="input-textarea"
              placeholder="Example: Whenever my Santander personal card description contains NOTION, classify it as a Company A software expense and set the merchant to NOTION."
              value={requestText}
              onChange={(event) => setRequestText(event.target.value)}
            />
          </label>
        </div>
        <div className="inline-actions" style={{ marginTop: 16 }}>
          <button className="btn-pill" type="button" onClick={queueDraft} disabled={isPending}>
            {isPending ? "Queueing…" : "Queue Draft Parse"}
          </button>
          <span className="muted">
            {model.drafts.parserConfigured
              ? "Background parser configured. Drafts will be interpreted by the worker."
              : "LLM credentials missing. Drafts will use the fallback parser until the selected model is configured."}
          </span>
        </div>
        {feedback ? <div className="status-note">{feedback}</div> : null}
      </SectionCard>

      <SectionCard title="Non-AI Logic" subtitle="Current deterministic enforcement" span="span-4">
        <div className="legend-list">
          {model.deterministicSummaries.map((summary) => (
            <div key={summary.id} className="draft-card" style={{ padding: 16 }}>
              <div className="legend-row">
                <span className="timeline-label">{summary.title}</span>
                <span className="pill">System</span>
              </div>
              <p className="muted" style={{ marginTop: 10 }}>
                {summary.summary}
              </p>
              <div className="evidence-list">
                {summary.evidence.map((item) => (
                  <div key={item} className="evidence-item">
                    {item}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </SectionCard>

      <SectionCard title="Precedence" subtitle="Classification order" span="span-12">
        <div className="inline-actions">
          {[
            "Manual override",
            "User-created rule",
            "Transfer matcher",
            "Investment parser and alias resolver",
            "LLM classification",
            "Fallback unknown",
          ].map((item, index) => (
            <span key={item} className={`pill ${index < 2 ? "warning" : ""}`}>
              {item}
            </span>
          ))}
        </div>
      </SectionCard>

      <SectionCard title="Draft Queue" subtitle="Worker-parsed rule drafts" span="span-12">
        <div className="banner-grid" style={{ marginBottom: 20 }}>
          <div>
            <span className="label-sm">Queued</span>
            <div className="metric-value" style={{ fontSize: 24 }}>
              {draftStats.queued}
            </div>
          </div>
          <div>
            <span className="label-sm">Parsed</span>
            <div className="metric-value" style={{ fontSize: 24 }}>
              {draftStats.completed}
            </div>
          </div>
          <div>
            <span className="label-sm">Failed</span>
            <div className="metric-value" style={{ fontSize: 24 }}>
              {draftStats.failed}
            </div>
          </div>
          <div>
            <span className="label-sm">Parser</span>
            <div className="metric-value" style={{ fontSize: 24 }}>
              {model.drafts.parserConfigured ? "LLM" : "Fallback"}
            </div>
          </div>
        </div>

        {model.drafts.drafts.length === 0 ? (
          <div className="status-note">No draft requests yet. Queue one from the composer above.</div>
        ) : (
          <div className="draft-list">
            {model.drafts.drafts.map((draft) => (
              <div key={draft.id} className="draft-card">
                <div className="draft-meta">
                  <div>
                    <span className="pill">{draft.status}</span>
                    {draft.parsedRule?.parseSource ? (
                      <span className={`pill ${draft.parsedRule.parseSource === "fallback" ? "warning" : ""}`}>
                        {draft.parsedRule.parseSource}
                      </span>
                    ) : null}
                    {draft.appliedRuleId ? <span className="pill">applied</span> : null}
                  </div>
                  <span className="timeline-date">
                    Created {formatTimestamp(draft.createdAt)} · Attempts {draft.attempts}
                  </span>
                </div>
                <p style={{ marginTop: 12, fontWeight: 600 }}>{draft.requestText}</p>
                {draft.lastError ? (
                  <div className="status-note" style={{ marginTop: 12 }}>
                    {draft.lastError}
                  </div>
                ) : null}
                {draft.parsedRule ? (
                  <div className="split-grid" style={{ marginTop: 16 }}>
                    <div>
                      <span className="label-sm">Draft Summary</span>
                      <div className="timeline-label">{draft.parsedRule.title}</div>
                      <p className="muted" style={{ marginTop: 8 }}>
                        {draft.parsedRule.summary}
                      </p>
                      <div className="evidence-list">
                        <div className="evidence-item">
                          Confidence: {draft.parsedRule.confidence}
                        </div>
                        <div className="evidence-item">
                          Priority: {draft.parsedRule.priority}
                        </div>
                        <div className="evidence-item">
                          Generated: {formatTimestamp(draft.parsedRule.generatedAt)}
                        </div>
                        {draft.parsedRule.explanation.map((item) => (
                          <div key={item} className="evidence-item">
                            {item}
                          </div>
                        ))}
                      </div>
                    </div>
                    <div>
                      <span className="label-sm">Structured Logic</span>
                      <pre className="code-block">{JSON.stringify({
                        scopeJson: draft.parsedRule.scopeJson,
                        conditionsJson: draft.parsedRule.conditionsJson,
                        outputsJson: draft.parsedRule.outputsJson,
                      }, null, 2)}</pre>
                    </div>
                  </div>
                ) : null}
                <div className="inline-actions" style={{ marginTop: 16 }}>
                  {draft.status === "completed" && draft.parsedRule && !draft.appliedRuleId ? (
                    <button
                      className="btn-pill"
                      type="button"
                      onClick={() => applyDraft(draft.id)}
                      disabled={isPending}
                    >
                      Apply Draft
                    </button>
                  ) : null}
                  <span className="muted">
                    {draft.appliedRuleId
                      ? `Applied as rule ${draft.appliedRuleId.slice(0, 8)}`
                      : "Drafts stay separate until you promote them into the live rule table."}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionCard>

      <SimpleTable
        span="span-12"
        headers={["Priority", "Scope", "Conditions", "Outputs", "Hit Count", "Last Hit", "Active"]}
        rows={model.rules.map((rule) => [
          String(rule.priority),
          JSON.stringify(rule.scopeJson),
          JSON.stringify(rule.conditionsJson),
          JSON.stringify(rule.outputsJson),
          String(rule.hitCount),
          rule.lastHitAt ?? "—",
          rule.active ? "Yes" : "No",
        ])}
      />
    </>
  );
}
