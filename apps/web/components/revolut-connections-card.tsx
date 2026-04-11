"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import { SectionCard } from "./primitives";

type EntityOption = {
  id: string;
  displayName: string;
};

type ManagedRevolutConnection = {
  id: string;
  entityName: string;
  status: string;
  lastSuccessfulSyncAt: string | null;
  lastSyncQueuedAt: string | null;
  lastWebhookAt: string | null;
  lastError: string | null;
  linkedAccounts: string[];
};

export function RevolutConnectionsCard({
  configured,
  missingEnvKeys,
  entities,
  connections,
}: {
  configured: boolean;
  missingEnvKeys: string[];
  entities: EntityOption[];
  connections: ManagedRevolutConnection[];
}) {
  const router = useRouter();
  const [selectedEntityId, setSelectedEntityId] = useState(entities[0]?.id ?? "");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  function handleSync(connectionId: string) {
    startTransition(async () => {
      setFeedback(null);
      try {
        const response = await fetch("/api/bank/revolut/sync", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ connectionId }),
        });
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as
            | { error?: string }
            | null;
          throw new Error(payload?.error || "Failed to queue the Revolut sync.");
        }
        const payload = (await response.json()) as {
          queued: boolean;
          jobId: string | null;
        };
        setFeedback(
          payload.queued
            ? "Revolut sync queued."
            : "A Revolut sync is already queued or running.",
        );
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Failed to queue the Revolut sync.",
        );
      }
    });
  }

  return (
    <SectionCard
      title="Revolut Business"
      subtitle="Direct company-bank sync with rich transaction context for the analyzer"
      span="span-8"
    >
      <div className="legend-list" style={{ marginBottom: 20 }}>
        <span className="pill">Read-only OAuth</span>
        <span className="pill">Per-currency account linking</span>
        <span className="pill">Webhook + scheduled sync</span>
        <span className="pill">LLM provider context preserved</span>
      </div>

      {!configured ? (
        <div className="status-note">
          Revolut is not configured yet. Missing env vars: {missingEnvKeys.join(", ")}.
        </div>
      ) : null}

      {configured ? (
        <div className="revolut-connect-row">
          <label className="input-label" style={{ minWidth: 240 }}>
            Connect entity
            <select
              className="input-select"
              value={selectedEntityId}
              onChange={(event) => setSelectedEntityId(event.target.value)}
            >
              {entities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.displayName}
                </option>
              ))}
            </select>
          </label>
          <a
            className="btn-pill"
            href={
              selectedEntityId
                ? `/api/bank/revolut/connect?entityId=${encodeURIComponent(selectedEntityId)}`
                : "#"
            }
          >
            Connect Revolut
          </a>
        </div>
      ) : null}

      {feedback ? <div className="status-note">{feedback}</div> : null}

      <div className="revolut-connection-list">
        {connections.length === 0 ? (
          <div className="status-note">
            No Revolut Business connections yet. Connect a company entity to replace monthly spreadsheet uploads with direct sync.
          </div>
        ) : (
          connections.map((connection) => (
            <article key={connection.id} className="revolut-connection-card">
              <div className="revolut-connection-head">
                <div>
                  <h3 className="section-title" style={{ marginBottom: 4 }}>
                    {connection.entityName}
                  </h3>
                  <div className="legend-list">
                    <span className="pill">{connection.status}</span>
                    <span className="pill">
                      Last sync {connection.lastSuccessfulSyncAt ?? "Never"}
                    </span>
                    <span className="pill">
                      Last queued {connection.lastSyncQueuedAt ?? "Never"}
                    </span>
                  </div>
                </div>
                <button
                  className="btn-pill"
                  type="button"
                  disabled={isPending}
                  onClick={() => handleSync(connection.id)}
                >
                  Sync now
                </button>
              </div>
              <div className="revolut-connection-meta">
                <div>
                  <div className="timeline-label">Linked accounts</div>
                  <div className="legend-list">
                    {connection.linkedAccounts.length > 0 ? (
                      connection.linkedAccounts.map((accountName) => (
                        <span key={accountName} className="pill">
                          {accountName}
                        </span>
                      ))
                    ) : (
                      <span className="pill warning">No local accounts linked</span>
                    )}
                  </div>
                </div>
                <div>
                  <div className="timeline-label">Last webhook</div>
                  <div className="metric-nominal">
                    {connection.lastWebhookAt ?? "No webhook received yet"}
                  </div>
                </div>
                {connection.lastError ? (
                  <div>
                    <div className="timeline-label">Last error</div>
                    <div className="status-note">{connection.lastError}</div>
                  </div>
                ) : null}
              </div>
            </article>
          ))
        )}
      </div>
    </SectionCard>
  );
}
