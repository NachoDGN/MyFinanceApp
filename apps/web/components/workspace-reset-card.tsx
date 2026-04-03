"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import { resetWorkspaceAction } from "../app/actions";

export function WorkspaceResetCard() {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<string | null>(null);

  function handleReset() {
    if (
      !window.confirm(
        "Clear all demo finance data in the local dev database? This will remove accounts, imports, transactions, rules, and templates for the seeded user.",
      )
    ) {
      return;
    }

    startTransition(async () => {
      setMessage(null);
      try {
        const result = await resetWorkspaceAction();
        setMessage(
          `Workspace cleared. Removed ${result.deleted.accounts} account(s), ${result.deleted.importBatches} import batch(es), and ${result.deleted.transactions} transaction(s).`,
        );
        router.refresh();
      } catch (error) {
        setMessage(error instanceof Error ? error.message : "Workspace reset failed.");
      }
    });
  }

  return (
    <div className="legend-list">
      <span className="pill warning">Local dev only</span>
      <p className="muted">
        This clears the seeded demo finance dataset for the current local user so you can create your own templates,
        accounts, and imports from scratch. It keeps the profile, categories, and existing entities.
      </p>
      <div className="inline-actions">
        <button className="btn-pill" type="button" disabled={isPending} onClick={handleReset}>
          {isPending ? "Clearing..." : "Clear Demo Data"}
        </button>
        <span className="muted">After this, use Templates, Accounts, and Imports to load your own data.</span>
      </div>
      {message ? <div className="status-note">{message}</div> : null}
    </div>
  );
}
