"use client";

export const IMPORT_REVIEW_ACTIVE_BATCH_STORAGE_KEY =
  "import-review-active-batch-id";
export const IMPORT_REVIEW_BATCH_UPDATED_EVENT =
  "myfinance:import-review-batch-updated";

export function readTrackedImportBatchId() {
  if (typeof window === "undefined") {
    return null;
  }

  const value = window.sessionStorage.getItem(
    IMPORT_REVIEW_ACTIVE_BATCH_STORAGE_KEY,
  );
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

export function persistTrackedImportBatchId(importBatchId: string | null) {
  if (typeof window === "undefined") {
    return;
  }

  if (importBatchId) {
    window.sessionStorage.setItem(
      IMPORT_REVIEW_ACTIVE_BATCH_STORAGE_KEY,
      importBatchId,
    );
  } else {
    window.sessionStorage.removeItem(IMPORT_REVIEW_ACTIVE_BATCH_STORAGE_KEY);
  }

  window.dispatchEvent(
    new CustomEvent(IMPORT_REVIEW_BATCH_UPDATED_EVENT, {
      detail: { importBatchId },
    }),
  );
}
