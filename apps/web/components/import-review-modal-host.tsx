"use client";

import { useEffect, useState } from "react";

import {
  IMPORT_REVIEW_BATCH_UPDATED_EVENT,
  persistTrackedImportBatchId,
  readTrackedImportBatchId,
} from "../lib/import-review-session";
import { ImportReviewModal } from "./import-review-modal";

export function ImportReviewModalHost() {
  const [importBatchId, setImportBatchId] = useState<string | null>(null);

  useEffect(() => {
    const syncTrackedBatch = () => {
      setImportBatchId(readTrackedImportBatchId());
    };

    syncTrackedBatch();
    window.addEventListener(IMPORT_REVIEW_BATCH_UPDATED_EVENT, syncTrackedBatch);

    return () => {
      window.removeEventListener(
        IMPORT_REVIEW_BATCH_UPDATED_EVENT,
        syncTrackedBatch,
      );
    };
  }, []);

  return (
    <ImportReviewModal
      importBatchId={importBatchId}
      onTrackedBatchSettled={(settledImportBatchId) => {
        if (readTrackedImportBatchId() === settledImportBatchId) {
          persistTrackedImportBatchId(null);
          setImportBatchId(null);
        }
      }}
    />
  );
}
