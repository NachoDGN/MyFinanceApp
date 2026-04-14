"use client";

import { useEffect, useState } from "react";

import {
  IMPORT_REVIEW_BATCH_UPDATED_EVENT,
  markTrackedImportBatchAutoOpenHandled,
  persistTrackedImportBatchId,
  readPendingAutoOpenImportBatchId,
  readTrackedImportBatchId,
} from "../lib/import-review-session";
import { ImportReviewModal } from "./import-review-modal";

export function ImportReviewModalHost() {
  const [importBatchId, setImportBatchId] = useState<string | null>(null);
  const [shouldAutoOpen, setShouldAutoOpen] = useState(false);

  useEffect(() => {
    const syncTrackedBatch = () => {
      const trackedImportBatchId = readTrackedImportBatchId();
      setImportBatchId(trackedImportBatchId);
      setShouldAutoOpen(
        trackedImportBatchId !== null &&
          readPendingAutoOpenImportBatchId() === trackedImportBatchId,
      );
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
      shouldAutoOpen={shouldAutoOpen}
      onAutoOpenHandled={(openedImportBatchId) => {
        markTrackedImportBatchAutoOpenHandled(openedImportBatchId);
        setShouldAutoOpen(false);
      }}
      onTrackedBatchSettled={(settledImportBatchId) => {
        if (readTrackedImportBatchId() === settledImportBatchId) {
          persistTrackedImportBatchId(null);
          setImportBatchId(null);
          setShouldAutoOpen(false);
        }
      }}
    />
  );
}
