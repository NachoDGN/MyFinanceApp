"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useState, useTransition } from "react";

import { refreshOwnedStockPricesAction } from "../app/actions";

function formatPlural(count: number, noun: string) {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

export function InvestmentPriceRefreshButton() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);

  function handleRefresh() {
    startTransition(async () => {
      setFeedback(null);
      try {
        const result = await refreshOwnedStockPricesAction();

        if (result.totalTrackedStocks === 0) {
          setFeedback(
            "No open stock or ETF positions are available to refresh.",
          );
          router.refresh();
          return;
        }

        if (result.refreshedCount === 0) {
          const skippedLabel =
            result.skippedDetails.length > 0
              ? ` ${result.skippedDetails
                  .map((item) => `${item.symbol}: ${item.reason}`)
                  .join(" ")}`
              : "";
          setFeedback(
            `No fresh quotes were returned for ${formatPlural(result.totalTrackedStocks, "holding")}.${skippedLabel}`,
          );
          router.refresh();
          return;
        }

        const skippedNote =
          result.skippedCount > 0
            ? ` ${result.skippedDetails
                .map((item) => `${item.symbol}: ${item.reason}`)
                .join(" ")}`
            : "";
        setFeedback(
          `Updated current prices for ${formatPlural(result.refreshedCount, "holding")}.${skippedNote}`,
        );

        const currentAsOf = searchParams.get("asOf");
        if (
          result.latestPriceDate &&
          (!currentAsOf || currentAsOf < result.latestPriceDate)
        ) {
          const nextParams = new URLSearchParams(searchParams.toString());
          nextParams.set("asOf", result.latestPriceDate);
          router.replace(`${pathname}?${nextParams.toString()}`);
          return;
        }

        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error
            ? error.message
            : "Current price refresh failed.",
        );
      }
    });
  }

  return (
    <div className="investment-price-refresh">
      <button
        className="btn-pill"
        type="button"
        disabled={isPending}
        onClick={handleRefresh}
      >
        {isPending ? "Updating current prices..." : "Update current prices"}
      </button>
      {feedback ? <div className="status-note">{feedback}</div> : null}
    </div>
  );
}
