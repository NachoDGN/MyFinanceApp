import { NextRequest, NextResponse } from "next/server";

import { buildDashboardSummary } from "@myfinance/analytics";
import { createFinanceRepository } from "@myfinance/db";
import { resolveAppState } from "../../../../lib/queries";

export async function GET(request: NextRequest) {
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const summary = buildDashboardSummary(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
    referenceDate: state.referenceDate,
  });
  return NextResponse.json(summary);
}
