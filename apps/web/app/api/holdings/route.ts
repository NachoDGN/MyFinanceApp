import { NextRequest, NextResponse } from "next/server";

import { buildInvestmentsReadModel } from "@myfinance/analytics";
import { resolveAppState } from "../../../lib/queries";

export async function GET(request: NextRequest) {
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const holdings = buildInvestmentsReadModel(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
    referenceDate: state.referenceDate,
  }).holdings;
  return NextResponse.json(holdings);
}
