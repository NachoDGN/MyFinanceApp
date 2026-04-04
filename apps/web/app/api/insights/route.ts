import { NextRequest, NextResponse } from "next/server";

import { buildInsights } from "@myfinance/analytics";
import { resolveAppState } from "../../../lib/queries";

export async function GET(request: NextRequest) {
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const insights = buildInsights(state.dataset, state.scope, {
    referenceDate: state.referenceDate,
  });
  return NextResponse.json({
    schemaVersion: "v1",
    scope: state.scope,
    insights,
    generatedAt: new Date().toISOString(),
  });
}
