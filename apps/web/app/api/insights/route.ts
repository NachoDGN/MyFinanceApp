import { NextRequest, NextResponse } from "next/server";

import { buildInsights } from "@myfinance/analytics";
import { createFinanceRepository } from "@myfinance/db";
import { resolveAppState } from "../../../lib/queries";

const repository = createFinanceRepository();

export async function GET(request: NextRequest) {
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const dataset = await repository.getDataset();
  const insights = buildInsights(dataset, state.scope, {
    referenceDate: state.referenceDate,
  });
  return NextResponse.json({
    schemaVersion: "v1",
    scope: state.scope,
    insights,
    generatedAt: new Date().toISOString(),
  });
}
