import { NextRequest, NextResponse } from "next/server";

import { buildMetricResult } from "@myfinance/analytics";
import { createFinanceRepository } from "@myfinance/db";
import { resolveAppState } from "../../../../lib/queries";

const repository = createFinanceRepository();

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ metricId: string }> },
) {
  const { metricId } = await context.params;
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const dataset = await repository.getDataset();
  const metric = buildMetricResult(dataset, state.scope, state.currency, metricId);
  return NextResponse.json(metric);
}
