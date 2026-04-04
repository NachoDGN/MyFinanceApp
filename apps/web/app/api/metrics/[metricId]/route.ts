import { NextRequest, NextResponse } from "next/server";

import { buildMetricResult } from "@myfinance/analytics";
import { resolveAppState } from "../../../../lib/queries";

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ metricId: string }> },
) {
  const { metricId } = await context.params;
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const metric = buildMetricResult(
    state.dataset,
    state.scope,
    state.currency,
    metricId,
    { referenceDate: state.referenceDate },
  );
  return NextResponse.json(metric);
}
