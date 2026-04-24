import { NextRequest, NextResponse } from "next/server";

import { domain } from "../../../lib/action-service";
import { resolveAppState } from "../../../lib/queries";

export async function GET(request: NextRequest) {
  const state = await resolveAppState(
    Object.fromEntries(request.nextUrl.searchParams),
  );
  const transactions = await domain.listTransactions(state.scope, {
    referenceDate: state.referenceDate,
    period: state.period,
    query: state.transactionSearchQuery,
  });
  return NextResponse.json(transactions);
}
