import { NextRequest, NextResponse } from "next/server";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";
import { resolveAppState } from "../../../lib/queries";

const domain = new FinanceDomainService(createFinanceRepository());

export async function GET(request: NextRequest) {
  const state = await resolveAppState(Object.fromEntries(request.nextUrl.searchParams));
  const holdings = await domain.listHoldings(state.scope);
  return NextResponse.json(holdings);
}
