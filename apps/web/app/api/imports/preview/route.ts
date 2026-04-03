import { NextRequest, NextResponse } from "next/server";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

import { parseImportRequest } from "../shared";

export const runtime = "nodejs";

const domain = new FinanceDomainService(createFinanceRepository());

export async function POST(request: NextRequest) {
  const parsed = await parseImportRequest(request);
  try {
    const result = await domain.previewImport(parsed.input);
    return NextResponse.json(result);
  } finally {
    await parsed.cleanup();
  }
}
