import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

const domain = new FinanceDomainService(createFinanceRepository());

const importSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
  originalFilename: z.string().optional(),
  filePath: z.string().optional(),
});

export async function POST(request: NextRequest) {
  const body = importSchema.parse(await request.json());
  const result = await domain.commitImport(body);
  return NextResponse.json(result);
}
