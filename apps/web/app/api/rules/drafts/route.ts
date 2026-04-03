import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

const domain = new FinanceDomainService(createFinanceRepository());

const bodySchema = z.object({
  requestText: z.string().min(8),
  apply: z.boolean().default(true),
});

export async function GET() {
  const drafts = await domain.listRuleDrafts();
  return NextResponse.json(drafts);
}

export async function POST(request: NextRequest) {
  const body = bodySchema.parse(await request.json());
  const result = await domain.queueRuleDraft({
    requestText: body.requestText,
    actorName: "web-api",
    sourceChannel: "web",
    apply: body.apply,
  });
  return NextResponse.json(result);
}
