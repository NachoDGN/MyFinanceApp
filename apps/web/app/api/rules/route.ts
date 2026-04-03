import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

const domain = new FinanceDomainService(createFinanceRepository());

const bodySchema = z.object({
  priority: z.number(),
  scopeJson: z.record(z.string(), z.any()),
  conditionsJson: z.record(z.string(), z.any()),
  outputsJson: z.record(z.string(), z.any()),
  apply: z.boolean().default(false),
});

export async function GET() {
  const rules = await domain.listRules();
  return NextResponse.json(rules);
}

export async function POST(request: NextRequest) {
  const body = bodySchema.parse(await request.json());
  const result = await domain.createRule({
    ...body,
    actorName: "web-api",
    sourceChannel: "web",
  });
  return NextResponse.json(result);
}
