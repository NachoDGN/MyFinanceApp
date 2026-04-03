import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

const domain = new FinanceDomainService(createFinanceRepository());

const bodySchema = z.object({
  patch: z.record(z.string(), z.any()),
  createRuleFromTransaction: z.boolean().optional(),
  apply: z.boolean().default(false),
});

export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ transactionId: string }> },
) {
  const { transactionId } = await context.params;
  const body = bodySchema.parse(await request.json());
  const result = await domain.updateTransaction({
    transactionId,
    patch: body.patch,
    createRuleFromTransaction: body.createRuleFromTransaction,
    apply: body.apply,
    actorName: "web-api",
    sourceChannel: "web",
  });
  return NextResponse.json(result);
}
