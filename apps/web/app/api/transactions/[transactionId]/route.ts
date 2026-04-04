import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";
import {
  revalidateFinanceReadPaths,
  revalidateRulesPaths,
} from "../../../../lib/api-revalidate";

const domain = new FinanceDomainService(createFinanceRepository());

const bodySchema = z.object({
  patch: z.record(z.string(), z.any()),
  createRuleFromTransaction: z.boolean().optional(),
  apply: z.boolean().default(true),
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
  if (result.applied) {
    revalidateFinanceReadPaths();
    if (body.createRuleFromTransaction) {
      revalidateRulesPaths();
    }
  }
  return NextResponse.json(result);
}
