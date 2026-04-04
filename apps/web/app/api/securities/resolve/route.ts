import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";
import { revalidateFinanceReadPaths } from "../../../../lib/api-revalidate";

const domain = new FinanceDomainService(createFinanceRepository());

const bodySchema = z.object({
  transactionId: z.string(),
  securityId: z.string(),
  apply: z.boolean().default(true),
});

export async function POST(request: NextRequest) {
  const body = bodySchema.parse(await request.json());
  const result = await domain.updateTransaction({
    transactionId: body.transactionId,
    patch: {
      securityId: body.securityId,
      needsReview: false,
      reviewReason: null,
    },
    apply: body.apply,
    actorName: "web-api",
    sourceChannel: "web",
  });
  if (result.applied) {
    revalidateFinanceReadPaths();
  }
  return NextResponse.json(result);
}
