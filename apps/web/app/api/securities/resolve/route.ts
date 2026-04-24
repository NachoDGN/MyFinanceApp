import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { domain } from "../../../../lib/action-service";
import { revalidateFinanceReadPaths } from "../../../../lib/api-revalidate";

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
