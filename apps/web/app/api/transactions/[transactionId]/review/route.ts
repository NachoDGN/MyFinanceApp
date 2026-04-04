import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { reanalyzeTransactionReview } from "@myfinance/db";
import { revalidateFinanceReadPaths } from "../../../../../lib/api-revalidate";

const bodySchema = z.object({
  reviewContext: z.string().trim().min(1),
});

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ transactionId: string }> },
) {
  try {
    const { transactionId } = await context.params;
    const body = bodySchema.parse(await request.json());
    const result = await reanalyzeTransactionReview({
      transactionId,
      reviewContext: body.reviewContext,
      actorName: "web-review-editor",
      sourceChannel: "web",
    });

    revalidateFinanceReadPaths();
    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Transaction review update failed.",
      },
      { status: 400 },
    );
  }
}
