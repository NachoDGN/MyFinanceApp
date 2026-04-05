import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { queueTransactionReviewReanalysis } from "@myfinance/db";

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
    const result = await queueTransactionReviewReanalysis({
      transactionId,
      reviewContext: body.reviewContext,
      actorName: "web-review-editor",
      sourceChannel: "web",
    });

    return NextResponse.json(result, { status: 202 });
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
