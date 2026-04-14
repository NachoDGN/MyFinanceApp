import { NextRequest, NextResponse } from "next/server";

import { getImportBatchReviewQueueState } from "@myfinance/db";

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ importBatchId: string }> },
) {
  try {
    const { importBatchId } = await context.params;
    const reviewedSourceTransactionIds = request.nextUrl.searchParams.getAll(
      "reviewedSourceTransactionId",
    );
    const queueState = await getImportBatchReviewQueueState({
      importBatchId,
      reviewedSourceTransactionIds,
    });

    return NextResponse.json(queueState);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Import review queue lookup failed.",
      },
      { status: 400 },
    );
  }
}
