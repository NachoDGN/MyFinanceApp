import { NextResponse } from "next/server";

import { getReviewReanalysisJobStatus } from "@myfinance/db";
import { revalidateFinanceReadPaths } from "../../../../lib/api-revalidate";

export async function GET(
  _request: Request,
  context: { params: Promise<{ jobId: string }> },
) {
  try {
    const { jobId } = await context.params;
    const job = await getReviewReanalysisJobStatus(jobId);
    if (job.status === "completed") {
      revalidateFinanceReadPaths();
    }
    return NextResponse.json(job);
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Review job lookup failed.",
      },
      { status: 400 },
    );
  }
}
