import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { resolveAppState } from "../../../../lib/queries";

const askSchema = z.object({
  question: z.string().trim().min(1).max(1000),
  scope: z.string().optional(),
  currency: z.enum(["EUR", "USD"]).optional(),
  period: z.string().optional(),
  asOf: z.string().optional(),
  start: z.string().optional(),
  end: z.string().optional(),
});

export async function POST(request: NextRequest) {
  try {
    const body = askSchema.parse(await request.json());
    const state = await resolveAppState({
      scope: body.scope,
      currency: body.currency,
      period: body.period,
      asOf: body.asOf,
      start: body.start,
      end: body.end,
    });
    const answer = await state.domainService.answerTransactionQuestion({
      question: body.question,
      scope: state.scope,
      period: state.period,
      referenceDate: state.referenceDate,
      currency: state.currency,
    });

    return NextResponse.json(answer);
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Transaction question failed.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
