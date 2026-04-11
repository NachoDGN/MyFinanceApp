import { NextRequest, NextResponse } from "next/server";

import { processRevolutWebhookEvent } from "@myfinance/db";
import { createApiErrorResponse } from "../../../../../lib/api-errors";

export async function POST(request: NextRequest) {
  try {
    const body = await request.text();
    const headers = Object.fromEntries(request.headers.entries());
    const result = await processRevolutWebhookEvent({ headers, body });
    return NextResponse.json(result);
  } catch (error) {
    return createApiErrorResponse(error);
  }
}
