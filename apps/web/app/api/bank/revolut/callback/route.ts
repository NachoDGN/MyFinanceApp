import { NextRequest, NextResponse } from "next/server";

import { completeRevolutAuthorization } from "@myfinance/db";
import { createApiErrorResponse } from "../../../../../lib/api-errors";
import { revalidateFinanceReadPaths } from "../../../../../lib/api-revalidate";

export async function GET(request: NextRequest) {
  const code = request.nextUrl.searchParams.get("code") ?? "";
  const state = request.nextUrl.searchParams.get("state") ?? "";

  if (!code || !state) {
    return NextResponse.json(
      { error: "code and state are required." },
      { status: 400 },
    );
  }

  try {
    await completeRevolutAuthorization({ code, state });
    revalidateFinanceReadPaths();
    return NextResponse.redirect(
      new URL("/accounts?revolut=connected", request.url),
    );
  } catch (error) {
    return createApiErrorResponse(error);
  }
}
