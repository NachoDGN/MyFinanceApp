import { NextRequest, NextResponse } from "next/server";

import { beginRevolutAuthorization } from "@myfinance/db";
import { createApiErrorResponse } from "../../../../../lib/api-errors";

export async function GET(request: NextRequest) {
  const entityId = request.nextUrl.searchParams.get("entityId") ?? "";
  if (!entityId) {
    return NextResponse.json(
      { error: "entityId is required." },
      { status: 400 },
    );
  }

  try {
    const result = await beginRevolutAuthorization({ entityId });
    return NextResponse.redirect(result.url);
  } catch (error) {
    return createApiErrorResponse(error);
  }
}
