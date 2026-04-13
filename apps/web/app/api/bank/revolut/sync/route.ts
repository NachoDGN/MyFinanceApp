import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createApiErrorResponse } from "../../../../../lib/api-errors";
import { revalidateFinanceReadPaths } from "../../../../../lib/api-revalidate";
import { queueAndRunRevolutSync } from "../../../../../lib/revolut-sync";

const syncSchema = z.object({
  connectionId: z.string().uuid(),
});

export async function POST(request: NextRequest) {
  try {
    const body = syncSchema.parse(await request.json());
    const result = await queueAndRunRevolutSync({
      connectionId: body.connectionId,
      trigger: "manual_sync",
    });
    revalidateFinanceReadPaths();
    return NextResponse.json(result);
  } catch (error) {
    return createApiErrorResponse(error);
  }
}
