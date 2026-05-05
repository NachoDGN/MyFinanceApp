import type { NextRequest } from "next/server";

import { domain } from "../../../../lib/action-service";
import { importExecutionSchema } from "../../../../lib/action-schemas";
import {
  jsonResponse,
  parseJsonRequest,
  withApiErrors,
} from "../../../../lib/api-handlers";

export const POST = withApiErrors(async (request: NextRequest) => {
  const body = await parseJsonRequest(request, importExecutionSchema);
  const result = await domain.previewImport(body);
  return jsonResponse(result);
});
