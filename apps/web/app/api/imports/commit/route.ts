import type { NextRequest } from "next/server";

import { domain } from "../../../../lib/action-service";
import { importExecutionSchema } from "../../../../lib/action-schemas";
import {
  jsonResponse,
  parseJsonRequest,
  withApiErrors,
} from "../../../../lib/api-handlers";
import { revalidateImportPaths } from "../../../../lib/api-revalidate";

export const POST = withApiErrors(async (request: NextRequest) => {
  const body = await parseJsonRequest(request, importExecutionSchema);
  const result = await domain.commitImport(body);
  revalidateImportPaths();
  return jsonResponse(result);
});
