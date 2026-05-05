import type { NextRequest } from "next/server";
import { z } from "zod";

import { domain } from "../../../../lib/action-service";
import {
  jsonResponse,
  parseJsonRequest,
  withApiErrors,
} from "../../../../lib/api-handlers";
import { revalidateRulesPaths } from "../../../../lib/api-revalidate";

const bodySchema = z.object({
  requestText: z.string().min(8),
  apply: z.boolean().default(true),
});

export const GET = withApiErrors(async () => {
  const drafts = await domain.listRuleDrafts();
  return jsonResponse(drafts);
});

export const POST = withApiErrors(async (request: NextRequest) => {
  const body = await parseJsonRequest(request, bodySchema);
  const result = await domain.queueRuleDraft({
    requestText: body.requestText,
    actorName: "web-api",
    sourceChannel: "web",
    apply: body.apply,
  });
  if (result.applied) {
    revalidateRulesPaths();
  }
  return jsonResponse(result);
});
