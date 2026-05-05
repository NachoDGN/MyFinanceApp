import type { NextRequest } from "next/server";
import { z } from "zod";

import { domain } from "../../../lib/action-service";
import {
  jsonResponse,
  parseJsonRequest,
  withApiErrors,
} from "../../../lib/api-handlers";
import { revalidateRulesPaths } from "../../../lib/api-revalidate";

const bodySchema = z.object({
  priority: z.number(),
  scopeJson: z.record(z.string(), z.any()),
  conditionsJson: z.record(z.string(), z.any()),
  outputsJson: z.record(z.string(), z.any()),
  apply: z.boolean().default(true),
});

export const GET = withApiErrors(async () => {
  const rules = await domain.listRules();
  return jsonResponse(rules);
});

export const POST = withApiErrors(async (request: NextRequest) => {
  const body = await parseJsonRequest(request, bodySchema);
  const result = await domain.createRule({
    ...body,
    actorName: "web-api",
    sourceChannel: "web",
  });
  if (result.applied) {
    revalidateRulesPaths();
  }
  return jsonResponse(result);
});
