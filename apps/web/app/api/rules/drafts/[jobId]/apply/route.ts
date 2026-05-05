import type { NextRequest } from "next/server";
import { z } from "zod";

import { domain } from "../../../../../../lib/action-service";
import {
  jsonResponse,
  parseJsonRequest,
  withApiErrors,
} from "../../../../../../lib/api-handlers";
import { revalidateRulesPaths } from "../../../../../../lib/api-revalidate";

const bodySchema = z.object({
  apply: z.boolean().default(true),
});

export const POST = withApiErrors(
  async (
    request: NextRequest,
    context: { params: Promise<{ jobId: string }> },
  ) => {
    const body = await parseJsonRequest(request, bodySchema);
    const { jobId } = await context.params;
    const result = await domain.applyRuleDraft({
      jobId,
      actorName: "web-api",
      sourceChannel: "web",
      apply: body.apply,
    });
    if (result.applied) {
      revalidateRulesPaths();
    }
    return jsonResponse(result);
  },
);
