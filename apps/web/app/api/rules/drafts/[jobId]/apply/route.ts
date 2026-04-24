import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { domain } from "../../../../../../lib/action-service";
import { revalidateRulesPaths } from "../../../../../../lib/api-revalidate";

const bodySchema = z.object({
  apply: z.boolean().default(true),
});

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ jobId: string }> },
) {
  const body = bodySchema.parse(await request.json());
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
  return NextResponse.json(result);
}
