import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { domain } from "../../../../lib/action-service";
import { revalidateRulesPaths } from "../../../../lib/api-revalidate";

const bodySchema = z.object({
  requestText: z.string().min(8),
  apply: z.boolean().default(true),
});

export async function GET() {
  const drafts = await domain.listRuleDrafts();
  return NextResponse.json(drafts);
}

export async function POST(request: NextRequest) {
  const body = bodySchema.parse(await request.json());
  const result = await domain.queueRuleDraft({
    requestText: body.requestText,
    actorName: "web-api",
    sourceChannel: "web",
    apply: body.apply,
  });
  if (result.applied) {
    revalidateRulesPaths();
  }
  return NextResponse.json(result);
}
