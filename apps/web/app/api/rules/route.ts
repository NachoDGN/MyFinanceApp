import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { domain } from "../../../lib/action-service";
import { revalidateRulesPaths } from "../../../lib/api-revalidate";

const bodySchema = z.object({
  priority: z.number(),
  scopeJson: z.record(z.string(), z.any()),
  conditionsJson: z.record(z.string(), z.any()),
  outputsJson: z.record(z.string(), z.any()),
  apply: z.boolean().default(true),
});

export async function GET() {
  const rules = await domain.listRules();
  return NextResponse.json(rules);
}

export async function POST(request: NextRequest) {
  const body = bodySchema.parse(await request.json());
  const result = await domain.createRule({
    ...body,
    actorName: "web-api",
    sourceChannel: "web",
  });
  if (result.applied) {
    revalidateRulesPaths();
  }
  return NextResponse.json(result);
}
