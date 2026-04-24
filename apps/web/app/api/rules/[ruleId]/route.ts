import { NextRequest, NextResponse } from "next/server";

import { repository } from "../../../../lib/action-service";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ ruleId: string }> },
) {
  const { ruleId } = await context.params;
  const dataset = await repository.getDataset();
  const rule = dataset.rules.find((row) => row.id === ruleId);
  return NextResponse.json(rule ?? null);
}
