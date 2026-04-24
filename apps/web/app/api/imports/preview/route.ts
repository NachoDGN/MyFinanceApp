import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { domain } from "../../../../lib/action-service";

const importSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
  originalFilename: z.string().optional(),
  filePath: z.string().optional(),
});

export async function POST(request: NextRequest) {
  const body = importSchema.parse(await request.json());
  const result = await domain.previewImport(body);
  return NextResponse.json(result);
}
