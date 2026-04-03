import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { NextRequest } from "next/server";
import { z } from "zod";

const jsonImportSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
  originalFilename: z.string().optional(),
  filePath: z.string().optional(),
});

const multipartSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
});

type ParsedImportRequest = {
  input: z.infer<typeof jsonImportSchema>;
  cleanup: () => Promise<void>;
};

export async function parseImportRequest(request: NextRequest): Promise<ParsedImportRequest> {
  const contentType = request.headers.get("content-type") ?? "";

  if (!contentType.includes("multipart/form-data")) {
    return {
      input: jsonImportSchema.parse(await request.json()),
      cleanup: async () => {},
    };
  }

  const formData = await request.formData();
  const fields = multipartSchema.parse({
    accountId: formData.get("accountId"),
    templateId: formData.get("templateId"),
  });
  const file = formData.get("file");
  if (!file || typeof file !== "object" || typeof (file as File).arrayBuffer !== "function") {
    throw new Error("A file upload is required.");
  }

  const uploadDirectory = join(tmpdir(), "myfinance-imports", randomUUID());
  await mkdir(uploadDirectory, { recursive: true });
  const filePath = join(uploadDirectory, file.name || "upload.bin");
  await writeFile(filePath, Buffer.from(await file.arrayBuffer()));

  return {
    input: {
      accountId: fields.accountId,
      templateId: fields.templateId,
      originalFilename: file.name,
      filePath,
    },
    cleanup: async () => {
      await rm(uploadDirectory, { recursive: true, force: true });
    },
  };
}
