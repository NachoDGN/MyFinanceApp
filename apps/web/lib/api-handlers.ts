import { type NextRequest, NextResponse } from "next/server";
import type { z } from "zod";

import { createApiErrorResponse } from "./api-errors";

type ApiHandler<TArgs extends unknown[]> = (
  ...args: TArgs
) => Promise<Response> | Response;

export function withApiErrors<TArgs extends unknown[]>(
  handler: ApiHandler<TArgs>,
) {
  return async (...args: TArgs) => {
    try {
      return await handler(...args);
    } catch (error) {
      return createApiErrorResponse(error);
    }
  };
}

export async function parseJsonRequest<TSchema extends z.ZodTypeAny>(
  request: NextRequest,
  schema: TSchema,
): Promise<z.infer<TSchema>> {
  return schema.parse(await request.json());
}

export function jsonResponse<T>(value: T) {
  return NextResponse.json(value);
}
