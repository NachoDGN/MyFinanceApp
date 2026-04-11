import { NextResponse } from "next/server";
import { ZodError } from "zod";

function buildValidationMessage(error: ZodError): string {
  const issues = error.issues.map((issue) => {
    const path = issue.path.join(".");
    return path ? `${path}: ${issue.message}` : issue.message;
  });
  return issues.join("; ") || "Invalid request.";
}

function resolveErrorStatus(message: string): number {
  if (
    /not configured|required to validate|missing env vars/i.test(message)
  ) {
    return 503;
  }

  if (/signature verification failed/i.test(message)) {
    return 401;
  }

  if (/not found/i.test(message)) {
    return 404;
  }

  if (
    /missing signature headers|outside the allowed window|required|malformed|invalid|does not support/i.test(
      message,
    )
  ) {
    return 400;
  }

  return 500;
}

export function createApiErrorResponse(error: unknown) {
  if (error instanceof ZodError) {
    return NextResponse.json(
      { error: buildValidationMessage(error) },
      { status: 400 },
    );
  }

  const message =
    error instanceof Error ? error.message : "Internal server error.";

  return NextResponse.json(
    { error: message },
    { status: resolveErrorStatus(message) },
  );
}
