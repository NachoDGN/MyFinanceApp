export function logTemporaryImportDebug(
  step: string,
  metadata?: Record<string, unknown>,
) {
  console.info("[TEMPORARY IMPORT DEBUG]", step, metadata ?? {});
}
