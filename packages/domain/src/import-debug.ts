function isImportDebugEnabled() {
  return /^(1|true|yes)$/i.test(process.env.IMPORT_TEMPLATE_DEBUG ?? "");
}

export function logImportDebug(step: string, metadata?: Record<string, unknown>) {
  if (!isImportDebugEnabled()) {
    return;
  }

  console.info("[import-debug]", step, metadata ?? {});
}

export const logTemporaryImportDebug = logImportDebug;
