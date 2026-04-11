import { revalidatePath } from "next/cache";

const financeReadPaths = [
  "/",
  "/dashboard",
  "/accounts",
  "/income",
  "/insights",
  "/investments",
  "/settings",
  "/spending",
  "/templates",
  "/transactions",
] as const;

export function revalidateFinanceReadPaths() {
  for (const path of financeReadPaths) {
    revalidatePath(path);
  }
}

export function revalidateWorkspacePaths() {
  revalidateFinanceReadPaths();
  revalidatePath("/imports");
  revalidatePath("/rules");
}

export function revalidateImportPaths() {
  revalidateFinanceReadPaths();
  revalidatePath("/imports");
}

export function revalidateRulesPaths() {
  revalidatePath("/rules");
}

export function revalidateAccountsPath() {
  revalidatePath("/accounts");
}

export function revalidateTemplatePaths() {
  revalidatePath("/templates");
  revalidatePath("/imports");
}

export function revalidatePromptPaths() {
  revalidatePath("/prompts");
}
