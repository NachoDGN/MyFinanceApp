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

function revalidatePaths(paths: readonly string[]) {
  for (const path of paths) {
    revalidatePath(path);
  }
}

export const revalidateFinanceReadPaths = () =>
  revalidatePaths(financeReadPaths);

export const revalidateWorkspacePaths = () =>
  revalidatePaths([...financeReadPaths, "/imports", "/rules"]);

export const revalidateImportPaths = () =>
  revalidatePaths([...financeReadPaths, "/imports"]);

export const revalidateRulesPaths = () => revalidatePath("/rules");

export const revalidateAccountsPath = () => revalidatePath("/accounts");

export const revalidateTemplatePaths = () =>
  revalidatePaths(["/templates", "/imports"]);

export const revalidatePromptPaths = () => revalidatePath("/prompts");
