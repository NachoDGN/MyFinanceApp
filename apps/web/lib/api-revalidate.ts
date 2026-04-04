import { revalidatePath } from "next/cache";

const financeReadPaths = [
  "/",
  "/accounts",
  "/income",
  "/insights",
  "/investments",
  "/spending",
  "/transactions",
] as const;

export function revalidateFinanceReadPaths() {
  for (const path of financeReadPaths) {
    revalidatePath(path);
  }
}

export function revalidateImportPaths() {
  revalidateFinanceReadPaths();
  revalidatePath("/imports");
}

export function revalidateRulesPaths() {
  revalidatePath("/rules");
}
