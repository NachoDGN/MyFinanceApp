import type { Account, AccountType, AssetDomain } from "./types";

const INVESTMENT_ACCOUNT_TYPES = new Set<AccountType>([
  "brokerage_account",
  "brokerage_cash",
]);

export function isBrokerageCashAccountType(accountType: AccountType) {
  return accountType === "brokerage_cash";
}

export function isInvestmentAccountType(accountType: AccountType) {
  return INVESTMENT_ACCOUNT_TYPES.has(accountType);
}

export function resolveAccountAssetDomain(
  accountType: AccountType,
): AssetDomain {
  return isInvestmentAccountType(accountType) ? "investment" : "cash";
}

export function normalizeAccountAssetDomain<
  T extends Pick<Account, "accountType" | "assetDomain">,
>(account: T): T {
  const assetDomain = resolveAccountAssetDomain(account.accountType);
  return account.assetDomain === assetDomain
    ? account
    : {
        ...account,
        assetDomain,
      };
}
