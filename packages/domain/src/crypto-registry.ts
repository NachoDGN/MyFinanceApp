import type { DomainDataset, Security } from "./types";

const CRYPTO_ASSETS = [
  { id: "00000000-0000-0000-0000-00000000c001", currency: "BTC", name: "Bitcoin" },
  { id: "00000000-0000-0000-0000-00000000c002", currency: "ETH", name: "Ethereum" },
] as const;

const CRYPTO_CURRENCY_CODES: ReadonlySet<string> = new Set(
  CRYPTO_ASSETS.map((asset) => asset.currency),
);

export const CRYPTO_SECURITY_SPECS = CRYPTO_ASSETS.map((asset) => ({
  ...asset,
  providerName: "twelve_data",
  providerSymbol: `${asset.currency}/EUR`,
  canonicalSymbol: asset.currency,
  displaySymbol: asset.currency,
  exchangeName: "Coinbase Pro",
  micCode: null,
  quoteCurrency: "EUR",
}));

export type CryptoSecuritySpec = (typeof CRYPTO_SECURITY_SPECS)[number];

export function isCryptoCurrency(currency: string | null | undefined) {
  return typeof currency === "string" && CRYPTO_CURRENCY_CODES.has(currency);
}

export function getCryptoSecuritySpec(currency: string | null | undefined) {
  const normalized = currency?.trim().toUpperCase();
  return (
    CRYPTO_SECURITY_SPECS.find((spec) => spec.currency === normalized) ?? null
  );
}

export function createCryptoSecurityFromSpec(
  spec: CryptoSecuritySpec,
  createdAt = "2026-01-01T00:00:00Z",
): Security {
  return {
    id: spec.id,
    providerName: spec.providerName,
    providerSymbol: spec.providerSymbol,
    canonicalSymbol: spec.canonicalSymbol,
    displaySymbol: spec.displaySymbol,
    name: spec.name,
    exchangeName: spec.exchangeName,
    micCode: spec.micCode,
    assetType: "crypto",
    quoteCurrency: spec.quoteCurrency,
    country: null,
    isin: null,
    figi: null,
    active: true,
    metadataJson: {
      instrumentType: "crypto",
      baseCurrency: spec.currency,
      quoteCurrency: spec.quoteCurrency,
    },
    lastPriceRefreshAt: null,
    createdAt,
  };
}

export function findCryptoSecurity(
  dataset: Pick<DomainDataset, "securities">,
  currency: string | null | undefined,
) {
  const spec = getCryptoSecuritySpec(currency);
  if (!spec) {
    return null;
  }

  return (
    dataset.securities.find(
      (security) =>
        security.providerName === spec.providerName &&
        security.providerSymbol.toUpperCase() === spec.providerSymbol,
    ) ??
    dataset.securities.find(
      (security) =>
        security.assetType === "crypto" &&
        security.displaySymbol.toUpperCase() === spec.displaySymbol,
    ) ??
    null
  );
}
