import type { DomainDataset, FxRate, Security, SecurityPrice } from "@myfinance/domain";

export interface MarketDataProvider {
  lookupInstrument(query: string): Promise<Security[]>;
  getLatestQuote(symbol: string): Promise<SecurityPrice | null>;
  getHistoricalTimeSeries(symbol: string): Promise<SecurityPrice[]>;
  getFxRate(baseCurrency: string, quoteCurrency: string): Promise<FxRate | null>;
}

export class TwelveDataProvider implements MarketDataProvider {
  constructor(
    private readonly dataset: DomainDataset,
    private readonly apiKey?: string,
  ) {}

  async lookupInstrument(query: string): Promise<Security[]> {
    const normalized = query.trim().toUpperCase();
    return this.dataset.securities.filter(
      (security) =>
        security.displaySymbol.toUpperCase().includes(normalized) ||
        security.name.toUpperCase().includes(normalized),
    );
  }

  async getLatestQuote(symbol: string): Promise<SecurityPrice | null> {
    const security = this.dataset.securities.find(
      (row) =>
        row.displaySymbol.toUpperCase() === symbol.toUpperCase() ||
        row.providerSymbol.toUpperCase() === symbol.toUpperCase(),
    );
    if (!security) return null;

    if (this.apiKey) {
      try {
        const url = new URL("https://api.twelvedata.com/quote");
        url.searchParams.set("symbol", security.providerSymbol);
        url.searchParams.set("apikey", this.apiKey);
        const response = await fetch(url);
        if (response.ok) {
          const payload = (await response.json()) as Record<string, string>;
          return {
            securityId: security.id,
            priceDate: new Date().toISOString().slice(0, 10),
            quoteTimestamp: new Date().toISOString(),
            price: payload.close ?? payload.price ?? "0.00",
            currency: security.quoteCurrency,
            sourceName: "twelve_data",
            isRealtime: payload.is_market_open === "true",
            isDelayed: payload.is_market_open !== "true",
            marketState: payload.is_market_open === "true" ? "open" : "closed",
            rawJson: payload,
            createdAt: new Date().toISOString(),
          };
        }
      } catch {
        // Fall back to the seeded dataset when live lookup is unavailable.
      }
    }

    return (
      this.dataset.securityPrices
        .filter((price) => price.securityId === security.id)
        .sort((a, b) => b.quoteTimestamp.localeCompare(a.quoteTimestamp))[0] ?? null
    );
  }

  async getHistoricalTimeSeries(symbol: string): Promise<SecurityPrice[]> {
    const security = this.dataset.securities.find(
      (row) => row.displaySymbol.toUpperCase() === symbol.toUpperCase(),
    );
    if (!security) return [];
    return this.dataset.securityPrices.filter((price) => price.securityId === security.id);
  }

  async getFxRate(baseCurrency: string, quoteCurrency: string): Promise<FxRate | null> {
    return (
      this.dataset.fxRates.find(
        (rate) =>
          rate.baseCurrency === baseCurrency && rate.quoteCurrency === quoteCurrency,
      ) ?? null
    );
  }
}

export function createMarketDataProvider(
  dataset: DomainDataset,
  apiKey = process.env.TWELVE_DATA_API_KEY,
): MarketDataProvider {
  return new TwelveDataProvider(dataset, apiKey);
}
