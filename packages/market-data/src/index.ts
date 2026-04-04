import type {
  DomainDataset,
  FxRate,
  Security,
  SecurityPrice,
} from "@myfinance/domain";

export interface MarketDataProvider {
  lookupInstrument(query: string): Promise<Security[]>;
  getLatestQuote(symbol: string): Promise<SecurityPrice | null>;
  getHistoricalTimeSeries(symbol: string): Promise<SecurityPrice[]>;
  getFxRate(
    baseCurrency: string,
    quoteCurrency: string,
  ): Promise<FxRate | null>;
}

function readPayloadField<T>(
  payload: Record<string, unknown>,
  keys: string[],
): T | null {
  for (const key of keys) {
    if (key in payload) {
      return payload[key] as T;
    }
  }
  return null;
}

function readPayloadString(payload: Record<string, unknown>, keys: string[]) {
  const value = readPayloadField<unknown>(payload, keys);
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return null;
}

function readPayloadBoolean(payload: Record<string, unknown>, keys: string[]) {
  const value = readPayloadField<unknown>(payload, keys);
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value.toLowerCase() === "true") return true;
    if (value.toLowerCase() === "false") return false;
  }
  return null;
}

function readPayloadTimestamp(
  payload: Record<string, unknown>,
  keys: string[],
) {
  const value = readPayloadField<unknown>(payload, keys);
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value * 1000).toISOString();
  }
  return null;
}

function isWeekend(date: Date) {
  const day = date.getUTCDay();
  return day === 0 || day === 6;
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
        if (isWeekend(new Date())) {
          url.searchParams.set("eod", "true");
        }
        const response = await fetch(url);
        if (response.ok) {
          const payload = (await response.json()) as Record<string, unknown>;
          const price = readPayloadString(payload, ["close", "price"]);
          const priceDate =
            readPayloadString(payload, ["datetime"])?.slice(0, 10) ??
            new Date().toISOString().slice(0, 10);
          const isMarketOpen =
            readPayloadBoolean(payload, ["is_market_open", "isMarketOpen"]) ??
            false;
          const quoteTimestamp =
            readPayloadTimestamp(payload, [
              "last_quote_at",
              "lastQuoteAt",
              "timestamp",
            ]) ?? new Date().toISOString();
          if (!price) {
            return null;
          }
          return {
            securityId: security.id,
            priceDate,
            quoteTimestamp,
            price,
            currency: security.quoteCurrency,
            sourceName: "twelve_data",
            isRealtime: isMarketOpen,
            isDelayed: !isMarketOpen,
            marketState: isMarketOpen ? "open" : "closed",
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
        .sort((a, b) => b.quoteTimestamp.localeCompare(a.quoteTimestamp))[0] ??
      null
    );
  }

  async getHistoricalTimeSeries(symbol: string): Promise<SecurityPrice[]> {
    const security = this.dataset.securities.find(
      (row) => row.displaySymbol.toUpperCase() === symbol.toUpperCase(),
    );
    if (!security) return [];
    return this.dataset.securityPrices.filter(
      (price) => price.securityId === security.id,
    );
  }

  async getFxRate(
    baseCurrency: string,
    quoteCurrency: string,
  ): Promise<FxRate | null> {
    return (
      this.dataset.fxRates.find(
        (rate) =>
          rate.baseCurrency === baseCurrency &&
          rate.quoteCurrency === quoteCurrency,
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
