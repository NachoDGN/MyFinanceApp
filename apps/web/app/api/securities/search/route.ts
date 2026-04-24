import { NextRequest, NextResponse } from "next/server";

import { createMarketDataProvider } from "@myfinance/market-data";
import { repository } from "../../../../lib/action-service";

export async function GET(request: NextRequest) {
  const dataset = await repository.getDataset();
  const provider = createMarketDataProvider(dataset);
  const query = request.nextUrl.searchParams.get("q") ?? "";
  const results = await provider.lookupInstrument(query);
  return NextResponse.json({
    schemaVersion: "v1",
    query,
    results,
    generatedAt: new Date().toISOString(),
  });
}
