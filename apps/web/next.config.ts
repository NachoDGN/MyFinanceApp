import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  transpilePackages: [
    "@myfinance/analytics",
    "@myfinance/classification",
    "@myfinance/db",
    "@myfinance/domain",
    "@myfinance/market-data",
  ],
};

export default nextConfig;
