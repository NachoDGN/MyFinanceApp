import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { NextConfig } from "next";

const webAppDirectory = dirname(fileURLToPath(import.meta.url));
const workspaceRoot = resolve(webAppDirectory, "../..");

function loadRootEnvFile(filename: string) {
  const filePath = resolve(workspaceRoot, filename);
  if (!existsSync(filePath)) return;

  const contents = readFileSync(filePath, "utf8");
  for (const line of contents.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex <= 0) continue;

    const key = trimmed.slice(0, separatorIndex).trim();
    if (!key || process.env[key]) continue;

    let value = trimmed.slice(separatorIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

// Temporary monorepo env bridge: load repo-root env files for the Next app.
loadRootEnvFile(".env.local");
loadRootEnvFile(".env");

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
