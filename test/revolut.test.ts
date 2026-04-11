import test from "node:test";
import assert from "node:assert/strict";

import { buildRevolutAuthorizationUrl } from "../packages/db/src/revolut.ts";

test("revolut authorization URL preserves the configured redirect URI", () => {
  const url = buildRevolutAuthorizationUrl(
    {
      clientId: "client-id",
      privateKeyPem: "pem",
      redirectUri: "https://example.com/revolut-callback",
      authBaseUrl: "https://business.revolut.com",
      apiBaseUrl: "https://b2b.revolut.com/api/1.0",
      webhookSigningSecret: null,
      masterKey: "secret",
      syncIntervalMinutes: 30,
      initialBackfillDays: 365,
      syncLookbackMinutes: 30,
    },
    "signed-state",
  );

  const parsed = new URL(url);
  assert.equal(parsed.origin, "https://business.revolut.com");
  assert.equal(parsed.pathname, "/app-confirm");
  assert.equal(
    parsed.searchParams.get("redirect_uri"),
    "https://example.com/revolut-callback",
  );
  assert.equal(parsed.searchParams.get("scope"), "READ");
});

test("revolut API URLs keep the api version path when given leading slashes", () => {
  const url = new URL(
    "/accounts".replace(/^\/+/, ""),
    "https://b2b.revolut.com/api/1.0/",
  );
  url.searchParams.set("count", "1000");

  assert.equal(url.toString(), "https://b2b.revolut.com/api/1.0/accounts?count=1000");
});
