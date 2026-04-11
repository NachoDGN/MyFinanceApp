import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  createPrivateKey,
  createSign,
  randomBytes,
} from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { z } from "zod";

const dbPackageDirectory = dirname(fileURLToPath(import.meta.url));
const workspaceRoot = resolve(dbPackageDirectory, "../../..");

let envFilesLoaded = false;

function loadRootEnvFile(filename: string) {
  const filePath = resolve(workspaceRoot, filename);
  if (!existsSync(filePath)) return;

  const contents = readFileSync(filePath, "utf8");
  for (const line of contents.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    if (!key || process.env[key]) {
      continue;
    }

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

function ensureRuntimeEnvLoaded() {
  if (envFilesLoaded) {
    return;
  }
  loadRootEnvFile(".env.local");
  loadRootEnvFile(".env");
  envFilesLoaded = true;
}

const revolutAccountSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1).optional().nullable(),
  balance: z.number(),
  currency: z.string().min(3),
  state: z.string().min(1),
  public: z.boolean().optional().nullable(),
  created_at: z.string().min(1),
  updated_at: z.string().min(1),
});

const revolutCardReferenceSchema = z.object({
  name: z.string().min(1),
  value: z.string().min(1),
});

const revolutTransactionLegSchema = z.object({
  leg_id: z.string().min(1),
  amount: z.number(),
  fee: z.number().optional().nullable(),
  currency: z.string().min(3),
  bill_amount: z.number().optional().nullable(),
  bill_currency: z.string().min(3).optional().nullable(),
  account_id: z.string().min(1),
  counterparty: z
    .object({
      account_id: z.string().min(1).optional().nullable(),
      account_type: z.string().min(1),
      id: z.string().min(1).optional().nullable(),
    })
    .optional()
    .nullable(),
  description: z.string().optional().nullable(),
  balance: z.number().optional().nullable(),
  card: z
    .object({
      id: z.string().min(1).optional().nullable(),
      card_number: z.string().min(1).optional().nullable(),
      first_name: z.string().min(1).optional().nullable(),
      last_name: z.string().min(1).optional().nullable(),
      phone: z.string().min(1).optional().nullable(),
      references: z.array(revolutCardReferenceSchema).optional().nullable(),
    })
    .optional()
    .nullable(),
});

const revolutTransactionSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  request_id: z.string().min(1).optional().nullable(),
  state: z.string().min(1),
  reason_code: z.string().min(1).optional().nullable(),
  created_at: z.string().min(1),
  updated_at: z.string().min(1),
  completed_at: z.string().min(1).optional().nullable(),
  scheduled_for: z.string().min(1).optional().nullable(),
  related_transaction_id: z.string().min(1).optional().nullable(),
  merchant: z
    .object({
      id: z.string().min(1).optional().nullable(),
      name: z.string().min(1).optional().nullable(),
      city: z.string().min(1).optional().nullable(),
      country: z.string().min(1).optional().nullable(),
      category_code: z.string().min(1).optional().nullable(),
    })
    .optional()
    .nullable(),
  reference: z.string().min(1).optional().nullable(),
  legs: z.array(revolutTransactionLegSchema),
});

const revolutExpenseSchema = z.object({
  id: z.string().min(1),
  state: z.string().min(1),
  transaction_type: z.string().min(1),
  description: z.string().optional().nullable(),
  submitted_at: z.string().min(1).optional().nullable(),
  completed_at: z.string().min(1).optional().nullable(),
  payer: z.string().min(1).optional().nullable(),
  merchant: z.string().min(1).optional().nullable(),
  transaction_id: z.string().min(1).optional().nullable(),
  expense_date: z.string().min(1),
  labels: z.record(z.array(z.string())).default({}),
  splits: z.array(
    z.object({
      amount: z.object({
        amount: z.number(),
        currency: z.string().min(3),
      }),
      category: z.object({
        id: z.string().min(1),
        name: z.string().min(1),
        code: z.string().min(1).optional().nullable(),
      }),
      tax_rate: z.object({
        id: z.string().min(1),
        name: z.string().min(1),
        percentage: z.number(),
      }),
      receipt_ids: z.array(z.string()).default([]),
      spent_amount: z.object({
        amount: z.number(),
        currency: z.string().min(3),
      }),
    }),
  ),
});

export type RevolutAccount = z.infer<typeof revolutAccountSchema>;
export type RevolutTransaction = z.infer<typeof revolutTransactionSchema>;
export type RevolutTransactionLeg = z.infer<typeof revolutTransactionLegSchema>;
export type RevolutExpense = z.infer<typeof revolutExpenseSchema>;

export interface RevolutRuntimeConfig {
  clientId: string;
  privateKeyPem: string;
  redirectUri: string;
  authBaseUrl: string;
  apiBaseUrl: string;
  webhookSigningSecret: string | null;
  masterKey: string;
  syncIntervalMinutes: number;
  initialBackfillDays: number;
  syncLookbackMinutes: number;
}

function getConfiguredRevolutPrivateKeyPem() {
  ensureRuntimeEnvLoaded();

  const inlineValue = process.env.REVOLUT_PRIVATE_KEY_PEM?.trim() ?? "";
  if (inlineValue) {
    return inlineValue.replace(/\\n/g, "\n");
  }

  const filePath = process.env.REVOLUT_PRIVATE_KEY_PEM_FILE?.trim() ?? "";
  if (!filePath) {
    return "";
  }

  const resolvedPath = resolve(workspaceRoot, filePath);
  if (!existsSync(resolvedPath)) {
    throw new Error(
      `Revolut private key file was not found at ${resolvedPath}.`,
    );
  }

  return readFileSync(resolvedPath, "utf8");
}

export function getRevolutRuntimeStatus() {
  ensureRuntimeEnvLoaded();
  const missingEnvKeys: string[] = [];
  if (!(process.env.REVOLUT_CLIENT_ID?.trim() ?? "")) {
    missingEnvKeys.push("REVOLUT_CLIENT_ID");
  }
  if (
    !(process.env.REVOLUT_PRIVATE_KEY_PEM?.trim() ?? "") &&
    !(process.env.REVOLUT_PRIVATE_KEY_PEM_FILE?.trim() ?? "")
  ) {
    missingEnvKeys.push(
      "REVOLUT_PRIVATE_KEY_PEM or REVOLUT_PRIVATE_KEY_PEM_FILE",
    );
  }
  if (!(process.env.REVOLUT_REDIRECT_URI?.trim() ?? "")) {
    missingEnvKeys.push("REVOLUT_REDIRECT_URI");
  }
  if (!(process.env.BANK_CONNECTIONS_MASTER_KEY?.trim() ?? "")) {
    missingEnvKeys.push("BANK_CONNECTIONS_MASTER_KEY");
  }

  return {
    configured: missingEnvKeys.length === 0,
    missingEnvKeys,
    apiBaseUrl:
      process.env.REVOLUT_API_BASE_URL?.trim() ||
      "https://b2b.revolut.com/api/1.0",
    authBaseUrl:
      process.env.REVOLUT_AUTH_BASE_URL?.trim() ||
      "https://business.revolut.com",
    readOnlyScope: "READ",
  };
}

export function getRevolutRuntimeConfig(): RevolutRuntimeConfig {
  const status = getRevolutRuntimeStatus();
  if (!status.configured) {
    throw new Error(
      `Revolut integration is not configured. Missing env vars: ${status.missingEnvKeys.join(", ")}.`,
    );
  }

  const syncIntervalMinutes = Number(
    process.env.BANK_SYNC_INTERVAL_MINUTES ?? "30",
  );
  const initialBackfillDays = Number(
    process.env.REVOLUT_INITIAL_BACKFILL_DAYS ?? "365",
  );
  const syncLookbackMinutes = Number(
    process.env.REVOLUT_SYNC_LOOKBACK_MINUTES ?? "30",
  );

  return {
    clientId: String(process.env.REVOLUT_CLIENT_ID),
    privateKeyPem: getConfiguredRevolutPrivateKeyPem(),
    redirectUri: String(process.env.REVOLUT_REDIRECT_URI),
    authBaseUrl: status.authBaseUrl,
    apiBaseUrl: status.apiBaseUrl,
    webhookSigningSecret:
      process.env.REVOLUT_WEBHOOK_SIGNING_SECRET?.trim() || null,
    masterKey: String(process.env.BANK_CONNECTIONS_MASTER_KEY),
    syncIntervalMinutes:
      Number.isFinite(syncIntervalMinutes) && syncIntervalMinutes > 0
        ? Math.floor(syncIntervalMinutes)
        : 30,
    initialBackfillDays:
      Number.isFinite(initialBackfillDays) && initialBackfillDays > 0
        ? Math.floor(initialBackfillDays)
        : 365,
    syncLookbackMinutes:
      Number.isFinite(syncLookbackMinutes) && syncLookbackMinutes > 0
        ? Math.floor(syncLookbackMinutes)
        : 30,
  };
}

function base64UrlEncode(value: Buffer | string) {
  return Buffer.from(value)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function base64UrlDecode(value: string) {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padding = normalized.length % 4 === 0 ? "" : "=".repeat(4 - (normalized.length % 4));
  return Buffer.from(`${normalized}${padding}`, "base64");
}

function getClientAssertionIss(redirectUri: string) {
  return new URL(redirectUri).hostname;
}

export function createRevolutClientAssertion(config: RevolutRuntimeConfig) {
  const header = {
    alg: "RS256",
    typ: "JWT",
  };
  const payload = {
    iss: getClientAssertionIss(config.redirectUri),
    sub: config.clientId,
    aud: "https://revolut.com",
    exp: Math.floor(Date.now() / 1000) + 5 * 60,
  };
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signer = createSign("RSA-SHA256");
  signer.update(`${encodedHeader}.${encodedPayload}`);
  signer.end();
  const signature = signer.sign(createPrivateKey(config.privateKeyPem));
  return `${encodedHeader}.${encodedPayload}.${base64UrlEncode(signature)}`;
}

function deriveSecretKey(masterKey: string) {
  return createHash("sha256").update(masterKey).digest();
}

export function encryptBankSecret(masterKey: string, plaintext: string) {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", deriveSecretKey(masterKey), iv);
  const ciphertext = Buffer.concat([
    cipher.update(plaintext, "utf8"),
    cipher.final(),
  ]);
  const authTag = cipher.getAuthTag();
  return base64UrlEncode(Buffer.concat([iv, authTag, ciphertext]));
}

export function decryptBankSecret(masterKey: string, ciphertext: string) {
  const payload = base64UrlDecode(ciphertext);
  const iv = payload.subarray(0, 12);
  const authTag = payload.subarray(12, 28);
  const encrypted = payload.subarray(28);
  const decipher = createDecipheriv(
    "aes-256-gcm",
    deriveSecretKey(masterKey),
    iv,
  );
  decipher.setAuthTag(authTag);
  return Buffer.concat([
    decipher.update(encrypted),
    decipher.final(),
  ]).toString("utf8");
}

export function createSignedRevolutState(
  config: RevolutRuntimeConfig,
  payload: Record<string, unknown>,
) {
  const body = {
    ...payload,
    iat: new Date().toISOString(),
  };
  const encodedPayload = base64UrlEncode(JSON.stringify(body));
  const signature = createHmac("sha256", deriveSecretKey(config.masterKey))
    .update(encodedPayload)
    .digest();
  return `${encodedPayload}.${base64UrlEncode(signature)}`;
}

export function verifySignedRevolutState(
  config: RevolutRuntimeConfig,
  state: string,
) {
  const [encodedPayload, encodedSignature] = state.split(".");
  if (!encodedPayload || !encodedSignature) {
    throw new Error("Revolut OAuth state is malformed.");
  }
  const expectedSignature = createHmac(
    "sha256",
    deriveSecretKey(config.masterKey),
  )
    .update(encodedPayload)
    .digest();
  const actualSignature = base64UrlDecode(encodedSignature);
  if (
    expectedSignature.length !== actualSignature.length ||
    !expectedSignature.equals(actualSignature)
  ) {
    throw new Error("Revolut OAuth state signature is invalid.");
  }

  const parsed = JSON.parse(
    base64UrlDecode(encodedPayload).toString("utf8"),
  ) as Record<string, unknown>;
  return parsed;
}

export function buildRevolutAuthorizationUrl(
  config: RevolutRuntimeConfig,
  state: string,
) {
  const url = new URL("/app-confirm", config.authBaseUrl);
  url.searchParams.set("client_id", config.clientId);
  url.searchParams.set("redirect_uri", config.redirectUri);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", "READ");
  url.searchParams.set("state", state);
  return url.toString();
}

const tokenResponseSchema = z.object({
  access_token: z.string().min(1),
  token_type: z.string().min(1),
  expires_in: z.number(),
  refresh_token: z.string().min(1).optional().nullable(),
});

async function exchangeTokenForm(
  config: RevolutRuntimeConfig,
  formValues: Record<string, string>,
) {
  const form = new URLSearchParams();
  for (const [key, value] of Object.entries(formValues)) {
    form.set(key, value);
  }

  const response = await fetch(`${config.apiBaseUrl}/auth/token`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: form.toString(),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `Revolut token exchange failed (${response.status}): ${errorText || response.statusText}`,
    );
  }

  return tokenResponseSchema.parse(await response.json());
}

export async function exchangeRevolutAuthorizationCode(
  config: RevolutRuntimeConfig,
  code: string,
) {
  return exchangeTokenForm(config, {
    grant_type: "authorization_code",
    code,
    client_assertion_type:
      "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    client_assertion: createRevolutClientAssertion(config),
  });
}

export async function refreshRevolutAccessToken(
  config: RevolutRuntimeConfig,
  refreshToken: string,
) {
  return exchangeTokenForm(config, {
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    client_assertion_type:
      "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    client_assertion: createRevolutClientAssertion(config),
  });
}

async function fetchRevolutJson<T>(
  config: RevolutRuntimeConfig,
  accessToken: string,
  path: string,
  query: Record<string, string | number | null | undefined> = {},
) {
  const normalizedBaseUrl = config.apiBaseUrl.endsWith("/")
    ? config.apiBaseUrl
    : `${config.apiBaseUrl}/`;
  const normalizedPath = path.replace(/^\/+/, "");
  const url = new URL(normalizedPath, normalizedBaseUrl);
  for (const [key, value] of Object.entries(query)) {
    if (value === null || value === undefined || value === "") {
      continue;
    }
    url.searchParams.set(key, String(value));
  }

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `Revolut API request failed for ${url.pathname} (${response.status}): ${errorText || response.statusText}`,
    );
  }

  return (await response.json()) as T;
}

export async function fetchRevolutAccounts(
  config: RevolutRuntimeConfig,
  accessToken: string,
) {
  const payload = await fetchRevolutJson<unknown[]>(
    config,
    accessToken,
    "/accounts",
  );
  return z.array(revolutAccountSchema).parse(payload);
}

export async function fetchRevolutTransactions(
  config: RevolutRuntimeConfig,
  accessToken: string,
  query: {
    from?: string | null;
    to?: string | null;
    account?: string | null;
    count?: number;
  },
) {
  const payload = await fetchRevolutJson<unknown[]>(
    config,
    accessToken,
    "/transactions",
    {
      from: query.from,
      to: query.to,
      account: query.account,
      count: query.count ?? 1000,
    },
  );
  return z.array(revolutTransactionSchema).parse(payload);
}

export async function fetchRevolutExpenses(
  config: RevolutRuntimeConfig,
  accessToken: string,
  query: {
    from?: string | null;
    to?: string | null;
    count?: number;
  },
) {
  const payload = await fetchRevolutJson<unknown[]>(
    config,
    accessToken,
    "/expenses",
    {
      from: query.from,
      to: query.to,
      count: query.count ?? 500,
    },
  );
  return z.array(revolutExpenseSchema).parse(payload);
}

function normalizeOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizeOptionalNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function buildRevolutProviderContext(input: {
  transaction: RevolutTransaction;
  leg: RevolutTransactionLeg;
  expense?: RevolutExpense | null;
}) {
  return {
    provider: "revolut_business",
    transaction: {
      id: input.transaction.id,
      type: input.transaction.type,
      state: input.transaction.state,
      reasonCode: input.transaction.reason_code ?? null,
      requestId: input.transaction.request_id ?? null,
      reference: input.transaction.reference ?? null,
      createdAt: input.transaction.created_at,
      updatedAt: input.transaction.updated_at,
      completedAt: input.transaction.completed_at ?? null,
      scheduledFor: input.transaction.scheduled_for ?? null,
      relatedTransactionId: input.transaction.related_transaction_id ?? null,
    },
    merchant: input.transaction.merchant
      ? {
          id: normalizeOptionalString(input.transaction.merchant.id),
          name: normalizeOptionalString(input.transaction.merchant.name),
          city: normalizeOptionalString(input.transaction.merchant.city),
          country: normalizeOptionalString(input.transaction.merchant.country),
          categoryCode: normalizeOptionalString(
            input.transaction.merchant.category_code,
          ),
        }
      : null,
    leg: {
      legId: input.leg.leg_id,
      amount: input.leg.amount,
      fee: normalizeOptionalNumber(input.leg.fee),
      currency: input.leg.currency,
      billAmount: normalizeOptionalNumber(input.leg.bill_amount),
      billCurrency: normalizeOptionalString(input.leg.bill_currency),
      accountId: input.leg.account_id,
      description: normalizeOptionalString(input.leg.description),
      balance: normalizeOptionalNumber(input.leg.balance),
      counterparty: input.leg.counterparty
        ? {
            accountType: input.leg.counterparty.account_type,
            accountId: normalizeOptionalString(input.leg.counterparty.account_id),
            id: normalizeOptionalString(input.leg.counterparty.id),
          }
        : null,
      card: input.leg.card
        ? {
            id: normalizeOptionalString(input.leg.card.id),
            cardNumber: normalizeOptionalString(input.leg.card.card_number),
            firstName: normalizeOptionalString(input.leg.card.first_name),
            lastName: normalizeOptionalString(input.leg.card.last_name),
            phone: normalizeOptionalString(input.leg.card.phone),
            references:
              input.leg.card.references?.map((reference) => ({
                name: reference.name,
                value: reference.value,
              })) ?? [],
          }
        : null,
    },
    expense: input.expense
      ? {
          id: input.expense.id,
          state: input.expense.state,
          transactionType: input.expense.transaction_type,
          description: input.expense.description ?? null,
          submittedAt: input.expense.submitted_at ?? null,
          completedAt: input.expense.completed_at ?? null,
          payer: input.expense.payer ?? null,
          merchant: input.expense.merchant ?? null,
          expenseDate: input.expense.expense_date,
          labels: input.expense.labels,
          splits: input.expense.splits.map((split) => ({
            amount: split.amount,
            spentAmount: split.spent_amount,
            category: split.category,
            taxRate: split.tax_rate,
            receiptIds: split.receipt_ids,
          })),
          receiptCount: input.expense.splits.reduce(
            (sum, split) => sum + split.receipt_ids.length,
            0,
          ),
        }
      : null,
  } satisfies Record<string, unknown>;
}

export function verifyRevolutWebhookTimestamp(timestamp: string) {
  const deliveredAt = Date.parse(timestamp);
  if (!Number.isFinite(deliveredAt)) {
    return false;
  }
  return Math.abs(Date.now() - deliveredAt) <= 5 * 60_000;
}

export function verifyRevolutWebhookSignature(input: {
  signingSecret: string;
  timestamp: string;
  signatureHeader: string;
  body: string;
}) {
  const signatures = input.signatureHeader
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const payload = `${input.timestamp}.${input.body}`;
  const expectedBuffer = createHmac("sha256", input.signingSecret)
    .update(payload)
    .digest();
  const expectedHex = expectedBuffer.toString("hex").toLowerCase();
  const expectedBase64 = expectedBuffer.toString("base64");
  const expectedBase64Url = base64UrlEncode(expectedBuffer);

  return signatures.some((signature) => {
    const normalized = signature.includes("=")
      ? signature.split("=").at(-1)?.trim() ?? ""
      : signature;
    return (
      normalized.toLowerCase() === expectedHex ||
      normalized === expectedBase64 ||
      normalized === expectedBase64Url
    );
  });
}
