import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService, SEEDED_USER_ID } from "@myfinance/domain";

const domain = new FinanceDomainService(createFinanceRepository());

const templateBodySchema = z.object({
    template: z.object({
      name: z.string().min(1),
      institutionName: z.string().min(1),
      compatibleAccountType: z.enum([
        "checking",
        "savings",
        "company_bank",
        "brokerage_cash",
        "brokerage_account",
        "credit_card",
        "other",
      ]),
    fileKind: z.enum(["csv", "xlsx"]),
    sheetName: z.string().nullable().optional(),
    headerRowIndex: z.coerce.number().int().min(1).default(1),
    rowsToSkipBeforeHeader: z.coerce.number().int().min(0).default(0),
    rowsToSkipAfterHeader: z.coerce.number().int().min(0).default(0),
    delimiter: z.string().nullable().optional(),
    encoding: z.string().nullable().optional(),
    decimalSeparator: z.string().nullable().optional(),
    thousandsSeparator: z.string().nullable().optional(),
    dateFormat: z.string().min(1).default("%Y-%m-%d"),
    defaultCurrency: z.string().min(1).default("EUR"),
    columnMapJson: z.record(z.string(), z.any()).default({}),
    signLogicJson: z.record(z.string(), z.any()).default({}),
    normalizationRulesJson: z.record(z.string(), z.any()).default({}),
    active: z.boolean().default(true),
  }),
  apply: z.boolean().default(true),
});

export async function GET() {
  const templates = await domain.listTemplates();
  return NextResponse.json(templates);
}

export async function POST(request: NextRequest) {
  const body = templateBodySchema.parse(await request.json());
  const result = await domain.createTemplate({
    template: {
      userId: process.env.APP_SEEDED_USER_ID ?? SEEDED_USER_ID,
      ...body.template,
      sheetName: body.template.sheetName ?? null,
      delimiter: body.template.delimiter ?? null,
      encoding: body.template.encoding ?? null,
      decimalSeparator: body.template.decimalSeparator ?? null,
      thousandsSeparator: body.template.thousandsSeparator ?? null,
    },
    actorName: "web-api",
    sourceChannel: "web",
    apply: body.apply,
  });
  return NextResponse.json(result);
}
