import assert from "node:assert/strict";
import test from "node:test";

import { categoryAppliesToScope } from "../apps/web/lib/page-models.ts";
import { createAccount, createDataset } from "./support/create-dataset";

const personalEntity = {
  id: "personal-entity",
  userId: "user-1",
  slug: "personal",
  displayName: "Personal",
  legalName: null,
  entityKind: "personal" as const,
  baseCurrency: "EUR" as const,
  active: true,
  createdAt: "2026-01-01T00:00:00Z",
};

const companyEntity = {
  id: "company-entity",
  userId: "user-1",
  slug: "thewhitebox",
  displayName: "TheWhiteBox",
  legalName: null,
  entityKind: "company" as const,
  baseCurrency: "EUR" as const,
  active: true,
  createdAt: "2026-01-01T00:00:00Z",
};

const scopedCategoryDataset = createDataset({
  entities: [personalEntity, companyEntity],
  accounts: [
    createAccount({
      id: "personal-account",
      entityId: personalEntity.id,
      accountType: "checking",
      assetDomain: "cash",
    }),
    createAccount({
      id: "company-account",
      entityId: companyEntity.id,
      accountType: "company_bank",
      assetDomain: "cash",
    }),
  ],
  categories: [
    {
      code: "business_income",
      displayName: "Business Income",
      parentCode: null,
      scopeKind: "personal",
      directionKind: "income",
      sortOrder: 10,
      active: true,
      metadataJson: {},
    },
    {
      code: "client_payment",
      displayName: "Client Payment",
      parentCode: null,
      scopeKind: "company",
      directionKind: "income",
      sortOrder: 20,
      active: true,
      metadataJson: {},
    },
    {
      code: "salary",
      displayName: "Salary",
      parentCode: null,
      scopeKind: "system",
      directionKind: "income",
      sortOrder: 30,
      active: true,
      metadataJson: {},
    },
  ],
});

test("category detail pages treat personal and company categories as scope-specific", () => {
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "entity", entityId: companyEntity.id },
      "business_income",
    ),
    false,
  );
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "entity", entityId: companyEntity.id },
      "client_payment",
    ),
    true,
  );
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "entity", entityId: personalEntity.id },
      "business_income",
    ),
    true,
  );
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "entity", entityId: personalEntity.id },
      "client_payment",
    ),
    false,
  );
});

test("category compatibility allows consolidated, system, and synthetic category routes", () => {
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "consolidated" },
      "business_income",
    ),
    true,
  );
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "entity", entityId: companyEntity.id },
      "salary",
    ),
    true,
  );
  assert.equal(
    categoryAppliesToScope(
      scopedCategoryDataset,
      { kind: "entity", entityId: companyEntity.id },
      "__unresolved_income",
    ),
    true,
  );
});
