import type { TransactionReviewCategoryCreation } from "@myfinance/classification";
import type { Category } from "@myfinance/domain";

import { mapFromSql, serializeJson } from "./sql-json";
import type { SqlClient } from "./sql-runtime";

const CATEGORY_CREATION_CODE_PATTERN = /^[a-z][a-z0-9_]{1,63}$/;

export interface AppliedTransactionReviewCategoryCreation {
  toolName: "category_creation";
  categoryCode: string;
  created: boolean;
  category: Category;
}

function assertValidCategoryCreationRequest(
  categoryCreation: TransactionReviewCategoryCreation,
) {
  if (!CATEGORY_CREATION_CODE_PATTERN.test(categoryCreation.code)) {
    throw new Error(
      `Category creation produced invalid category code "${categoryCreation.code}".`,
    );
  }

  if (
    categoryCreation.scopeKind === "system" ||
    categoryCreation.scopeKind === "investment"
  ) {
    throw new Error(
      `Category creation cannot create ${categoryCreation.scopeKind} categories from transaction review.`,
    );
  }
}

export async function applyTransactionReviewCategoryCreation(
  sql: SqlClient,
  input: {
    userId: string;
    transactionId: string;
    categoryCreation: TransactionReviewCategoryCreation | null | undefined;
  },
): Promise<AppliedTransactionReviewCategoryCreation | null> {
  if (!input.categoryCreation) {
    return null;
  }

  assertValidCategoryCreationRequest(input.categoryCreation);

  const existingRows = await sql`
    select *
    from public.categories
    where code = ${input.categoryCreation.code}
    limit 1
  `;
  if (existingRows[0]) {
    const existingCategory = mapFromSql<Category>(existingRows[0]);
    const compatibleScope =
      existingCategory.scopeKind === input.categoryCreation.scopeKind ||
      existingCategory.scopeKind === "both";
    const compatibleDirection =
      existingCategory.directionKind === input.categoryCreation.directionKind;
    if (!compatibleScope || !compatibleDirection) {
      throw new Error(
        `Category ${input.categoryCreation.code} already exists with incompatible scope or direction.`,
      );
    }

    return {
      toolName: "category_creation",
      categoryCode: input.categoryCreation.code,
      created: false,
      category: existingCategory,
    };
  }

  const [sortOrderRow] = await sql`
    select coalesce(max(sort_order), 0)::int as max_sort_order
    from public.categories
    where (scope_kind = ${input.categoryCreation.scopeKind} or scope_kind = 'both')
      and direction_kind = ${input.categoryCreation.directionKind}
  `;
  const nextSortOrder = Number(sortOrderRow?.max_sort_order ?? 0) + 10;
  const createdAt = new Date().toISOString();
  const metadataJson = {
    createdBy: "transaction_review",
    source: "transaction_review",
    toolName: "category_creation",
    sourceTransactionId: input.transactionId,
    userId: input.userId,
    reason: input.categoryCreation.reason,
    createdAt,
  };

  const insertedRows = await sql`
    insert into public.categories (
      code,
      display_name,
      parent_code,
      scope_kind,
      direction_kind,
      sort_order,
      active,
      metadata_json
    ) values (
      ${input.categoryCreation.code},
      ${input.categoryCreation.displayName},
      ${input.categoryCreation.parentCode},
      ${input.categoryCreation.scopeKind},
      ${input.categoryCreation.directionKind},
      ${nextSortOrder},
      true,
      ${serializeJson(sql, metadataJson)}::jsonb
    )
    returning *
  `;

  return {
    toolName: "category_creation",
    categoryCode: input.categoryCreation.code,
    created: true,
    category: mapFromSql<Category>(insertedRows[0]),
  };
}
