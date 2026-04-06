# MyFinanceApp HTTP Surface

## Shared Query Parameters for Read Routes

These routes resolve state through `apps/web/lib/queries.ts` and understand the same search params:

- `scope`: `consolidated`, an entity slug, or `account:<accountId>`
- `currency`: `EUR` or `USD`
- `period`: `mtd`, `ytd`, `24m`, `week`, or `custom`
- `asOf`: reference date in `YYYY-MM-DD`
- `start` and `end`: custom period bounds

Use `scripts/discover-myfinance.py` to resolve valid entity slugs and account ids before calling these routes.

## REST Routes

### Dashboard and Analytics

- `GET /api/dashboard/summary`
  - Query: shared read params
  - Returns: dashboard summary from `buildDashboardSummary`

- `GET /api/metrics/:metricId`
  - Query: shared read params
  - Returns: one metric result from `buildMetricResult`

- `GET /api/insights`
  - Query: shared read params
  - Returns: insights plus `schemaVersion`, `scope`, and `generatedAt`

- `GET /api/holdings`
  - Query: shared read params
  - Returns: holdings read model from `buildInvestmentsReadModel(...).holdings`

### Transactions

- `GET /api/transactions`
  - Query: shared read params
  - Returns: `domain.listTransactions(state.scope)`

- `PATCH /api/transactions/:transactionId`
  - Body:
    - `patch: Record<string, unknown>`
    - `createRuleFromTransaction?: boolean`
    - `apply?: boolean` default `true`
  - Side effects:
    - revalidates finance read paths when `result.applied`
    - revalidates rules paths when `createRuleFromTransaction` is true and the update applies
  - Note: this route calls the same domain update flow as the CLI transaction command

- `POST /api/transactions/:transactionId/review`
  - Body:
    - `reviewContext: string` with at least one non-whitespace character
  - Returns: queued review reanalysis job with HTTP `202`

### Imports

- `POST /api/imports/preview`
  - Body:
    - `accountId: string`
    - `templateId: string`
    - `originalFilename?: string`
    - `filePath?: string`
  - Returns: preview import result

- `POST /api/imports/commit`
  - Body:
    - `accountId: string`
    - `templateId: string`
    - `originalFilename?: string`
    - `filePath?: string`
  - Returns: commit import result
  - Side effects: revalidates import paths

### Rules

- `GET /api/rules`
  - Returns: `domain.listRules()`

- `POST /api/rules`
  - Body:
    - `priority: number`
    - `scopeJson: Record<string, unknown>`
    - `conditionsJson: Record<string, unknown>`
    - `outputsJson: Record<string, unknown>`
    - `apply?: boolean` default `true`
  - Side effects: revalidates rules paths when applied

- `GET /api/rules/:ruleId`
  - Returns: one rule or `null`

- `GET /api/rules/drafts`
  - Returns: rule draft jobs

- `POST /api/rules/drafts`
  - Body:
    - `requestText: string` minimum length `8`
    - `apply?: boolean` default `true`
  - Side effects: revalidates rules paths when applied

- `POST /api/rules/drafts/:jobId/apply`
  - Body:
    - `apply?: boolean` default `true`
  - Side effects: revalidates rules paths when applied

### Securities

- `GET /api/securities/search`
  - Query:
    - `q`: free-text query string
  - Returns: market data provider lookup results

- `POST /api/securities/resolve`
  - Body:
    - `transactionId: string`
    - `securityId: string`
    - `apply?: boolean` default `true`
  - Side effects: clears `needsReview` and `reviewReason`; revalidates finance read paths when applied

### Review Jobs

- `GET /api/review-jobs/:jobId`
  - Returns: review reanalysis job status
  - Side effects: revalidates finance read paths when the job is completed

## Non-REST Server Actions

These capabilities are exposed through Next.js server actions in `apps/web/app/actions.ts`, not through `route.ts` endpoints:

- `previewImportAction(formData)`
- `commitImportAction(formData)`
- `createTemplateAction(input)`
- `deleteTemplateAction(templateId)`
- `createAccountAction(input)`
- `deleteAccountAction(accountId)`
- `resetWorkspaceAction()`
- `updatePromptProfileAction(formData)`
- `queueRuleDraftAction(requestText)`
- `applyRuleDraftAction(jobId)`

## Action Schemas Worth Remembering

### Create Template Action

`createTemplateAction` accepts a richer template payload than the CLI create command, including:

- `sheetName`
- `headerRowIndex`
- `rowsToSkipBeforeHeader`
- `rowsToSkipAfterHeader`
- `delimiter`
- `encoding`
- `decimalSeparator`
- `thousandsSeparator`
- `dateFormat`
- `columnMappings`
- `signMode`
- `invertSign`
- `directionColumn`
- `debitColumn`
- `creditColumn`
- `debitValuesText`
- `creditValuesText`
- `dateDayFirst`
- `active`

### Create Account Action

`createAccountAction` accepts:

- `entityId`
- `institutionName`
- `displayName`
- `accountType`
- `defaultCurrency`
- `openingBalanceOriginal`
- `openingBalanceDate`
- `includeInConsolidation`
- `importTemplateDefaultId`
- `matchingAliasesText`
- `accountSuffix`
- `balanceMode`
- `staleAfterDays`

### Reset Workspace Action

- `resetWorkspaceAction()` performs a persistent workspace reset and revalidates the main finance pages plus settings and templates

## Route Selection Guidance

- Prefer the CLI wrapper for agent-facing reads and repo-local operations.
- Prefer HTTP routes when the user explicitly wants endpoint behavior, when you need query/body contract details, or when integrating with the web app.
- Remember that some capabilities have no REST route and require server actions or direct repo/domain access.
