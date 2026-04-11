import type { DomainDataset } from "@myfinance/domain";

import { mapFromSql } from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { transactionColumnsSql } from "./transaction-columns";

export async function loadDatasetForUser(
  sql: SqlClient,
  userId: string,
): Promise<DomainDataset> {
  const [
    profiles,
    entities,
    accounts,
    bankConnections,
    bankAccountLinks,
    templates,
    importBatches,
    transactions,
    categories,
    rules,
    auditEvents,
    jobs,
    accountBalanceSnapshots,
    securities,
    securityAliases,
    securityPrices,
    fxRates,
    holdingAdjustments,
    manualInvestments,
    manualInvestmentValuations,
    investmentPositions,
    dailyPortfolioSnapshots,
    monthlyCashFlowRollups,
  ] = await Promise.all([
    sql`select * from public.profiles where id = ${userId} limit 1`,
    sql`select * from public.entities where user_id = ${userId} order by created_at`,
    sql`select * from public.accounts where user_id = ${userId} order by created_at`,
    sql`
      select
        id,
        user_id,
        entity_id,
        provider,
        connection_label,
        status,
        external_business_id,
        last_cursor_created_at,
        last_successful_sync_at,
        last_sync_queued_at,
        last_webhook_at,
        auth_expires_at,
        last_error,
        metadata_json,
        created_at,
        updated_at
      from public.bank_connections
      where user_id = ${userId}
      order by created_at
    `,
    sql`
      select *
      from public.bank_account_links
      where user_id = ${userId}
      order by created_at
    `,
    sql`select * from public.import_templates where user_id = ${userId} order by created_at`,
    sql`select * from public.import_batches where user_id = ${userId} order by imported_at desc`,
    sql`
      select ${transactionColumnsSql(sql)}
      from public.transactions
      where user_id = ${userId}
      order by transaction_date desc, created_at desc
    `,
    sql`select * from public.categories order by sort_order, code`,
    sql`select * from public.classification_rules where user_id = ${userId} order by priority`,
    sql`select * from public.audit_events order by created_at desc limit 200`,
    sql`select * from public.jobs order by created_at desc`,
    sql`select * from public.account_balance_snapshots where account_id in (select id from public.accounts where user_id = ${userId}) order by as_of_date desc`,
    sql`select * from public.securities order by display_symbol`,
    sql`select * from public.security_aliases order by created_at desc`,
    sql`select * from public.security_prices order by price_date desc, quote_timestamp desc`,
    sql`select * from public.fx_rates order by as_of_date desc`,
    sql`select * from public.holding_adjustments where user_id = ${userId} order by effective_date desc`,
    sql`select * from public.manual_investments where user_id = ${userId} order by created_at desc`,
    sql`
      select *
      from public.manual_investment_valuations
      where user_id = ${userId}
      order by snapshot_date desc, updated_at desc
    `,
    sql`select * from public.investment_positions where user_id = ${userId}`,
    sql`select * from public.daily_portfolio_snapshots where user_id = ${userId} order by snapshot_date desc`,
    sql`
      with income as (
        select entity_id, month, income_total_eur
        from public.mv_monthly_income_totals
        where user_id = ${userId}
      ),
      spending as (
        select entity_id, month, sum(spending_total_eur) as spending_total_eur
        from public.mv_monthly_spending_totals
        where user_id = ${userId}
        group by entity_id, month
      )
      select
        coalesce(income.entity_id, spending.entity_id) as entity_id,
        coalesce(income.month, spending.month) as month,
        coalesce(income.income_total_eur, 0) as income_eur,
        coalesce(spending.spending_total_eur, 0) as spending_eur,
        coalesce(income.income_total_eur, 0) - coalesce(spending.spending_total_eur, 0) as operating_net_eur
      from income
      full outer join spending
        on spending.entity_id = income.entity_id
       and spending.month = income.month
      order by month asc
    `,
  ]);

  if (!profiles[0]) {
    throw new Error(
      `Seeded user ${userId} was not found in the database. Run the seed or set APP_SEEDED_USER_ID correctly.`,
    );
  }

  return {
    schemaVersion: "v1" as const,
    profile: mapFromSql<DomainDataset["profile"]>(profiles[0]),
    entities: mapFromSql<DomainDataset["entities"]>(entities),
    accounts: mapFromSql<DomainDataset["accounts"]>(accounts),
    bankConnections:
      mapFromSql<DomainDataset["bankConnections"]>(bankConnections),
    bankAccountLinks:
      mapFromSql<DomainDataset["bankAccountLinks"]>(bankAccountLinks),
    templates: mapFromSql<DomainDataset["templates"]>(templates),
    importBatches: mapFromSql<DomainDataset["importBatches"]>(importBatches),
    transactions: mapFromSql<DomainDataset["transactions"]>(transactions),
    categories: mapFromSql<DomainDataset["categories"]>(categories),
    rules: mapFromSql<DomainDataset["rules"]>(rules),
    auditEvents: mapFromSql<DomainDataset["auditEvents"]>(auditEvents),
    jobs: mapFromSql<DomainDataset["jobs"]>(jobs),
    accountBalanceSnapshots: mapFromSql<
      DomainDataset["accountBalanceSnapshots"]
    >(accountBalanceSnapshots),
    securities: mapFromSql<DomainDataset["securities"]>(securities),
    securityAliases:
      mapFromSql<DomainDataset["securityAliases"]>(securityAliases),
    securityPrices: mapFromSql<DomainDataset["securityPrices"]>(securityPrices),
    fxRates: mapFromSql<DomainDataset["fxRates"]>(fxRates),
    holdingAdjustments:
      mapFromSql<DomainDataset["holdingAdjustments"]>(holdingAdjustments),
    manualInvestments:
      mapFromSql<DomainDataset["manualInvestments"]>(manualInvestments),
    manualInvestmentValuations: mapFromSql<
      DomainDataset["manualInvestmentValuations"]
    >(manualInvestmentValuations),
    investmentPositions:
      mapFromSql<DomainDataset["investmentPositions"]>(investmentPositions),
    dailyPortfolioSnapshots: mapFromSql<
      DomainDataset["dailyPortfolioSnapshots"]
    >(dailyPortfolioSnapshots),
    monthlyCashFlowRollups: mapFromSql<DomainDataset["monthlyCashFlowRollups"]>(
      monthlyCashFlowRollups,
    ),
  };
}
