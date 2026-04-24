create or replace view public.agent_ledger_fx_rates
with (security_invoker = true)
as
select
  base_currency,
  quote_currency,
  as_of_date,
  as_of_timestamp,
  rate,
  source_name
from public.fx_rates;
