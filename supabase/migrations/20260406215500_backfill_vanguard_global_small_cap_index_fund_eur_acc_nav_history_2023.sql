-- Backfill user-provided VANIEUI NAV history for 2023-11-22.

insert into public.security_prices (
  security_id,
  price_date,
  quote_timestamp,
  price,
  currency,
  source_name,
  is_realtime,
  is_delayed,
  market_state,
  raw_json
)
select
  security.id,
  price_data.price_date::date,
  price_data.quote_timestamp::timestamptz,
  price_data.price::numeric,
  price_data.currency,
  price_data.source_name,
  price_data.is_realtime,
  price_data.is_delayed,
  price_data.market_state,
  price_data.raw_json::jsonb
from (select id from public.securities where provider_name = 'manual_fund_nav' and provider_symbol = 'IE00B42W4L06' limit 1) as security(id)
cross join (
values
  ('2023-11-22', '2023-11-22T16:00:00Z', 274.71, 'EUR', 'manual_nav_import', false, true, 'official_nav', '{"importSource":"user_provided_history","priceType":"nav","source":"user_chat","displaySymbol":"VANIEUI"}')
) as price_data(
  price_date,
  quote_timestamp,
  price,
  currency,
  source_name,
  is_realtime,
  is_delayed,
  market_state,
  raw_json
)
on conflict (security_id, price_date, source_name)
do update set
  quote_timestamp = excluded.quote_timestamp,
  price = excluded.price,
  currency = excluded.currency,
  is_realtime = excluded.is_realtime,
  is_delayed = excluded.is_delayed,
  market_state = excluded.market_state,
  raw_json = excluded.raw_json;
