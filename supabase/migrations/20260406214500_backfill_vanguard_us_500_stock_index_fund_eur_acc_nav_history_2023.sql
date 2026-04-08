-- Backfill user-provided VANUIEI NAV history for 2023-11-21 and 2023-12-19.

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
from (select id from public.securities where provider_name = 'manual_fund_nav' and provider_symbol = 'IE0032126645' limit 1) as security(id)
cross join (
values
  ('2023-11-21', '2023-11-21T16:00:00Z', 49.54, 'EUR', 'manual_nav_import', false, true, 'official_nav', '{"importSource":"user_provided_history","priceType":"nav","source":"user_chat","displaySymbol":"VANUIEI"}'),
  ('2023-12-19', '2023-12-19T16:00:00Z', 51.85, 'EUR', 'manual_nav_import', false, true, 'official_nav', '{"importSource":"user_provided_history","priceType":"nav","source":"user_chat","displaySymbol":"VANUIEI"}')
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
