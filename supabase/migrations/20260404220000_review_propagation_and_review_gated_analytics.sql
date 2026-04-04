alter type public.job_type add value if not exists 'review_propagation';

create or replace view public.v_transaction_analytics as
select
  t.id,
  t.user_id,
  t.account_id,
  t.account_entity_id,
  t.economic_entity_id,
  t.transaction_date,
  t.amount_base_eur,
  t.transaction_class,
  t.category_code,
  t.merchant_normalized,
  t.counterparty_name,
  t.needs_review,
  t.classification_source,
  t.classification_confidence,
  t.transfer_match_status,
  t.cross_entity_flag
from public.transactions t
where t.voided_at is null
  and t.exclude_from_analytics is false
  and coalesce(t.needs_review, false) is false;
