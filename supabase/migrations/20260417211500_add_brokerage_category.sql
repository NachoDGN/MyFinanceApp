insert into public.categories (
  code,
  display_name,
  parent_code,
  scope_kind,
  direction_kind,
  sort_order,
  active,
  metadata_json
)
values (
  'brokerage',
  'Brokerage',
  null,
  'investment',
  'neutral',
  22,
  true,
  '{}'::jsonb
)
on conflict (code) do update
set
  display_name = excluded.display_name,
  parent_code = excluded.parent_code,
  scope_kind = excluded.scope_kind,
  direction_kind = excluded.direction_kind,
  sort_order = excluded.sort_order,
  active = excluded.active,
  metadata_json = excluded.metadata_json;

update public.transactions as t
set category_code = 'brokerage'
from public.accounts as a
where a.id = t.account_id
  and a.account_type = 'brokerage_cash'
  and t.category_code in ('other_expense', 'other_income');
