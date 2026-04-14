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
values
  ('tax', 'Tax', null, 'both', 'expense', 15, true, '{}'::jsonb),
  ('tax_credit', 'Tax Credit', null, 'both', 'income', 18, true, '{}'::jsonb),
  ('office', 'Office', null, 'company', 'expense', 27, true, '{}'::jsonb),
  ('meals', 'Meals', null, 'company', 'expense', 28, true, '{}'::jsonb)
on conflict (code) do update
set
  display_name = excluded.display_name,
  parent_code = excluded.parent_code,
  scope_kind = excluded.scope_kind,
  direction_kind = excluded.direction_kind,
  sort_order = excluded.sort_order,
  active = excluded.active,
  metadata_json = excluded.metadata_json;
