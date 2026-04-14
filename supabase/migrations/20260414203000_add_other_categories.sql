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
  ('other_expense', 'Other', null, 'both', 'expense', 29, true, '{}'::jsonb),
  ('other_income', 'Other', null, 'both', 'income', 30, true, '{}'::jsonb)
on conflict (code) do update
set
  display_name = excluded.display_name,
  parent_code = excluded.parent_code,
  scope_kind = excluded.scope_kind,
  direction_kind = excluded.direction_kind,
  sort_order = excluded.sort_order,
  active = excluded.active,
  metadata_json = excluded.metadata_json;
