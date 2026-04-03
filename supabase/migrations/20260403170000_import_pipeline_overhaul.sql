create unique index if not exists idx_transactions_user_source_fingerprint_unique
  on public.transactions(user_id, source_fingerprint);

create index if not exists idx_transactions_import_batch_created
  on public.transactions(import_batch_id, created_at);

create index if not exists idx_transactions_llm_analysis_status
  on public.transactions ((coalesce(llm_payload->>'analysisStatus', 'pending')));

insert into public.categories (code, display_name, parent_code, scope_kind, direction_kind, sort_order, active, metadata_json)
values
  ('uncategorized_income', 'Uncategorized Income', null, 'system', 'income', 15, true, '{}'::jsonb)
on conflict (code) do nothing;
