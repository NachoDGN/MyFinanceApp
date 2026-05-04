create extension if not exists vector with schema extensions;

alter table public.transactions
  add column if not exists search_source_batch_key text,
  add column if not exists search_contextualized_text text,
  add column if not exists search_document_summary text,
  add column if not exists search_bm25_text text generated always as (coalesce(search_contextualized_text, description_raw)) stored,
  add column if not exists search_embedding extensions.vector(3072),
  add column if not exists search_embedding_model text,
  add column if not exists search_embedding_status text not null default 'missing' check (search_embedding_status in ('ready', 'stale', 'missing')),
  add column if not exists search_embedding_source_text text not null default 'search_contextualized_text' check (search_embedding_source_text in ('original_text', 'search_contextualized_text')),
  add column if not exists search_contextualization_model text,
  add column if not exists search_contextualization_payload jsonb not null default '{}'::jsonb,
  add column if not exists search_indexed_at timestamptz;

do $$
begin
  if to_regclass('public.transaction_search_rows') is not null then
    update public.transactions as t
    set
      search_source_batch_key = b.source_batch_key,
      search_contextualized_text = r.contextualized_text,
      search_document_summary = r.document_summary,
      search_embedding = r.embedding,
      search_embedding_model = r.embedding_model,
      search_embedding_status = r.embedding_status,
      search_embedding_source_text =
        case r.embedding_source_text
          when 'contextualized_text' then 'search_contextualized_text'
          else 'original_text'
        end,
      search_contextualization_model = r.contextualization_model,
      search_contextualization_payload = r.contextualization_payload,
      search_indexed_at = coalesce(b.last_indexed_at, r.updated_at, timezone('utc', now()))
    from public.transaction_search_rows as r
    left join public.transaction_search_batches as b
      on b.id = r.batch_id
    where t.id = r.transaction_id;
  end if;
end;
$$;

drop view if exists public.agent_ledger_search_rows;

do $$
begin
  if to_regclass('public.transaction_search_rows') is not null then
    execute 'drop trigger if exists mark_transaction_search_row_embedding_stale on public.transaction_search_rows';
    execute 'drop trigger if exists set_transaction_search_rows_updated_at on public.transaction_search_rows';
  end if;
  if to_regclass('public.transaction_search_batches') is not null then
    execute 'drop trigger if exists set_transaction_search_batches_updated_at on public.transaction_search_batches';
  end if;
end;
$$;

drop function if exists public.mark_transaction_search_row_embedding_stale();
drop function if exists public.set_transaction_search_updated_at();
drop table if exists public.transaction_search_rows cascade;
drop table if exists public.transaction_search_batches cascade;

do $$
begin
  if exists (
    select 1
    from pg_extension
    where extname = 'pg_textsearch'
  ) then
    execute '
      create index if not exists transactions_search_bm25_idx
      on public.transactions
      using bm25 (search_bm25_text)
      with (text_config = ''english'')
    ';
  else
    execute '
      create index if not exists transactions_search_bm25_idx
      on public.transactions
      using gin (to_tsvector(''english'', search_bm25_text))
    ';
  end if;
end;
$$;

create index if not exists transactions_search_embedding_idx
on public.transactions
using hnsw ((search_embedding::halfvec(3072)) halfvec_cosine_ops)
where search_embedding is not null;

create index if not exists transactions_search_user_status_idx
on public.transactions (user_id, search_embedding_status, transaction_date desc, created_at desc);

create or replace function public.transaction_search_direction(p_amount numeric)
returns text
language sql
immutable
as $$
  select case
    when p_amount > 0 then 'credit'
    when p_amount < 0 then 'debit'
    else 'neutral'
  end
$$;

create or replace function public.transaction_search_review_state(
  p_needs_review boolean,
  p_category_code text,
  p_credit_card_statement_status text,
  p_description_raw text,
  p_description_clean text,
  p_llm_payload jsonb
)
returns text
language sql
stable
as $$
  select case
    when p_llm_payload->>'analysisStatus' = 'pending' then 'pending_enrichment'
    when coalesce(p_needs_review, false)
      or lower(coalesce(p_category_code, '')) in ('uncategorized_expense', 'uncategorized_income')
      or p_credit_card_statement_status = 'upload_required'
      or (
        p_credit_card_statement_status = 'not_applicable'
        and upper(coalesce(p_description_raw, '') || ' ' || coalesce(p_description_clean, '')) like '%LIQUIDACION%'
        and upper(coalesce(p_description_raw, '') || ' ' || coalesce(p_description_clean, '')) like '%TARJETAS DE CREDITO%'
      )
      then 'needs_review'
    else 'resolved'
  end
$$;

create or replace function public.mark_transaction_search_embedding_stale()
returns trigger
language plpgsql
as $$
begin
  if new.search_embedding_status is distinct from old.search_embedding_status then
    return new;
  end if;

  if new.search_embedding_status = 'missing' then
    return new;
  end if;

  if new.search_embedding is not distinct from old.search_embedding then
    new.search_embedding_status := 'stale';
    new.search_embedding_source_text := 'search_contextualized_text';
  end if;

  return new;
end;
$$;

drop trigger if exists mark_transaction_search_embedding_stale
on public.transactions;

create trigger mark_transaction_search_embedding_stale
before update on public.transactions
for each row
when (
  new.search_contextualized_text is distinct from old.search_contextualized_text
  or new.description_raw is distinct from old.description_raw
  or new.description_clean is distinct from old.description_clean
  or new.merchant_normalized is distinct from old.merchant_normalized
  or new.counterparty_name is distinct from old.counterparty_name
  or new.category_code is distinct from old.category_code
  or new.transaction_class is distinct from old.transaction_class
  or new.amount_original is distinct from old.amount_original
  or new.currency_original is distinct from old.currency_original
  or new.account_id is distinct from old.account_id
  or new.economic_entity_id is distinct from old.economic_entity_id
  or new.needs_review is distinct from old.needs_review
  or new.review_reason is distinct from old.review_reason
  or new.llm_payload is distinct from old.llm_payload
  or new.credit_card_statement_status is distinct from old.credit_card_statement_status
)
execute function public.mark_transaction_search_embedding_stale();

create or replace function public.search_semantic_transactions(
  p_user_id uuid,
  p_query_embedding vector(3072),
  p_match_count integer default 40,
  p_account_ids uuid[] default null,
  p_entity_ids uuid[] default null,
  p_account_types public.account_type[] default null,
  p_entity_kinds public.entity_kind[] default null,
  p_review_states text[] default null,
  p_directions text[] default null,
  p_date_start date default null,
  p_date_end date default null
)
returns table (
  transaction_id uuid,
  batch_id uuid,
  source_batch_key text,
  transaction_date date,
  posted_at date,
  amount numeric,
  currency text,
  merchant text,
  counterparty text,
  category text,
  account_id uuid,
  account_name text,
  institution_name text,
  account_type public.account_type,
  economic_entity_id uuid,
  economic_entity_name text,
  economic_entity_kind public.entity_kind,
  direction text,
  review_state text,
  review_reason text,
  original_text text,
  contextualized_text text,
  document_summary text,
  embedding_source_text text,
  semantic_distance double precision
)
language sql
stable
as $$
  with indexed_transactions as (
    select
      t.id as transaction_id,
      null::uuid as batch_id,
      coalesce(t.search_source_batch_key, 'transaction:' || t.id::text) as source_batch_key,
      t.transaction_date,
      coalesce(t.posted_date, t.transaction_date) as posted_at,
      t.amount_original as amount,
      t.currency_original as currency,
      t.merchant_normalized as merchant,
      t.counterparty_name as counterparty,
      t.category_code as category,
      t.account_id,
      a.display_name as account_name,
      a.institution_name,
      a.account_type,
      t.economic_entity_id,
      e.display_name as economic_entity_name,
      e.entity_kind as economic_entity_kind,
      public.transaction_search_direction(t.amount_original) as direction,
      public.transaction_search_review_state(
        t.needs_review,
        t.category_code,
        t.credit_card_statement_status,
        t.description_raw,
        t.description_clean,
        t.llm_payload
      ) as review_state,
      t.review_reason,
      t.description_raw as original_text,
      coalesce(t.search_contextualized_text, t.description_raw) as contextualized_text,
      coalesce(t.search_document_summary, '') as document_summary,
      t.search_embedding_source_text as embedding_source_text,
      t.search_embedding as embedding
    from public.transactions as t
    join public.accounts as a
      on a.id = t.account_id
    left join public.entities as e
      on e.id = t.economic_entity_id
    where t.user_id = p_user_id
      and t.search_embedding_status = 'ready'
      and t.search_embedding is not null
  ),
  approximate_candidates as (
    select
      indexed.*,
      indexed.embedding::halfvec(3072) <=> p_query_embedding::halfvec(3072) as approx_distance
    from indexed_transactions as indexed
    where (p_account_ids is null or cardinality(p_account_ids) = 0 or indexed.account_id = any(p_account_ids))
      and (p_entity_ids is null or cardinality(p_entity_ids) = 0 or indexed.economic_entity_id = any(p_entity_ids))
      and (p_account_types is null or cardinality(p_account_types) = 0 or indexed.account_type = any(p_account_types))
      and (p_entity_kinds is null or cardinality(p_entity_kinds) = 0 or indexed.economic_entity_kind = any(p_entity_kinds))
      and (
        p_review_states is null
        or cardinality(p_review_states) = 0
        or indexed.review_state = any(p_review_states)
        or ('unresolved' = any(p_review_states) and indexed.review_state in ('needs_review', 'pending_enrichment'))
      )
      and (p_directions is null or cardinality(p_directions) = 0 or indexed.direction = any(p_directions))
      and (p_date_start is null or indexed.transaction_date >= p_date_start)
      and (p_date_end is null or indexed.transaction_date <= p_date_end)
    order by indexed.embedding::halfvec(3072) <=> p_query_embedding::halfvec(3072)
    limit greatest(1, p_match_count * 4)
  )
  select
    candidate.transaction_id,
    candidate.batch_id,
    candidate.source_batch_key,
    candidate.transaction_date,
    candidate.posted_at,
    candidate.amount,
    candidate.currency,
    candidate.merchant,
    candidate.counterparty,
    candidate.category,
    candidate.account_id,
    candidate.account_name,
    candidate.institution_name,
    candidate.account_type,
    candidate.economic_entity_id,
    candidate.economic_entity_name,
    candidate.economic_entity_kind,
    candidate.direction,
    candidate.review_state,
    candidate.review_reason,
    candidate.original_text,
    candidate.contextualized_text,
    candidate.document_summary,
    candidate.embedding_source_text,
    candidate.embedding <=> p_query_embedding as semantic_distance
  from approximate_candidates as candidate
  order by candidate.embedding <=> p_query_embedding
  limit greatest(1, p_match_count);
$$;

do $$
begin
  if exists (
    select 1
    from pg_extension
    where extname = 'pg_textsearch'
  ) then
    execute $sql$
      create or replace function public.search_keyword_transactions(
        p_user_id uuid,
        p_keyword_query text,
        p_match_count integer default 20,
        p_account_ids uuid[] default null,
        p_entity_ids uuid[] default null,
        p_account_types public.account_type[] default null,
        p_entity_kinds public.entity_kind[] default null,
        p_review_states text[] default null,
        p_directions text[] default null,
        p_date_start date default null,
        p_date_end date default null
      )
      returns table (
        transaction_id uuid,
        batch_id uuid,
        source_batch_key text,
        transaction_date date,
        posted_at date,
        amount numeric,
        currency text,
        merchant text,
        counterparty text,
        category text,
        account_id uuid,
        account_name text,
        institution_name text,
        account_type public.account_type,
        economic_entity_id uuid,
        economic_entity_name text,
        economic_entity_kind public.entity_kind,
        direction text,
        review_state text,
        review_reason text,
        original_text text,
        contextualized_text text,
        document_summary text,
        bm25_score double precision
      )
      language sql
      stable
      as $fn$
        with indexed_transactions as (
          select
            t.id as transaction_id,
            null::uuid as batch_id,
            coalesce(t.search_source_batch_key, 'transaction:' || t.id::text) as source_batch_key,
            t.transaction_date,
            coalesce(t.posted_date, t.transaction_date) as posted_at,
            t.amount_original as amount,
            t.currency_original as currency,
            t.merchant_normalized as merchant,
            t.counterparty_name as counterparty,
            t.category_code as category,
            t.account_id,
            a.display_name as account_name,
            a.institution_name,
            a.account_type,
            t.economic_entity_id,
            e.display_name as economic_entity_name,
            e.entity_kind as economic_entity_kind,
            public.transaction_search_direction(t.amount_original) as direction,
            public.transaction_search_review_state(
              t.needs_review,
              t.category_code,
              t.credit_card_statement_status,
              t.description_raw,
              t.description_clean,
              t.llm_payload
            ) as review_state,
            t.review_reason,
            t.description_raw as original_text,
            coalesce(t.search_contextualized_text, t.description_raw) as contextualized_text,
            coalesce(t.search_document_summary, '') as document_summary,
            t.search_bm25_text
          from public.transactions as t
          join public.accounts as a
            on a.id = t.account_id
          left join public.entities as e
            on e.id = t.economic_entity_id
          where t.user_id = p_user_id
            and t.search_bm25_text is not null
        )
        select
          indexed.transaction_id,
          indexed.batch_id,
          indexed.source_batch_key,
          indexed.transaction_date,
          indexed.posted_at,
          indexed.amount,
          indexed.currency,
          indexed.merchant,
          indexed.counterparty,
          indexed.category,
          indexed.account_id,
          indexed.account_name,
          indexed.institution_name,
          indexed.account_type,
          indexed.economic_entity_id,
          indexed.economic_entity_name,
          indexed.economic_entity_kind,
          indexed.direction,
          indexed.review_state,
          indexed.review_reason,
          indexed.original_text,
          indexed.contextualized_text,
          indexed.document_summary,
          indexed.search_bm25_text <@> to_bm25query(p_keyword_query, 'transactions_search_bm25_idx') as bm25_score
        from indexed_transactions as indexed
        where (p_account_ids is null or cardinality(p_account_ids) = 0 or indexed.account_id = any(p_account_ids))
          and (p_entity_ids is null or cardinality(p_entity_ids) = 0 or indexed.economic_entity_id = any(p_entity_ids))
          and (p_account_types is null or cardinality(p_account_types) = 0 or indexed.account_type = any(p_account_types))
          and (p_entity_kinds is null or cardinality(p_entity_kinds) = 0 or indexed.economic_entity_kind = any(p_entity_kinds))
          and (
            p_review_states is null
            or cardinality(p_review_states) = 0
            or indexed.review_state = any(p_review_states)
            or ('unresolved' = any(p_review_states) and indexed.review_state in ('needs_review', 'pending_enrichment'))
          )
          and (p_directions is null or cardinality(p_directions) = 0 or indexed.direction = any(p_directions))
          and (p_date_start is null or indexed.transaction_date >= p_date_start)
          and (p_date_end is null or indexed.transaction_date <= p_date_end)
        order by indexed.search_bm25_text <@> to_bm25query(p_keyword_query, 'transactions_search_bm25_idx')
        limit greatest(1, p_match_count);
      $fn$;
    $sql$;
  else
    execute $sql$
      create or replace function public.search_keyword_transactions(
        p_user_id uuid,
        p_keyword_query text,
        p_match_count integer default 20,
        p_account_ids uuid[] default null,
        p_entity_ids uuid[] default null,
        p_account_types public.account_type[] default null,
        p_entity_kinds public.entity_kind[] default null,
        p_review_states text[] default null,
        p_directions text[] default null,
        p_date_start date default null,
        p_date_end date default null
      )
      returns table (
        transaction_id uuid,
        batch_id uuid,
        source_batch_key text,
        transaction_date date,
        posted_at date,
        amount numeric,
        currency text,
        merchant text,
        counterparty text,
        category text,
        account_id uuid,
        account_name text,
        institution_name text,
        account_type public.account_type,
        economic_entity_id uuid,
        economic_entity_name text,
        economic_entity_kind public.entity_kind,
        direction text,
        review_state text,
        review_reason text,
        original_text text,
        contextualized_text text,
        document_summary text,
        bm25_score double precision
      )
      language sql
      stable
      as $fn$
        with keyword_query as (
          select websearch_to_tsquery('english', coalesce(p_keyword_query, '')) as ts_query
        ),
        indexed_transactions as (
          select
            t.id as transaction_id,
            null::uuid as batch_id,
            coalesce(t.search_source_batch_key, 'transaction:' || t.id::text) as source_batch_key,
            t.transaction_date,
            coalesce(t.posted_date, t.transaction_date) as posted_at,
            t.amount_original as amount,
            t.currency_original as currency,
            t.merchant_normalized as merchant,
            t.counterparty_name as counterparty,
            t.category_code as category,
            t.account_id,
            a.display_name as account_name,
            a.institution_name,
            a.account_type,
            t.economic_entity_id,
            e.display_name as economic_entity_name,
            e.entity_kind as economic_entity_kind,
            public.transaction_search_direction(t.amount_original) as direction,
            public.transaction_search_review_state(
              t.needs_review,
              t.category_code,
              t.credit_card_statement_status,
              t.description_raw,
              t.description_clean,
              t.llm_payload
            ) as review_state,
            t.review_reason,
            t.description_raw as original_text,
            coalesce(t.search_contextualized_text, t.description_raw) as contextualized_text,
            coalesce(t.search_document_summary, '') as document_summary,
            t.search_bm25_text
          from public.transactions as t
          join public.accounts as a
            on a.id = t.account_id
          left join public.entities as e
            on e.id = t.economic_entity_id
          where t.user_id = p_user_id
            and t.search_bm25_text is not null
        )
        select
          indexed.transaction_id,
          indexed.batch_id,
          indexed.source_batch_key,
          indexed.transaction_date,
          indexed.posted_at,
          indexed.amount,
          indexed.currency,
          indexed.merchant,
          indexed.counterparty,
          indexed.category,
          indexed.account_id,
          indexed.account_name,
          indexed.institution_name,
          indexed.account_type,
          indexed.economic_entity_id,
          indexed.economic_entity_name,
          indexed.economic_entity_kind,
          indexed.direction,
          indexed.review_state,
          indexed.review_reason,
          indexed.original_text,
          indexed.contextualized_text,
          indexed.document_summary,
          ts_rank_cd(
            to_tsvector('english', indexed.search_bm25_text),
            keyword_query.ts_query
          )::double precision as bm25_score
        from indexed_transactions as indexed
        cross join keyword_query
        where numnode(keyword_query.ts_query) > 0
          and to_tsvector('english', indexed.search_bm25_text) @@ keyword_query.ts_query
          and (p_account_ids is null or cardinality(p_account_ids) = 0 or indexed.account_id = any(p_account_ids))
          and (p_entity_ids is null or cardinality(p_entity_ids) = 0 or indexed.economic_entity_id = any(p_entity_ids))
          and (p_account_types is null or cardinality(p_account_types) = 0 or indexed.account_type = any(p_account_types))
          and (p_entity_kinds is null or cardinality(p_entity_kinds) = 0 or indexed.economic_entity_kind = any(p_entity_kinds))
          and (
            p_review_states is null
            or cardinality(p_review_states) = 0
            or indexed.review_state = any(p_review_states)
            or ('unresolved' = any(p_review_states) and indexed.review_state in ('needs_review', 'pending_enrichment'))
          )
          and (p_directions is null or cardinality(p_directions) = 0 or indexed.direction = any(p_directions))
          and (p_date_start is null or indexed.transaction_date >= p_date_start)
          and (p_date_end is null or indexed.transaction_date <= p_date_end)
        order by
          ts_rank_cd(
            to_tsvector('english', indexed.search_bm25_text),
            keyword_query.ts_query
          ) desc,
          indexed.transaction_date desc,
          indexed.transaction_id
        limit greatest(1, p_match_count);
      $fn$;
    $sql$;
  end if;
end;
$$;

create or replace view public.agent_ledger_search_rows
with (security_invoker = true)
as
select
  t.id as transaction_id,
  t.user_id,
  t.account_id,
  t.economic_entity_id,
  coalesce(t.search_source_batch_key, 'transaction:' || t.id::text) as source_batch_key,
  t.transaction_date,
  coalesce(t.posted_date, t.transaction_date) as posted_at,
  t.amount_original as amount,
  t.currency_original as currency,
  t.merchant_normalized as merchant,
  t.counterparty_name as counterparty,
  t.category_code as category,
  public.transaction_search_direction(t.amount_original) as direction,
  public.transaction_search_review_state(
    t.needs_review,
    t.category_code,
    t.credit_card_statement_status,
    t.description_raw,
    t.description_clean,
    t.llm_payload
  ) as review_state,
  t.review_reason,
  t.description_raw as original_text,
  coalesce(t.search_contextualized_text, t.description_raw) as contextualized_text,
  coalesce(t.search_document_summary, '') as document_summary,
  t.search_embedding_status as embedding_status,
  t.created_at,
  coalesce(t.search_indexed_at, t.updated_at) as updated_at
from public.transactions t;
