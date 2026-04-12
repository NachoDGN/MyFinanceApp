do $$
begin
  if exists (
    select 1
    from pg_available_extensions
    where name = 'pg_textsearch'
  ) then
    create extension if not exists pg_textsearch;
  end if;
end;
$$;

alter type public.job_type add value if not exists 'transaction_search_index';

create table if not exists public.transaction_search_batches (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  source_batch_key text not null unique,
  account_id uuid references public.accounts(id) on delete set null,
  account_name text,
  institution_name text,
  period_start date,
  period_end date,
  batch_summary text not null,
  extracted_metadata jsonb not null default '{}'::jsonb,
  status text not null default 'ready' check (status in ('ready', 'processing', 'stale', 'failed')),
  last_indexed_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.transaction_search_rows (
  transaction_id uuid primary key references public.transactions(id) on delete cascade,
  batch_id uuid not null references public.transaction_search_batches(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  economic_entity_id uuid references public.entities(id) on delete set null,
  transaction_date date not null,
  posted_at date,
  amount numeric(20, 8),
  currency text,
  merchant text,
  counterparty text,
  category text,
  account_name text,
  institution_name text,
  account_type public.account_type,
  economic_entity_name text,
  economic_entity_kind public.entity_kind,
  direction text not null check (direction in ('debit', 'credit', 'neutral')),
  review_state text not null check (review_state in ('pending_enrichment', 'needs_review', 'resolved')),
  review_reason text,
  original_text text not null,
  contextualized_text text not null,
  document_summary text not null,
  bm25_text text generated always as (contextualized_text) stored,
  embedding vector(3072) not null,
  embedding_model text not null,
  embedding_status text not null default 'ready' check (embedding_status in ('ready', 'stale', 'missing')),
  embedding_source_text text not null default 'contextualized_text' check (embedding_source_text in ('original_text', 'contextualized_text')),
  contextualization_model text not null,
  contextualization_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists transaction_search_batches_user_status_idx
on public.transaction_search_batches (user_id, status, period_end desc nulls last, created_at desc);

create index if not exists transaction_search_batches_account_idx
on public.transaction_search_batches (account_id, period_start desc nulls last);

create index if not exists transaction_search_rows_user_review_state_idx
on public.transaction_search_rows (user_id, review_state, transaction_date desc, created_at desc);

create index if not exists transaction_search_rows_account_idx
on public.transaction_search_rows (account_id, transaction_date desc, created_at desc);

create index if not exists transaction_search_rows_entity_idx
on public.transaction_search_rows (economic_entity_id, transaction_date desc, created_at desc);

create index if not exists transaction_search_rows_batch_idx
on public.transaction_search_rows (batch_id, transaction_date desc, transaction_id);

do $$
begin
  if exists (
    select 1
    from pg_extension
    where extname = 'pg_textsearch'
  ) then
    execute '
      create index if not exists transaction_search_rows_bm25_idx
      on public.transaction_search_rows
      using bm25 (bm25_text)
      with (text_config = ''english'')
    ';
  else
    execute '
      create index if not exists transaction_search_rows_bm25_idx
      on public.transaction_search_rows
      using gin (to_tsvector(''english'', bm25_text))
    ';
  end if;
end;
$$;

create index if not exists transaction_search_rows_embedding_idx
on public.transaction_search_rows
using hnsw ((embedding::halfvec(3072)) halfvec_cosine_ops);

create or replace function public.mark_transaction_search_row_embedding_stale()
returns trigger
language plpgsql
as $$
begin
  if new.contextualized_text is distinct from old.contextualized_text
     and new.embedding is not distinct from old.embedding then
    new.embedding_status := 'stale';
    new.embedding_source_text := 'contextualized_text';
  end if;

  return new;
end;
$$;

create or replace function public.set_transaction_search_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists set_transaction_search_batches_updated_at
on public.transaction_search_batches;

create trigger set_transaction_search_batches_updated_at
before update on public.transaction_search_batches
for each row
execute function public.set_transaction_search_updated_at();

drop trigger if exists set_transaction_search_rows_updated_at
on public.transaction_search_rows;

create trigger set_transaction_search_rows_updated_at
before update on public.transaction_search_rows
for each row
execute function public.set_transaction_search_updated_at();

drop trigger if exists mark_transaction_search_row_embedding_stale
on public.transaction_search_rows;

create trigger mark_transaction_search_row_embedding_stale
before update on public.transaction_search_rows
for each row
when (new.contextualized_text is distinct from old.contextualized_text)
execute function public.mark_transaction_search_row_embedding_stale();

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
  with approximate_candidates as (
    select
      r.transaction_id,
      r.batch_id,
      b.source_batch_key,
      r.transaction_date,
      r.posted_at,
      r.amount,
      r.currency,
      r.merchant,
      r.counterparty,
      r.category,
      r.account_id,
      r.account_name,
      r.institution_name,
      r.account_type,
      r.economic_entity_id,
      r.economic_entity_name,
      r.economic_entity_kind,
      r.direction,
      r.review_state,
      r.review_reason,
      r.original_text,
      r.contextualized_text,
      r.document_summary,
      r.embedding_source_text,
      r.embedding,
      r.embedding::halfvec(3072) <=> p_query_embedding::halfvec(3072) as approx_distance
    from public.transaction_search_rows as r
    join public.transaction_search_batches as b
      on b.id = r.batch_id
    where r.user_id = p_user_id
      and b.status = 'ready'
      and r.embedding_status = 'ready'
      and (p_account_ids is null or cardinality(p_account_ids) = 0 or r.account_id = any(p_account_ids))
      and (p_entity_ids is null or cardinality(p_entity_ids) = 0 or r.economic_entity_id = any(p_entity_ids))
      and (p_account_types is null or cardinality(p_account_types) = 0 or r.account_type = any(p_account_types))
      and (p_entity_kinds is null or cardinality(p_entity_kinds) = 0 or r.economic_entity_kind = any(p_entity_kinds))
      and (
        p_review_states is null
        or cardinality(p_review_states) = 0
        or r.review_state = any(p_review_states)
        or ('unresolved' = any(p_review_states) and r.review_state in ('needs_review', 'pending_enrichment'))
      )
      and (p_directions is null or cardinality(p_directions) = 0 or r.direction = any(p_directions))
      and (p_date_start is null or r.transaction_date >= p_date_start)
      and (p_date_end is null or r.transaction_date <= p_date_end)
    order by r.embedding::halfvec(3072) <=> p_query_embedding::halfvec(3072)
    limit greatest(1, p_match_count * 4)
  )
  select
    transaction_id,
    batch_id,
    source_batch_key,
    transaction_date,
    posted_at,
    amount,
    currency,
    merchant,
    counterparty,
    category,
    account_id,
    account_name,
    institution_name,
    account_type,
    economic_entity_id,
    economic_entity_name,
    economic_entity_kind,
    direction,
    review_state,
    review_reason,
    original_text,
    contextualized_text,
    document_summary,
    embedding_source_text,
    embedding <=> p_query_embedding as semantic_distance
  from approximate_candidates
  order by embedding <=> p_query_embedding
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
        select
          r.transaction_id,
          r.batch_id,
          b.source_batch_key,
          r.transaction_date,
          r.posted_at,
          r.amount,
          r.currency,
          r.merchant,
          r.counterparty,
          r.category,
          r.account_id,
          r.account_name,
          r.institution_name,
          r.account_type,
          r.economic_entity_id,
          r.economic_entity_name,
          r.economic_entity_kind,
          r.direction,
          r.review_state,
          r.review_reason,
          r.original_text,
          r.contextualized_text,
          r.document_summary,
          r.bm25_text <@> to_bm25query(p_keyword_query, 'transaction_search_rows_bm25_idx') as bm25_score
        from public.transaction_search_rows as r
        join public.transaction_search_batches as b
          on b.id = r.batch_id
        where r.user_id = p_user_id
          and b.status = 'ready'
          and r.bm25_text is not null
          and (p_account_ids is null or cardinality(p_account_ids) = 0 or r.account_id = any(p_account_ids))
          and (p_entity_ids is null or cardinality(p_entity_ids) = 0 or r.economic_entity_id = any(p_entity_ids))
          and (p_account_types is null or cardinality(p_account_types) = 0 or r.account_type = any(p_account_types))
          and (p_entity_kinds is null or cardinality(p_entity_kinds) = 0 or r.economic_entity_kind = any(p_entity_kinds))
          and (
            p_review_states is null
            or cardinality(p_review_states) = 0
            or r.review_state = any(p_review_states)
            or ('unresolved' = any(p_review_states) and r.review_state in ('needs_review', 'pending_enrichment'))
          )
          and (p_directions is null or cardinality(p_directions) = 0 or r.direction = any(p_directions))
          and (p_date_start is null or r.transaction_date >= p_date_start)
          and (p_date_end is null or r.transaction_date <= p_date_end)
        order by r.bm25_text <@> to_bm25query(p_keyword_query, 'transaction_search_rows_bm25_idx')
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
        )
        select
          r.transaction_id,
          r.batch_id,
          b.source_batch_key,
          r.transaction_date,
          r.posted_at,
          r.amount,
          r.currency,
          r.merchant,
          r.counterparty,
          r.category,
          r.account_id,
          r.account_name,
          r.institution_name,
          r.account_type,
          r.economic_entity_id,
          r.economic_entity_name,
          r.economic_entity_kind,
          r.direction,
          r.review_state,
          r.review_reason,
          r.original_text,
          r.contextualized_text,
          r.document_summary,
          ts_rank_cd(
            to_tsvector('english', r.bm25_text),
            keyword_query.ts_query
          )::double precision as bm25_score
        from public.transaction_search_rows as r
        join public.transaction_search_batches as b
          on b.id = r.batch_id
        cross join keyword_query
        where r.user_id = p_user_id
          and b.status = 'ready'
          and r.bm25_text is not null
          and numnode(keyword_query.ts_query) > 0
          and to_tsvector('english', r.bm25_text) @@ keyword_query.ts_query
          and (p_account_ids is null or cardinality(p_account_ids) = 0 or r.account_id = any(p_account_ids))
          and (p_entity_ids is null or cardinality(p_entity_ids) = 0 or r.economic_entity_id = any(p_entity_ids))
          and (p_account_types is null or cardinality(p_account_types) = 0 or r.account_type = any(p_account_types))
          and (p_entity_kinds is null or cardinality(p_entity_kinds) = 0 or r.economic_entity_kind = any(p_entity_kinds))
          and (
            p_review_states is null
            or cardinality(p_review_states) = 0
            or r.review_state = any(p_review_states)
            or ('unresolved' = any(p_review_states) and r.review_state in ('needs_review', 'pending_enrichment'))
          )
          and (p_directions is null or cardinality(p_directions) = 0 or r.direction = any(p_directions))
          and (p_date_start is null or r.transaction_date >= p_date_start)
          and (p_date_end is null or r.transaction_date <= p_date_end)
        order by bm25_score desc, r.transaction_date desc, r.transaction_id
        limit greatest(1, p_match_count);
      $fn$;
    $sql$;
  end if;
end;
$$;
