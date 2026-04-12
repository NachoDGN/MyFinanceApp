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
