create extension if not exists vector with schema extensions;

alter table public.transactions
  add column if not exists description_embedding extensions.vector(768);
