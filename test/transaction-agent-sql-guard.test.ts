import test from "node:test";
import assert from "node:assert/strict";

import {
  isDuplicateAgentSearchQuery,
  sanitizeReadOnlyLedgerSql,
} from "../packages/db/src/transaction-agent-sql-guard";

test("ledger SQL guard accepts SELECT on allowlisted agent views and appends a limit", () => {
  const query = sanitizeReadOnlyLedgerSql(`
    select transaction_id, amount_base_eur
    from public.agent_ledger_transactions
    where transaction_class = 'expense'
  `);

  assert.match(query, /^select/iu);
  assert.match(query, /from public\.agent_ledger_transactions/iu);
  assert.match(query, /limit 25$/iu);
});

test("ledger SQL guard accepts WITH queries that read allowlisted views", () => {
  const query = sanitizeReadOnlyLedgerSql(`
    with spend as (
      select transaction_id
      from agent_ledger_transactions
    )
    select transaction_id
    from spend
  `);

  assert.match(query, /^with/iu);
  assert.match(query, /from spend/iu);
  assert.match(query, /limit 25$/iu);
});

test("ledger SQL guard allows the agent FX view", () => {
  const query = sanitizeReadOnlyLedgerSql(`
    select rate
    from public.agent_ledger_fx_rates
    where base_currency = 'EUR' and quote_currency = 'USD'
    order by as_of_date desc
  `);

  assert.match(query, /from public\.agent_ledger_fx_rates/iu);
  assert.match(query, /limit 25$/iu);
});

test("ledger SQL guard blocks mutations and non-agent relations", () => {
  assert.throws(
    () => sanitizeReadOnlyLedgerSql("update agent_ledger_transactions set amount_base_eur = 0"),
    /SELECT or WITH/,
  );

  assert.throws(
    () => sanitizeReadOnlyLedgerSql("select * from public.transactions"),
    /agent ledger views/,
  );
});

test("duplicate search detection normalizes punctuation and case", () => {
  assert.equal(
    isDuplicateAgentSearchQuery("Cardisa invoice?", [
      "cardisa invoice",
      "custodio spend",
    ]),
    true,
  );
  assert.equal(
    isDuplicateAgentSearchQuery("Cardisa latest invoice", [
      "cardisa invoice",
      "custodio spend",
    ]),
    false,
  );
});
