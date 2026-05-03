import assert from "node:assert/strict";
import test from "node:test";

import { formatMonthLabel } from "../apps/web/lib/dashboard.ts";

test("month labels format month-start dates in UTC", () => {
  assert.equal(formatMonthLabel("2026-01-01"), "Jan");
  assert.equal(formatMonthLabel("2026-05-01"), "May");
});
