import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import test from "node:test";

test("ft fund history python unit tests pass", () => {
  const workspaceRoot =
    "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp";
  const scriptPath = resolve(workspaceRoot, "scripts/test_ft_fund_history.py");
  const result = spawnSync("python3", [scriptPath], {
    cwd: workspaceRoot,
    encoding: "utf-8",
  });

  assert.equal(
    result.status,
    0,
    `python test failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
  );
});
