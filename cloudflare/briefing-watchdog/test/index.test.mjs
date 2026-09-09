import assert from "node:assert/strict";
import test from "node:test";
import { githubHeaders, hktDate, modeForCron, reportPath } from "../src/index.js";

test("converts a UTC scheduled time to the HKT report date", () => {
  assert.equal(hktDate(new Date("2026-09-01T00:10:00Z")), "2026-09-01");
  assert.equal(hktDate(new Date("2026-09-01T16:25:00Z")), "2026-09-02");
});

test("routes the two watchdog schedules to the correct briefing", () => {
  assert.equal(modeForCron("10 0 * * *"), "morning");
  assert.equal(modeForCron("25 9 * * *"), "afternoon");
  assert.throws(() => modeForCron("0 0 * * *"), /Unsupported watchdog cron/);
});

test("identifies GitHub API requests without exposing the token", () => {
  const headers = githubHeaders({ GITHUB_TOKEN: "test-token" });
  assert.equal(headers["User-Agent"], "macro-flux-briefing-watchdog");
  assert.equal(headers.Authorization, "Bearer test-token");
});

test("uses the canonical remote report path", () => {
  assert.equal(reportPath("2026-09-01-morning"), "docs/past/2026-09-01-morning.md");
});
