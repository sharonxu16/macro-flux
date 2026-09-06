const HKT_TIME_ZONE = "Asia/Hong_Kong";
const WATCHDOG_EVENT = "briefing_watchdog";
const MORNING_CRON = "10 0 * * *";
const AFTERNOON_CRON = "25 10 * * *";

function hktDate(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: HKT_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const values = Object.fromEntries(parts.map(({ type, value }) => [type, value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function modeForCron(cron) {
  if (cron === MORNING_CRON) return "morning";
  if (cron === AFTERNOON_CRON) return "afternoon";
  throw new Error(`Unsupported watchdog cron: ${cron}`);
}

function reportPath(reportId) {
  return `docs/past/${reportId}.md`;
}

function githubHeaders(env) {
  if (!env.GITHUB_TOKEN) throw new Error("GITHUB_TOKEN secret is not configured");
  return {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };
}

function githubUrl(env, suffix) {
  const owner = env.GITHUB_OWNER || "sharonxu16";
  const repo = env.GITHUB_REPO || "macro-flux";
  return `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}${suffix}`;
}

async function remoteReportExists(env, targetFile) {
  const response = await fetch(
    `${githubUrl(env, `/contents/${targetFile}`)}?ref=main`,
    { headers: githubHeaders(env) },
  );
  if (response.status === 200) return { exists: true, result: "exists" };
  if (response.status === 404) return { exists: false, result: "missing" };
  return { exists: null, result: `error_http_${response.status}` };
}

async function dispatchBriefing(env, mode, targetDate) {
  const response = await fetch(githubUrl(env, "/dispatches"), {
    method: "POST",
    headers: { ...githubHeaders(env), "Content-Type": "application/json" },
    body: JSON.stringify({
      event_type: WATCHDOG_EVENT,
      client_payload: {
        briefing_type: mode,
        run_date_hkt: targetDate,
      },
    }),
  });
  return response.status === 204;
}

async function runWatchdog(event, env) {
  const mode = modeForCron(event.cron);
  const targetDate = hktDate(new Date(event.scheduledTime || Date.now()));
  const reportId = `${targetDate}-${mode}`;
  const targetFile = reportPath(reportId);
  const baseLog = {
    service: "briefing-watchdog",
    scheduled_mode: mode,
    report_id: reportId,
    target_file: targetFile,
    check_time_hkt: new Intl.DateTimeFormat("sv-SE", {
      timeZone: HKT_TIME_ZONE,
      dateStyle: "short",
      timeStyle: "medium",
    }).format(new Date()),
  };

  const check = await remoteReportExists(env, targetFile);
  if (check.exists === true) {
    console.log(JSON.stringify({ ...baseLog, remote_check_result: check.result, result: "skip", skip_reason: "remote_target_exists" }));
    return;
  }
  if (check.exists !== false) {
    console.error(JSON.stringify({ ...baseLog, remote_check_result: check.result, result: "failed", skip_reason: "remote_check_failed" }));
    return;
  }

  const dispatched = await dispatchBriefing(env, mode, targetDate);
  console.log(JSON.stringify({
    ...baseLog,
    remote_check_result: check.result,
    result: dispatched ? "dispatched" : "dispatch_failed",
    skip_reason: "",
  }));
  if (!dispatched) throw new Error(`GitHub repository dispatch failed for ${reportId}`);
}

export default {
  async scheduled(event, env) {
    await runWatchdog(event, env);
  },
};

export { hktDate, modeForCron, reportPath };
