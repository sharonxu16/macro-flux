# Macro Flux external briefing watchdog

This Cloudflare Workers Free worker is the external scheduler for Macro Flux.
It runs at 08:10 and 18:25 HKT, checks the target report on the remote `main`
branch, and sends a `repository_dispatch` only when the report is missing.
It never calls the news sources, the LLM, or SMTP.

## One-time setup

1. Create a free Cloudflare Workers account and install Wrangler.
2. From this directory, run `wrangler login`.
3. Create a fine-grained GitHub token restricted to `sharonxu16/macro-flux` with
   only the repository `Contents: write` permission required by the GitHub
   repository-dispatch API. Do not put the token in this repository.
4. Store it as a Cloudflare secret:

   `wrangler secret put GITHUB_TOKEN`

5. Deploy the worker:

   `wrangler deploy`

The two UTC cron expressions are intentional: Cloudflare Cron Triggers use UTC.
The GitHub workflow accepts only the `briefing_watchdog` event and carries the
target date and mode in the dispatch payload. Its remote idempotency check and
single email step remain the final duplicate protection.

## Safe checks

Use `wrangler tail` to inspect status logs. Logs contain report IDs and result
states only; the GitHub token is never printed. A failed remote check fails
closed and does not trigger generation.
