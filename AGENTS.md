# Rules for anyone (human or agent) changing this repo

1. **Never add order execution.** No broker order endpoints, no paper-trading APIs.
   Trades are scored by a replay simulator, never placed.
2. **Point-in-time is non-negotiable.** Every silver row carries `event_ts` (when it
   happened) and `received_ts` (when we knew it). Downstream joins use `received_ts`.
   Never backfill a field from data that arrived later.
3. **Bronze is append-only.** Raw responses are never edited or deleted. Silver and
   anything above it must be rebuildable from bronze (`confluence rebuild-silver`).
4. **One process owns the QuantData key.** All requests go through `QuantDataClient`
   and its rate limiter. The 240/min quota is per user and shared with other agents.
5. **No secrets in code or config.** Keys live only in `.env`.
6. **Schema changes are additive.** Add columns; do not rename or repurpose existing
   ones. Note every change in `docs/SCHEMA.md`.
7. **Only documented QuantData fields.** Do not invent endpoints, filters or enum values.
   Reference: https://quantdata.us/llms-full.txt
