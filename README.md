# magi-deep-research

Gemini Enterprise Deep Research Agent writer for MAGI.

Fetches the daily morning market brief from Gemini Enterprise's Deep
Research Agent, strips Jun-only content, and fan-outs the result to Box
(full brief), GCS (raw JSON envelope), and BigQuery
(`magi_core.market_research`, stripped summary).

Design reference: `MAGI-GE-DESIGN-001-v2`.

## Status

Phase C implementation is wired in `src/deep-research.mjs`.
`src/index.mjs` can run in `MAGI_BRIEF_MODE=deep-research` to call the
Gemini Enterprise `streamAssist` API with `agentId=deep_research` and
Magi BigQuery-backed data stores as the grounding corpus. Phase B
fallback (`fallback.mjs`) remains available via
`MAGI_BRIEF_MODE=fallback` (the default until the Deep Research API is
fully operational).

Modules:
- `src/deep-research.mjs` — `streamAssist` caller, Deep Research two-step
  flow, NDJSON/SSE/JSON-array stream parsing.
- `src/index.mjs` — Cloud Run Job entrypoint, trading-day gate, brief
  generation dispatch, Section 5 strip, fan-out to Box/GCS/BigQuery.
- `src/strip.mjs`, `src/fallback.mjs`, `src/bigquery.mjs`, `src/gcs.mjs`,
  `src/box.mjs`, `src/nyse.mjs` — supporting modules per design §3.1.

## Develop

```bash
node --test test/
```

Requires Node 20+. Run the test suite with Node's built-in runner.

## Absolute boundary (design §2.3)

- PLM (8 LLM Jobs) is NEVER routed through Gemini Enterprise.
- Vertex AI Gemini API and Gemini Developer API are NEVER replaced.
- Section 5 (`## 5. Jun Review Only`) — ticker picks, entry, stop,
  target — MUST NEVER reach the PLM. `strip.mjs` is the contractual
  enforcement point before BigQuery insert; any bypass of this module
  is a violation of the design's absolute boundary.
