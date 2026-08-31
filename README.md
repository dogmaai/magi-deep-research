# magi-deep-research

Gemini Enterprise Deep Research Agent writer for MAGI.

Fetches the daily morning market brief from Gemini Enterprise's Deep
Research Agent, strips Jun-only content, and fan-outs the result to Box
(full brief), GCS (raw JSON envelope), and BigQuery
(`magi_core.market_research`, stripped summary).

Design reference: `MAGI-GE-DESIGN-001-v2`.

## Status

**Operational flow:** The weekday daily brief is now generated and
uploaded by the Devin Automation `MAGI 日次市場ブリーフ投入`
(`https://app.devin.ai/automations/36ae4174a1f84057a113bcd53fc1d570`),
which writes to `magi_core.market_research` via
`magi-core/scripts/upload-deep-research.mjs`. This repository remains
the historical/reference implementation of the Cloud Run Job design.

The original Phase C implementation is wired in `src/deep-research.mjs`.
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
- Section 5 (the "Jun Review Only" block) — ticker picks, entry, stop,
  target — MUST NEVER reach the PLM. `strip.mjs` is the contractual
  enforcement point before BigQuery insert; any bypass of this module
  is a violation of the design's absolute boundary.

`strip.mjs` removes Section 5 three ways (defense in depth), so it holds
for both the Phase B fallback format and the live Phase C format:

1. **Sentinel fence** — `buildPrompt()` instructs the model to wrap
   Section 5 between `<!--MAGI:SECTION5:BEGIN-->` and
   `<!--MAGI:SECTION5:END-->`. This is numbering- and
   language-agnostic and is the primary contract.
2. **Heading label** — any H2 carrying the "Jun Review Only" label is
   stripped regardless of section numbering (`## 5.`, `## §5`, …) or a
   `(CONFIDENTIAL)` trailer. (Real `DAILY_DEEP_RESEARCH` rows use `§`
   numbering and localized titles, so a literal `## 5. Jun Review Only`
   match alone would silently no-op.)
3. **Leak sensor** — after stripping, the residue is scanned for
   actionable-pick lines (a ticker — `$AAPL` *or* bare `AAPL` — together
   with a LONG/SHORT direction and/or entry/stop/target levels) and for
   `$TICKER` density (≥5). Either condition flags `status='partial'`.
   The legitimate Watchlist (bare tickers in prose) does not trip it.

The same `containsSection5()` predicate backs the writer-boundary guards
in `bigquery.mjs` and `box.mjs`, so the strip and the guards cannot
drift apart.
