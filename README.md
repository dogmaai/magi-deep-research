# magi-deep-research

Gemini Enterprise Deep Research Agent writer for MAGI.

Fetches the daily morning market brief from Gemini Enterprise's Deep
Research Agent (Preview, allowlist-gated), strips Jun-only content, and
fan-outs the result to Box (full brief), GCS (raw JSON envelope), and
BigQuery (`magi_core.market_research`, stripped summary).

Design reference: `MAGI-GE-DESIGN-001-v2`.

## Status

Phase A (foundation work). Allowlist approval is pending; Phase B will
operate the fallback `generateDeepBrief()` path manually; Phase C lights
up the full Cloud Run Job automation.

This repository currently ships **only** the Phase A-8 deliverable:

- `src/strip.mjs` — Section 5 stripper (contractual strip point before
  BigQuery insert; see design §2.3 and §5.3).
- `test/strip.test.mjs` — 12 cases covering basic strip, multi-section,
  CRLF, sub-headings, ticker leak detection, and input validation.

The remaining modules listed in design §3.1 (`src/deep-research.mjs`,
`src/box.mjs`, `src/bigquery.mjs`, `src/gcs.mjs`, `src/fallback.mjs`,
`src/nyse.mjs`, `src/index.mjs`) will land in subsequent PRs and are
deliberately **not** scaffolded here to keep the first PR reviewable.

## Develop

```bash
node --test test/
```

Requires Node 20+. `src/strip.mjs` has no runtime dependencies; the test
suite uses the built-in `node:test` runner.

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
