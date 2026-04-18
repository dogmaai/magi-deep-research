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
- Section 5 (`## 5. Jun Review Only`) — ticker picks, entry, stop,
  target — MUST NEVER reach the PLM. `strip.mjs` is the contractual
  enforcement point before BigQuery insert; any bypass of this module
  is a violation of the design's absolute boundary.
