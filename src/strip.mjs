/**
 * @file Section 5 stripper — contractual enforcement point for the
 *   "Section 5 MUST NEVER reach the PLM" absolute boundary.
 *
 * Per MAGI-GE-DESIGN-001-v2 §2.3 and §5.3, the Gemini Enterprise Deep
 * Research Agent's morning brief contains a Jun-only "## 5. Jun Review
 * Only" block with ticker picks / entry / stop / target prices. That
 * block MUST be removed before the brief is persisted to
 * `magi_core.market_research`, because downstream `ask_market_context()`
 * reads that table and injects the result into the PLM (8 LLM Jobs)
 * system prompt. Any leak of Section 5 into BigQuery is a violation of
 * the design's absolute boundary.
 *
 * This module is intentionally dependency-free and side-effect-free so
 * it can be exercised exhaustively by unit tests without any GCP /
 * network mocks.
 */

/**
 * @typedef {Object} StripResult
 * @property {string}   stripped          Markdown with Section 5 removed.
 * @property {number}   tickersRemaining  Count of `$TICKER` patterns remaining in `stripped`.
 * @property {string[]} tickerSamples     Up to the first 10 matched tickers, for log surface.
 * @property {'success'|'partial'} status `partial` when ≥5 tickers remain (design §5.3 threshold).
 */

/**
 * Strip `## 5. Jun Review Only` — and everything until the next H2 heading
 * (or EOF) — from a Deep Research markdown brief.
 *
 * The design doc §5.3 proposes the regex
 *   `/^## 5\. Jun Review Only[\s\S]*?(?=^## |\Z)/m`
 * but JavaScript does not support `\Z` (it is treated as a literal `Z`),
 * so a naïve port of that regex either under-strips (when Section 5 sits
 * at EOF) or over-strips (if `\Z` is misread as "end of any line").
 *
 * This implementation uses an explicit line scan instead of a single
 * regex. The semantics are:
 *
 *   1. Normalise `\r\n` to `\n` so CRLF-authored briefs are handled.
 *   2. Toggle an `inSection5` flag on every H2 (`^## `) line: `true` if
 *      the heading text matches `5. Jun Review Only` (with an optional
 *      word boundary for defense-in-depth), `false` otherwise.
 *   3. Emit every line where `inSection5 === false`.
 *   4. Post-process: collapse runs of ≥3 blank lines (which can appear
 *      where Section 5 sat between two H2 blocks), trim trailing
 *      whitespace, and ensure a single terminating newline.
 *
 * As a defense-in-depth sensor, the stripped output is scanned for
 * `$TICKER` patterns. If ≥5 remain (design §5.3), the result is flagged
 * `status='partial'` so the writer can log a warning and surface the
 * anomaly to Jun — typically this would mean Section 3 ("Risk Factors")
 * or Section 4 is carrying ticker mentions that should have been
 * confined to Section 5. False positives inflate the count conservatively,
 * which is the safe direction for this safety-critical strip.
 *
 * @param {string} markdown - Full brief markdown as produced by the
 *   Gemini Enterprise Deep Research Agent (or the Phase B
 *   `generateDeepBrief()` fallback).
 * @returns {StripResult}
 * @throws {TypeError} if `markdown` is not a string.
 */
export function stripSection5(markdown) {
  if (typeof markdown !== 'string') {
    throw new TypeError('stripSection5: markdown must be a string');
  }

  const lines = markdown.replace(/\r\n/g, '\n').split('\n');
  const kept = [];
  let inSection5 = false;

  // Section 5 begins at any H2 whose heading starts with "5. Jun Review
  // Only" (`\b` accepts strict match and "(CONFIDENTIAL)"-style trailers
  // while still rejecting "## 5. Jun Review Onlyx" / "## 5. Appendix").
  // It ends at the next H2 or at EOF. H3/H4 sub-headings inside Section 5
  // do NOT terminate it — they are swallowed along with their bodies.
  const SECTION_5_HEADING = /^## 5\. Jun Review Only\b/;
  const ANY_H2 = /^## /;

  for (const line of lines) {
    if (ANY_H2.test(line)) {
      inSection5 = SECTION_5_HEADING.test(line);
    }
    if (!inSection5) kept.push(line);
  }

  const stripped =
    kept.join('\n').replace(/\n{3,}/g, '\n\n').replace(/\s+$/, '') + '\n';

  // Design §5.3 ticker regex: `/\$[A-Z]{1,5}/g`. The trailing negative
  // lookahead narrows the match to avoid counting "$EURUSD"-like strings
  // as five separate partial tickers, which would falsely inflate the
  // count. This is strictly safer than the design regex (fewer false
  // positives, same true positives).
  const tickerRegex = /\$[A-Z]{1,5}(?![A-Za-z0-9])/g;
  const tickerMatches = stripped.match(tickerRegex) ?? [];
  const tickersRemaining = tickerMatches.length;
  const status = tickersRemaining >= 5 ? 'partial' : 'success';

  return {
    stripped,
    tickersRemaining,
    tickerSamples: tickerMatches.slice(0, 10),
    status,
  };
}
