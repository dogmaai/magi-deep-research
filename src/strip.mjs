/**
 * @file Section 5 stripper — contractual enforcement point for the
 *   "Section 5 MUST NEVER reach the PLM" absolute boundary.
 *
 * Per MAGI-GE-DESIGN-001-v2 §2.3 and §5.3, the Gemini Enterprise Deep
 * Research Agent's morning brief contains a Jun-only "Jun Review Only"
 * block with ticker picks / entry / stop / target prices. That block
 * MUST be removed before the brief is persisted to
 * `magi_core.market_research`, because downstream `ask_market_context()`
 * reads that table and injects the result into the PLM (8 LLM Jobs)
 * system prompt. Any leak of Section 5 into BigQuery is a violation of
 * the design's absolute boundary.
 *
 * Why this module is heading-format tolerant: the live
 * `DAILY_DEEP_RESEARCH` rows already in `magi_core.market_research` do
 * NOT use the design's `## 5. Jun Review Only` heading — they use
 * section-sign numbering (`## §1` … `## §7`) and localized titles. A
 * strip keyed only on the literal `## 5. Jun Review Only` would silently
 * no-op on that real-world format, so the Jun-only block is detected
 * here three ways (defense in depth):
 *
 *   1. Sentinel fence — `<!--MAGI:SECTION5:BEGIN-->` …
 *      `<!--MAGI:SECTION5:END-->`. Emitted by `buildPrompt()` around
 *      Section 5; language- and numbering-agnostic, so it survives any
 *      heading rewording or localization.
 *   2. Heading label — any H2 whose text carries the "Jun Review Only"
 *      confidential label, regardless of the leading number / `§` /
 *      full-width prefix or a trailing "(CONFIDENTIAL)".
 *   3. Leak sensor — after stripping, the residue is scanned for
 *      actionable-pick lines (a ticker co-occurring with a LONG/SHORT
 *      direction and/or entry/stop/target levels) and for `$TICKER`
 *      density, flagging `status='partial'` so the anomaly is surfaced.
 *
 * This module is intentionally dependency-free and side-effect-free so
 * it can be exercised exhaustively by unit tests without any GCP /
 * network mocks.
 */

/**
 * Machine-readable sentinel fence wrapping Section 5. `buildPrompt()`
 * instructs the model to emit these verbatim around the Jun-only block;
 * `stripSection5()` removes everything between them (inclusive) before
 * any heading parsing runs. This is the most robust contract because it
 * does not depend on the heading text at all.
 */
export const SECTION5_SENTINEL_BEGIN = '<!--MAGI:SECTION5:BEGIN-->';
export const SECTION5_SENTINEL_END = '<!--MAGI:SECTION5:END-->';

/**
 * Section 5 begins at any H2 whose heading carries the "Jun Review
 * Only" label. The match is keyed on the LABEL, not the section number,
 * because real briefs number this section differently (`## 5.` in the
 * design prompt, `## §5` / localized in the live BigQuery rows). The
 * leading `.*` absorbs whatever numbering / prefix precedes the label;
 * the trailing `\b` allows "(CONFIDENTIAL)"-style trailers while still
 * rejecting "## 5. Appendix (not review)".
 */
const SECTION_5_HEADING = /^##\s+.*\bjun\s+review\s+only\b/i;
const ANY_H2 = /^## /;

/**
 * @typedef {Object} StripResult
 * @property {string}   stripped          Markdown with Section 5 removed.
 * @property {boolean}  section5Removed   True if a sentinel fence or Jun-only heading was stripped.
 * @property {number}   tickersRemaining  Count of `$TICKER` patterns remaining in `stripped`.
 * @property {string[]} tickerSamples     Up to the first 10 matched `$TICKER` tokens, for log surface.
 * @property {boolean}  leakSuspected     True if an actionable Jun-only pick line survived the strip.
 * @property {string[]} leakSamples       Up to the first 10 suspected pick lines, for log surface.
 * @property {'success'|'partial'} status `partial` when ≥5 `$TICKER` remain (design §5.3) or a pick leak is suspected.
 */

/**
 * Strip the Jun-only "Jun Review Only" section — and everything until
 * the next H2 heading (or EOF) — from a Deep Research markdown brief.
 *
 * The design doc §5.3 proposes the regex
 *   `/^## 5\. Jun Review Only[\s\S]*?(?=^## |\Z)/m`
 * but JavaScript does not support `\Z` (it is treated as a literal `Z`),
 * and — more importantly — that regex is keyed on the exact `## 5.`
 * numbering, which the live brief format does not use. This
 * implementation uses an explicit line scan with sentinel + label
 * detection instead. The semantics are:
 *
 *   1. Normalise `\r\n` to `\n` so CRLF-authored briefs are handled.
 *   2. Drop everything inside a `<!--MAGI:SECTION5:BEGIN-->` …
 *      `<!--MAGI:SECTION5:END-->` sentinel fence (inclusive). An
 *      unterminated fence drops through to EOF (fail-safe).
 *   3. Toggle an `inSection5` flag on every H2 (`^## `) line: `true` if
 *      the heading carries the "Jun Review Only" label, `false`
 *      otherwise. Emit every line where neither flag is set. H3/H4
 *      sub-headings inside Section 5 do NOT terminate it.
 *   4. Post-process: collapse runs of ≥3 blank lines, trim trailing
 *      whitespace, and ensure a single terminating newline.
 *
 * As a defense-in-depth sensor, the stripped output is scanned for
 * `$TICKER` density (≥5 → `partial`, design §5.3) and for
 * actionable-pick lines (a ticker plus a LONG/SHORT direction and/or
 * entry/stop/target levels — the signature of a leaked Jun-only pick).
 * Either condition flags `status='partial'` so the writer can log a
 * warning and surface the anomaly to Jun. The sensor is deliberately
 * conservative about pick detection so the legitimate Watchlist section
 * (which carries bare tickers in prose) does not raise false alarms.
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
  let inFence = false;
  let inSection5 = false;
  let section5Removed = false;

  for (const line of lines) {
    // (1) Sentinel fence — highest priority, language/heading-agnostic.
    if (!inFence && line.includes(SECTION5_SENTINEL_BEGIN)) {
      inFence = true;
      section5Removed = true;
      continue; // drop the BEGIN line
    }
    if (inFence) {
      if (line.includes(SECTION5_SENTINEL_END)) inFence = false;
      continue; // drop fence body and the END line
    }
    // A stray END sentinel without a BEGIN is an artifact — never keep it.
    if (line.includes(SECTION5_SENTINEL_END)) continue;

    // (2) Heading-label toggle.
    if (ANY_H2.test(line)) {
      inSection5 = SECTION_5_HEADING.test(line);
      if (inSection5) section5Removed = true;
    }
    if (!inSection5) kept.push(line);
  }

  const stripped =
    kept.join('\n').replace(/\n{3,}/g, '\n\n').replace(/\s+$/, '') + '\n';

  // (3a) `$TICKER` density sensor. Design §5.3 ticker regex:
  // `/\$[A-Z]{1,5}/g`. The trailing negative lookahead narrows the
  // match to avoid counting "$EURUSD"-like strings as five separate
  // partial tickers, which would falsely inflate the count.
  const tickerRegex = /\$[A-Z]{1,5}(?![A-Za-z0-9])/g;
  const tickerMatches = stripped.match(tickerRegex) ?? [];
  const tickersRemaining = tickerMatches.length;

  // (3b) Actionable-pick sensor. A leaked Jun-only pick line co-locates
  // a ticker with trade instructions. We flag a line only when it has
  // strong pick structure so the Watchlist's qualitative bare-ticker
  // prose does not trip the alarm.
  const leakSamples = detectPickLeaks(stripped);
  const leakSuspected = leakSamples.length > 0;

  const status =
    tickersRemaining >= 5 || leakSuspected ? 'partial' : 'success';

  return {
    stripped,
    section5Removed,
    tickersRemaining,
    tickerSamples: tickerMatches.slice(0, 10),
    leakSuspected,
    leakSamples,
    status,
  };
}

/**
 * Detect actionable Jun-only pick lines that survived the strip.
 *
 * A line is flagged when it carries a ticker token (either `$TICKER`
 * or a bare 2–5 letter all-caps symbol) together with EITHER:
 *   - a LONG/SHORT direction AND at least one price level keyword
 *     (entry / stop / target, English or Japanese) or two numeric
 *     prices, OR
 *   - two or more distinct price level keywords (e.g.
 *     "entry 150 stop 148 target 155").
 *
 * This is the structural signature of a Section 5 pick. Qualitative
 * Watchlist prose ("AAPL: AI roadmap is solid") has a ticker but no
 * direction and at most one stray level word, so it is not flagged.
 *
 * @param {string} text
 * @returns {string[]} up to 10 suspected pick lines, trimmed.
 */
function detectPickLeaks(text) {
  const hasTicker = (line) =>
    /\$[A-Z]{1,5}(?![A-Za-z0-9])/.test(line) || /\b[A-Z]{2,5}\b/.test(line);
  // Direction tokens are matched case-sensitively for the English
  // forms so "long-term" / "short-term" prose does not register.
  const hasDirection = (line) =>
    /\b(?:LONG|SHORT)\b/.test(line) || /(?:ロング|ショート)/.test(line);
  const levelCount = (line) => {
    let n = 0;
    if (/\bentr(?:y|ies)\b/i.test(line) || /エントリー/.test(line)) n += 1;
    if (/\bstop(?:[\s-]?loss)?\b/i.test(line) || /(?:損切り|ストップ)/.test(line)) n += 1;
    if (
      /\b(?:target|take[\s-]?profit)\b/i.test(line) ||
      /(?:利確|利食い|目標株価)/.test(line)
    ) {
      n += 1;
    }
    return n;
  };
  // Standalone price-like numbers (≥2 digits, optional decimals/commas),
  // used to corroborate a directional pick written without level words
  // (e.g. a "| AAPL | LONG | 150 | 148 | 155 |" table row).
  const priceCount = (line) => (line.match(/\b\d{2,7}(?:[.,]\d+)?\b/g) ?? []).length;

  const out = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (line === '' || !hasTicker(line)) continue;
    const levels = levelCount(line);
    const directional = hasDirection(line);
    const isPick =
      (directional && (levels >= 1 || priceCount(line) >= 2)) || levels >= 2;
    if (isPick) {
      out.push(line);
      if (out.length >= 10) break;
    }
  }
  return out;
}

/**
 * Predicate used by the writer-boundary guards (`bigquery.mjs`,
 * `box.mjs`) to reject any payload that still contains Section 5.
 * Centralised here so the §2.3 detection logic lives in exactly one
 * auditable place and cannot drift between the strip and the guards.
 *
 * @param {unknown} markdown
 * @returns {boolean} true if a sentinel fence or Jun-only heading is present.
 */
export function containsSection5(markdown) {
  if (typeof markdown !== 'string') return false;
  const text = markdown.replace(/\r\n/g, '\n');
  if (text.includes(SECTION5_SENTINEL_BEGIN)) return true;
  return text.split('\n').some((line) => SECTION_5_HEADING.test(line));
}
