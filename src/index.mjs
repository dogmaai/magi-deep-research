/**
 * @file Cloud Run Job entrypoint for MAGI Deep Research (skeleton).
 *
 * This is the Phase A-11 / Phase C scaffold. It wires together the
 * five Phase A modules (`nyse`, `fallback`, `strip`, `bigquery`,
 * `gcs`, `box`) into the order specified by design §5.5:
 *
 *   1. NYSE trading-day gate   — skip cleanly on weekends / holidays.
 *   2. Brief generation        — Gemini Enterprise (Phase C, once the
 *                                allowlist clears) or Vertex fallback
 *                                (Phase B) via `fallback.mjs`.
 *   3. Section 5 strip         — remove Jun-only picks via
 *                                `strip.mjs::stripSection5()`.
 *   4. Fan-out (best-effort)   — write the stripped brief to
 *                                BigQuery, the raw envelope to GCS,
 *                                and the stripped markdown to Box.
 *                                Failures in one path do NOT block
 *                                the others (design §5.5 "partial").
 *
 * **Status**: SKELETON. The real Gemini Enterprise Deep Research
 * Agent call is not invoked here; the code path currently uses the
 * Vertex fallback from `fallback.mjs`. When the allowlist clears and
 * `deep-research.mjs` lands, swap the `generateBrief()` dispatch.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §3.1, §5.5, §7.3.
 */

import { etDateString, isTradingDay } from './nyse.mjs';
import { generateDeepBrief } from './fallback.mjs';
import { stripSection5 } from './strip.mjs';
import {
  SOURCE_AGENT,
  STATUS,
  buildDeepResearchRow,
  writeMarketResearch,
} from './bigquery.mjs';
import { uploadRawEnvelope } from './gcs.mjs';
import { uploadBrief } from './box.mjs';

/**
 * Known run modes. Selected via `MAGI_BRIEF_MODE` env var; defaults
 * to `fallback` until Phase C flips the default once the allowlist
 * clears.
 */
export const MODE = Object.freeze({
  FALLBACK: 'fallback',
  DEEP_RESEARCH: 'deep-research',
});

/**
 * Run the morning Deep Research job. Returns a structured result
 * that is easy to log and to inspect in tests. Never throws for
 * business-logic failures (holiday, fan-out partial, etc.); only
 * throws for misconfiguration before the first SDK call.
 *
 * Dependency injection is explicit and narrow: the caller passes in
 * the exact functions to use for each stage. This keeps the
 * skeleton testable without standing up real GCP clients and makes
 * it trivial to swap `generateFallback` for a real
 * `generateDeepResearch` in Phase C.
 *
 * @param {Object} [opts]
 * @param {Date} [opts.now=new Date()]                     - Run instant.
 * @param {'fallback'|'deep-research'} [opts.mode]         - Run mode.
 * @param {string} [opts.promptVersion='v2.0']             - Prompt revision tag stored alongside the row.
 * @param {Object} [opts.deps]                             - DI seams for side-effectful stages.
 * @param {(date: Date) => boolean} [opts.deps.isTradingDay]
 * @param {(date: Date) => string} [opts.deps.etDateString]
 * @param {(args: {dateIso: string}) => Promise<string>} [opts.deps.generateFallback]
 * @param {(args: {dateIso: string}) => Promise<string>} [opts.deps.generateDeepResearch]
 * @param {(markdown: string) => {stripped: string, status: string, tickersRemaining?: number, tickerSamples?: string[]}} [opts.deps.stripSection5]
 * @param {(row: import('./bigquery.mjs').DeepResearchRow, opts?: object) => Promise<boolean>} [opts.deps.writeMarketResearch]
 * @param {(args: object) => Promise<{ok: boolean, gcsUri: string}>} [opts.deps.uploadRawEnvelope]
 * @param {(args: object) => Promise<{ok: boolean, fileId: string|null, boxUrl: string|null}>} [opts.deps.uploadBrief]
 * @returns {Promise<RunResult>}
 */
export async function runJob({
  now = new Date(),
  mode = process.env.MAGI_BRIEF_MODE ?? MODE.FALLBACK,
  promptVersion = process.env.MAGI_PROMPT_VERSION ?? 'v2.0',
  deps = {},
} = {}) {
  const {
    isTradingDay: isTradingDayFn = isTradingDay,
    etDateString: etDateStringFn = etDateString,
    generateFallback = (args) => generateDeepBrief(args),
    generateDeepResearch,
    stripSection5: stripSection5Fn = stripSection5,
    writeMarketResearch: writeMarketResearchFn = writeMarketResearch,
    uploadRawEnvelope: uploadRawEnvelopeFn = uploadRawEnvelope,
    uploadBrief: uploadBriefFn = uploadBrief,
  } = deps;

  const date = etDateStringFn(now);
  const startedAt = Date.now();

  // §1. NYSE trading-day gate. Weekends and NYSE holidays exit
  // cleanly — the Job returns `skipped_holiday` and writes nothing
  // downstream. Cloud Run Job treats this as success (exit 0).
  if (!isTradingDayFn(now)) {
    console.log(
      `[deep-research-job] ${date} is not an NYSE trading day — skipping cleanly.`,
    );
    return {
      ok: true,
      status: STATUS.SKIPPED_HOLIDAY,
      date,
      mode,
      reason: 'not_a_trading_day',
      durationSec: Math.round((Date.now() - startedAt) / 1000),
      bigquery: null,
      gcs: null,
      box: null,
    };
  }

  // §2. Brief generation. Phase B runs fallback; Phase C swaps in
  // the Deep Research Agent caller. Either way, the output is a
  // single markdown string that MUST contain all five H2 sections
  // (enforced structurally by `test/prompt-contract.test.mjs`).
  let rawMarkdown;
  let sourceAgent;
  try {
    if (mode === MODE.DEEP_RESEARCH) {
      if (typeof generateDeepResearch !== 'function') {
        throw new Error(
          'index.mjs: mode=deep-research requires deps.generateDeepResearch — not yet wired (allowlist pending).',
        );
      }
      rawMarkdown = await generateDeepResearch({ dateIso: date });
      sourceAgent = SOURCE_AGENT.GEMINI_ENTERPRISE;
    } else if (mode === MODE.FALLBACK) {
      rawMarkdown = await generateFallback({ dateIso: date });
      sourceAgent = SOURCE_AGENT.FALLBACK;
    } else {
      throw new Error(
        `index.mjs: unknown mode ${JSON.stringify(mode)} — expected 'fallback' or 'deep-research'`,
      );
    }
  } catch (err) {
    console.error(
      `[deep-research-job] brief generation failed:`,
      err instanceof Error ? err.message : err,
    );
    return {
      ok: false,
      status: STATUS.FAILED,
      date,
      mode,
      reason: 'brief_generation_failed',
      error: err instanceof Error ? err.message : String(err),
      durationSec: Math.round((Date.now() - startedAt) / 1000),
      bigquery: null,
      gcs: null,
      box: null,
    };
  }

  // §3. Section 5 strip. Absolute boundary (§2.3): Section 5 never
  // flows into BigQuery or Box. The raw envelope going to GCS is
  // deliberately pre-strip (Jun-only bucket).
  const { stripped, status: stripStatus, tickersRemaining } = stripSection5Fn(rawMarkdown);
  const overallStatus =
    stripStatus === 'partial' ? STATUS.PARTIAL : STATUS.SUCCESS;

  // §4. Fan-out. Each writer is isolated in its own try/catch so an
  // unexpected throw in one leg does NOT skip the others — this is
  // the design §5.5 "partial" guarantee. Each module already returns
  // `{ ok: false }` on transport errors; these try/catch blocks are
  // defensive coverage for the narrower case where an injected or
  // future implementation throws unexpectedly. GCS and Box are
  // independent so we launch them in parallel, then await both
  // before composing the BigQuery row (which needs their URIs).
  const gcsResult = await settleFanOut('gcs', () =>
    uploadRawEnvelopeFn({
      date,
      envelope: {
        date,
        mode,
        sourceAgent,
        raw: rawMarkdown,
        promptVersion,
      },
      status: overallStatus,
    }),
    { ok: false, gcsUri: null, objectName: null },
  );
  const boxResult = await settleFanOut('box', () =>
    uploadBriefFn({
      date,
      markdown: stripped,
      status: overallStatus,
      opts: {},
    }),
    { ok: false, fileId: null, fileName: null, boxUrl: null },
  );

  const row = buildDeepResearchRow({
    date,
    strippedSummary: stripped,
    status: overallStatus,
    sourceAgent,
    promptVersion,
    gcsUri: gcsResult?.gcsUri ?? null,
    boxFileId: boxResult?.fileId ?? null,
    boxUrl: boxResult?.boxUrl ?? null,
    executionDurationSec: Math.round((Date.now() - startedAt) / 1000),
  });
  const bqOk = await settleFanOut('bigquery', () => writeMarketResearchFn(row), false);

  const ok = bqOk && (gcsResult?.ok ?? false) && (boxResult?.ok ?? false);
  return {
    ok,
    status: overallStatus,
    date,
    mode,
    sourceAgent,
    tickersRemaining: tickersRemaining ?? 0,
    durationSec: row.execution_duration_sec,
    bigquery: { ok: bqOk },
    gcs: gcsResult,
    box: boxResult,
  };
}

/**
 * Fan-out leg runner: invokes `fn` and converts any thrown exception
 * into `fallback`. The individual writers already return
 * `{ ok: false }` on transport errors, so this is only hit when a
 * writer (or its DI replacement) throws unexpectedly.
 *
 * @template T
 * @param {string} label
 * @param {() => Promise<T>} fn
 * @param {T} fallback
 * @returns {Promise<T>}
 */
async function settleFanOut(label, fn, fallback) {
  try {
    return await fn();
  } catch (err) {
    console.error(
      `[deep-research-job] ${label} leg threw unexpectedly:`,
      err instanceof Error ? err.message : err,
    );
    return fallback;
  }
}

/**
 * Minimal CLI entry — allows `node src/index.mjs` to run the job
 * once. Cloud Run Jobs invokes `node src/index.mjs` directly so
 * this branch is the production path.
 *
 * Intentionally tiny: no flag parsing, no config loading. All
 * behavior is controlled via env vars so the Job deployment stays
 * declarative.
 */
/* istanbul ignore next -- entrypoint only */
if (import.meta.url === `file://${process.argv[1]}`) {
  runJob()
    .then((result) => {
      console.log('[deep-research-job] result:', JSON.stringify(result));
      // Exit 0 on trading-day-skip or success; non-zero on hard
      // failures so Cloud Run Job retry / alerts fire.
      process.exit(result.ok || result.status === STATUS.SKIPPED_HOLIDAY ? 0 : 1);
    })
    .catch((err) => {
      console.error('[deep-research-job] unhandled error:', err);
      process.exit(2);
    });
}

/**
 * @typedef {Object} RunResult
 * @property {boolean} ok
 * @property {string} status
 * @property {string} date
 * @property {string} mode
 * @property {string} [sourceAgent]
 * @property {string} [reason]
 * @property {string} [error]
 * @property {number} [tickersRemaining]
 * @property {number} durationSec
 * @property {{ok: boolean} | null} bigquery
 * @property {{ok: boolean, gcsUri: string} | null} gcs
 * @property {{ok: boolean, fileId: string|null, boxUrl: string|null} | null} box
 */
