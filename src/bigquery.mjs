/**
 * @file BigQuery writer for `screen-share-459802.magi_core.market_research`.
 *
 * This module is the only path through which Phase C's Cloud Run Job
 * persists a `DAILY_DEEP_RESEARCH` row. Existing `MACRO` and `SYMBOL`
 * rows continue to be written by `magi-core/sentiment-monitor.js` via
 * `lib/bigquery.js::safeInsert`; the Deep Research writer is split
 * out into its own module so the Section 5 safety invariant (design
 * §2.3) is localised in one auditable place.
 *
 * Schema contract:
 *   The table was extended by the migration in `magi-core` PR #33
 *   (`sql/alter_market_research_v2.sql`). In addition to the existing
 *   sentiment-monitor columns (`date`, `research_type`, `symbol`,
 *   `summary`, `sentiment`, `risk_level`, `key_events`, `raw_data`,
 *   `created_at`), Deep Research rows populate:
 *     - `source_agent`            STRING
 *     - `box_file_id`             STRING NULL
 *     - `box_url`                 STRING NULL
 *     - `gcs_uri`                 STRING NULL
 *     - `word_count`              INT64  NULL
 *     - `session_id`              STRING NULL
 *     - `execution_duration_sec`  INT64  NULL
 *     - `search_query_count`      INT64  NULL
 *     - `estimated_cost_usd`      FLOAT64 NULL
 *     - `prompt_version`          STRING NULL
 *     - `status`                  STRING  ({success|partial|failed|skipped_holiday})
 *     - `assessment_score`        INT64  NULL  (populated weekly by Jun)
 *
 * Safety invariant (design §2.3, §5.3):
 *   `row.summary` MUST have been through `stripSection5()`. This
 *   module will refuse to insert a row whose summary still contains
 *   `## 5. Jun Review Only` — surfacing the bug at the writer
 *   boundary, never at the PLM's `ask_market_context()` read.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §3.1, §5.3, §7.3.
 */

import { BigQuery } from '@google-cloud/bigquery';

export const DEFAULT_PROJECT =
  process.env.GOOGLE_CLOUD_PROJECT || 'screen-share-459802';
export const DEFAULT_DATASET = 'magi_core';
export const DEFAULT_TABLE = 'market_research';

/**
 * The `research_type` sentinel for Deep Research rows. Existing
 * sentiment-monitor rows use `'MACRO'` or `'SYMBOL'`, so
 * `ask_market_context()` in magi-core can filter by this value when
 * surfacing the brief to the PLM (see magi-core PR #35).
 */
export const RESEARCH_TYPE = 'DAILY_DEEP_RESEARCH';

/**
 * Accepted `status` values per the PR #33 schema option description.
 */
export const STATUS = Object.freeze({
  SUCCESS: 'success',
  PARTIAL: 'partial',
  FAILED: 'failed',
  SKIPPED_HOLIDAY: 'skipped_holiday',
});

const VALID_STATUS = new Set(Object.values(STATUS));

/**
 * Writer identities written to `source_agent`. The allowlist is
 * deliberately narrow — anything outside this set indicates a new
 * writer that should have its own code review / audit trail.
 */
export const SOURCE_AGENT = Object.freeze({
  GEMINI_ENTERPRISE: 'gemini_enterprise_deep_research',
  FALLBACK: 'sentiment_monitor_fallback',
});

const VALID_SOURCE_AGENT = new Set(Object.values(SOURCE_AGENT));

/**
 * Defense-in-depth check: the summary must not contain Section 5.
 *
 * @param {unknown} summary
 * @throws {TypeError | Error}
 */
function assertStripped(summary) {
  if (typeof summary !== 'string') {
    throw new TypeError(
      'bigquery.mjs: row.summary must be a string (stripSection5 output)',
    );
  }
  if (/^## 5\. Jun Review Only\b/m.test(summary)) {
    throw new Error(
      'bigquery.mjs: row.summary still contains "## 5. Jun Review Only". ' +
        'Call stripSection5() before writeMarketResearch(). ' +
        'This is a MAGI-GE-DESIGN-001-v2 §2.3 absolute-boundary violation.',
    );
  }
}

/**
 * Assert that all required fields of a Deep Research row are present.
 *
 * @param {Partial<DeepResearchRow>} row
 */
function assertRowShape(row) {
  if (!row || typeof row !== 'object') {
    throw new TypeError('bigquery.mjs: row must be an object');
  }
  if (typeof row.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(row.date)) {
    throw new TypeError(
      'bigquery.mjs: row.date must be a YYYY-MM-DD string (ET calendar date)',
    );
  }
  if (row.research_type !== RESEARCH_TYPE) {
    throw new Error(
      `bigquery.mjs: row.research_type must be '${RESEARCH_TYPE}' (got ${JSON.stringify(row.research_type)})`,
    );
  }
  if (row.symbol !== null && typeof row.symbol !== 'string') {
    throw new TypeError(
      'bigquery.mjs: row.symbol must be null or a string (null for DAILY_DEEP_RESEARCH rows)',
    );
  }
  assertStripped(row.summary);
  if (!VALID_STATUS.has(row.status)) {
    throw new Error(
      `bigquery.mjs: row.status must be one of ${[...VALID_STATUS].join(', ')} (got ${JSON.stringify(row.status)})`,
    );
  }
  if (!VALID_SOURCE_AGENT.has(row.source_agent)) {
    throw new Error(
      `bigquery.mjs: row.source_agent must be one of ${[...VALID_SOURCE_AGENT].join(', ')} (got ${JSON.stringify(row.source_agent)})`,
    );
  }
}

/**
 * Build a ready-to-insert `market_research` row from the outputs of
 * `stripSection5()` and runtime metadata.
 *
 * @param {Object} args
 * @param {string} args.date                 - ET calendar date, YYYY-MM-DD.
 * @param {string} args.strippedSummary      - `stripSection5()` `.stripped`.
 * @param {'success'|'partial'|'failed'|'skipped_holiday'} args.status
 * @param {string} args.sourceAgent          - `SOURCE_AGENT.*`.
 * @param {string} [args.sentiment]          - BULLISH / BEARISH / NEUTRAL.
 * @param {string} [args.riskLevel]          - LOW / MEDIUM / HIGH.
 * @param {unknown[]} [args.keyEvents]       - JSON-serialisable array.
 * @param {unknown} [args.rawData]           - JSON-serialisable envelope.
 * @param {string} [args.boxFileId]
 * @param {string} [args.boxUrl]
 * @param {string} [args.gcsUri]
 * @param {string} [args.sessionId]
 * @param {number} [args.executionDurationSec]
 * @param {number} [args.searchQueryCount]
 * @param {number} [args.estimatedCostUsd]
 * @param {string} [args.promptVersion]
 * @param {Date}   [args.createdAt]          - Defaults to `new Date()`.
 * @returns {DeepResearchRow}
 */
export function buildDeepResearchRow({
  date,
  strippedSummary,
  status,
  sourceAgent,
  sentiment,
  riskLevel,
  keyEvents,
  rawData,
  boxFileId,
  boxUrl,
  gcsUri,
  sessionId,
  executionDurationSec,
  searchQueryCount,
  estimatedCostUsd,
  promptVersion,
  createdAt,
} = {}) {
  assertStripped(strippedSummary);

  /** @type {DeepResearchRow} */
  const row = {
    date,
    research_type: RESEARCH_TYPE,
    symbol: null,
    summary: strippedSummary,
    sentiment: sentiment ?? null,
    risk_level: riskLevel ?? null,
    key_events: keyEvents !== undefined ? JSON.stringify(keyEvents) : null,
    raw_data: rawData !== undefined ? JSON.stringify(rawData) : null,
    created_at: (createdAt ?? new Date()).toISOString(),
    source_agent: sourceAgent,
    box_file_id: boxFileId ?? null,
    box_url: boxUrl ?? null,
    gcs_uri: gcsUri ?? null,
    word_count: countWords(strippedSummary),
    session_id: sessionId ?? null,
    execution_duration_sec: executionDurationSec ?? null,
    search_query_count: searchQueryCount ?? null,
    estimated_cost_usd: estimatedCostUsd ?? null,
    prompt_version: promptVersion ?? null,
    status,
    assessment_score: null,
  };

  assertRowShape(row);
  return row;
}

/**
 * Word count for the stripped summary. Used to populate the
 * `word_count` column (design §7.3). Treats whitespace-separated
 * tokens as words — good enough for the quality-monitoring dashboard,
 * not a natural-language parser.
 *
 * @param {string} s
 * @returns {number}
 */
function countWords(s) {
  const trimmed = s.trim();
  if (trimmed === '') return 0;
  return trimmed.split(/\s+/).length;
}

/**
 * Default sleep utility for retry backoff. Exposed as a seam so
 * tests can inject `() => {}` for instant retry loops.
 *
 * @param {number} ms
 * @returns {Promise<void>}
 */
function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Lazy default `@google-cloud/bigquery` client factory. Constructing
 * the client at import time would require ADC even for unit tests
 * that never touch the network — same pattern as `fallback.mjs`.
 *
 * @param {Object} [opts]
 * @param {string} [opts.projectId]
 * @returns {BigQuery}
 */
export function createDefaultClient({ projectId = DEFAULT_PROJECT } = {}) {
  return new BigQuery({ projectId });
}

/**
 * Insert one Deep Research row into `magi_core.market_research` with
 * retry + exponential backoff. Returns `true` on success, `false`
 * after `maxRetries` failed attempts.
 *
 * @param {DeepResearchRow} row
 * @param {Object} [opts]
 * @param {BigQuery} [opts.bq]          - Injectable `@google-cloud/bigquery` client.
 * @param {string} [opts.dataset]       - Overrides `DEFAULT_DATASET`.
 * @param {string} [opts.table]         - Overrides `DEFAULT_TABLE`.
 * @param {number} [opts.maxRetries=3]
 * @param {number} [opts.backoffMs=1000] - Base backoff (ms) — multiplied by attempt number.
 * @param {(ms: number) => Promise<void>} [opts.sleep=defaultSleep] - Injectable for tests.
 * @returns {Promise<boolean>}
 */
export async function writeMarketResearch(
  row,
  {
    bq,
    dataset = DEFAULT_DATASET,
    table = DEFAULT_TABLE,
    maxRetries = 3,
    backoffMs = 1000,
    sleep = defaultSleep,
  } = {},
) {
  assertRowShape(row);

  const client = bq ?? createDefaultClient();
  const target = client.dataset(dataset).table(table);
  const label = `${dataset}.${table}`;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const [response] = await target.insert([row]);
      if (response && response.insertErrors) {
        console.error(
          `[deep-research-bq] attempt ${attempt}/${maxRetries} insertErrors:`,
          JSON.stringify(response.insertErrors),
        );
      } else {
        console.log(
          `[deep-research-bq] inserted 1 row into ${label} (attempt ${attempt}/${maxRetries}, status=${row.status})`,
        );
        return true;
      }
    } catch (err) {
      // @google-cloud/bigquery throws PartialFailureError with .errors;
      // fall through to plain stringify for other exceptions.
      const payload =
        err && typeof err === 'object' && 'errors' in err
          ? /** @type {{errors: unknown}} */ (err).errors
          : err;
      console.error(
        `[deep-research-bq] attempt ${attempt}/${maxRetries} threw:`,
        JSON.stringify(payload, replacerSafe),
      );
    }
    if (attempt < maxRetries) {
      await sleep(backoffMs * attempt);
    }
  }
  console.error(
    `[deep-research-bq] FAILED after ${maxRetries} attempts (${label}, status=${row.status})`,
  );
  return false;
}

/**
 * `JSON.stringify` replacer that tolerates `Error` instances and
 * circular references — otherwise a BigQuery SDK error with self-
 * referential `request` objects would itself throw when logged.
 *
 * @param {string} _key
 * @param {unknown} value
 */
function replacerSafe(_key, value) {
  if (value instanceof Error) {
    return { name: value.name, message: value.message, stack: value.stack };
  }
  return value;
}

/**
 * @typedef {Object} DeepResearchRow
 * @property {string} date
 * @property {typeof RESEARCH_TYPE} research_type
 * @property {null} symbol
 * @property {string} summary
 * @property {string | null} sentiment
 * @property {string | null} risk_level
 * @property {string | null} key_events
 * @property {string | null} raw_data
 * @property {string} created_at
 * @property {string} source_agent
 * @property {string | null} box_file_id
 * @property {string | null} box_url
 * @property {string | null} gcs_uri
 * @property {number | null} word_count
 * @property {string | null} session_id
 * @property {number | null} execution_duration_sec
 * @property {number | null} search_query_count
 * @property {number | null} estimated_cost_usd
 * @property {string | null} prompt_version
 * @property {string} status
 * @property {number | null} assessment_score
 */
