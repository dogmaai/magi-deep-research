import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_DATASET,
  DEFAULT_TABLE,
  RESEARCH_TYPE,
  SOURCE_AGENT,
  STATUS,
  buildDeepResearchRow,
  writeMarketResearch,
} from '../src/bigquery.mjs';
import { stripSection5 } from '../src/strip.mjs';

/**
 * Shared valid baseline for tests that need a well-formed row.
 */
const VALID = Object.freeze({
  date: '2026-04-20',
  strippedSummary:
    '## 1. Macro\nVIX 17.\n## 2. Sector\nTech led.\n## 3. Risks\nCPI print.\n## 4. Watchlist\n- NVDA earnings.\n',
  status: STATUS.SUCCESS,
  sourceAgent: SOURCE_AGENT.FALLBACK,
});

/**
 * Build a `@google-cloud/bigquery`-shaped client mock that captures
 * each insert call and controls its outcome.
 *
 * @param {Object} [opts]
 * @param {Array<{type: 'ok'|'insertErrors'|'throw', payload?: unknown}>} [opts.outcomes]
 *   One entry per expected insert attempt. Defaults to a single 'ok'.
 */
function makeBqMock({ outcomes = [{ type: 'ok' }] } = {}) {
  const captured = { calls: [], datasetArg: null, tableArg: null };
  let attempt = 0;
  const client = {
    dataset(name) {
      captured.datasetArg = name;
      return {
        table(tableName) {
          captured.tableArg = tableName;
          return {
            async insert(rows) {
              captured.calls.push(rows);
              const outcome = outcomes[attempt] ?? outcomes[outcomes.length - 1];
              attempt += 1;
              if (outcome.type === 'ok') return [{}];
              if (outcome.type === 'insertErrors') {
                return [{ insertErrors: outcome.payload ?? [{ row: rows[0], errors: [{ reason: 'stubbed' }] }] }];
              }
              if (outcome.type === 'throw') {
                throw outcome.payload ?? new Error('stub bq exception');
              }
              throw new Error(`unknown outcome ${outcome.type}`);
            },
          };
        },
      };
    },
  };
  return { client, captured };
}

describe('buildDeepResearchRow', () => {
  it('constructs a valid row with sensible defaults for optional fields', () => {
    const row = buildDeepResearchRow(VALID);
    assert.equal(row.date, '2026-04-20');
    assert.equal(row.research_type, RESEARCH_TYPE);
    assert.equal(row.symbol, null);
    assert.equal(row.status, STATUS.SUCCESS);
    assert.equal(row.source_agent, SOURCE_AGENT.FALLBACK);
    assert.equal(row.sentiment, null);
    assert.equal(row.risk_level, null);
    assert.equal(row.key_events, null);
    assert.equal(row.raw_data, null);
    assert.equal(row.box_file_id, null);
    assert.equal(row.box_url, null);
    assert.equal(row.gcs_uri, null);
    assert.equal(row.session_id, null);
    assert.equal(row.execution_duration_sec, null);
    assert.equal(row.search_query_count, null);
    assert.equal(row.estimated_cost_usd, null);
    assert.equal(row.prompt_version, null);
    assert.equal(row.assessment_score, null);
    assert.match(row.created_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
  });

  it('serialises keyEvents and rawData as JSON strings', () => {
    const row = buildDeepResearchRow({
      ...VALID,
      keyEvents: ['CPI 08:30 ET', 'FOMC minutes 14:00 ET'],
      rawData: { foo: 'bar', nested: { n: 1 } },
    });
    assert.deepEqual(JSON.parse(row.key_events), [
      'CPI 08:30 ET',
      'FOMC minutes 14:00 ET',
    ]);
    assert.deepEqual(JSON.parse(row.raw_data), { foo: 'bar', nested: { n: 1 } });
  });

  it('computes word_count from the stripped summary', () => {
    const row = buildDeepResearchRow({
      ...VALID,
      strippedSummary: 'one two three four five',
    });
    assert.equal(row.word_count, 5);
  });

  it('returns word_count=0 for whitespace-only summary', () => {
    // Whitespace-only summaries are permitted by the row-shape assert
    // (they don't violate the Section 5 guard). word_count should
    // still be a well-defined 0.
    const row = buildDeepResearchRow({ ...VALID, strippedSummary: '   \n\n\t' });
    assert.equal(row.word_count, 0);
  });

  it('preserves all Deep Research metadata fields when provided', () => {
    const createdAt = new Date(Date.UTC(2026, 3, 20, 13, 5, 0));
    const row = buildDeepResearchRow({
      ...VALID,
      sentiment: 'BULLISH',
      riskLevel: 'MEDIUM',
      boxFileId: 'box-12345',
      boxUrl: 'https://app.box.com/file/12345',
      gcsUri: 'gs://magi-deep-research-raw/2026-04-20.json',
      sessionId: 'session-abc',
      executionDurationSec: 87,
      searchQueryCount: 14,
      estimatedCostUsd: 0.42,
      promptVersion: 'v2.0',
      createdAt,
    });
    assert.equal(row.sentiment, 'BULLISH');
    assert.equal(row.risk_level, 'MEDIUM');
    assert.equal(row.box_file_id, 'box-12345');
    assert.equal(row.box_url, 'https://app.box.com/file/12345');
    assert.equal(row.gcs_uri, 'gs://magi-deep-research-raw/2026-04-20.json');
    assert.equal(row.session_id, 'session-abc');
    assert.equal(row.execution_duration_sec, 87);
    assert.equal(row.search_query_count, 14);
    assert.equal(row.estimated_cost_usd, 0.42);
    assert.equal(row.prompt_version, 'v2.0');
    assert.equal(row.created_at, '2026-04-20T13:05:00.000Z');
  });

  it('throws when summary still contains "## 5. Jun Review Only" (§2.3 guard)', () => {
    const raw =
      '## 1. Macro\nA\n## 5. Jun Review Only\n$AAPL entry 150 stop 148 target 155\n';
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, strippedSummary: raw }),
      /absolute-boundary/,
    );
  });

  it('throws TypeError when strippedSummary is not a string', () => {
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, strippedSummary: null }),
      TypeError,
    );
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, strippedSummary: 42 }),
      TypeError,
    );
  });

  it('throws on invalid status', () => {
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, status: 'WEIRD' }),
      /row\.status must be one of/,
    );
  });

  it('throws on invalid source_agent', () => {
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, sourceAgent: 'bespoke_writer' }),
      /row\.source_agent must be one of/,
    );
  });

  it('throws on malformed date', () => {
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, date: '2026/04/20' }),
      TypeError,
    );
    assert.throws(
      () => buildDeepResearchRow({ ...VALID, date: undefined }),
      TypeError,
    );
  });
});

describe('writeMarketResearch', () => {
  it('inserts into magi_core.market_research by default and returns true', async () => {
    const row = buildDeepResearchRow(VALID);
    const { client, captured } = makeBqMock({ outcomes: [{ type: 'ok' }] });
    const ok = await writeMarketResearch(row, { bq: client });
    assert.equal(ok, true);
    assert.equal(captured.datasetArg, DEFAULT_DATASET);
    assert.equal(captured.tableArg, DEFAULT_TABLE);
    assert.equal(captured.calls.length, 1);
    assert.deepEqual(captured.calls[0], [row]);
  });

  it('respects dataset / table overrides', async () => {
    const row = buildDeepResearchRow(VALID);
    const { client, captured } = makeBqMock();
    await writeMarketResearch(row, {
      bq: client,
      dataset: 'magi_core_test',
      table: 'market_research_shadow',
    });
    assert.equal(captured.datasetArg, 'magi_core_test');
    assert.equal(captured.tableArg, 'market_research_shadow');
  });

  it('retries up to maxRetries=3 on insertErrors, then returns false', async () => {
    const row = buildDeepResearchRow(VALID);
    const { client, captured } = makeBqMock({
      outcomes: [
        { type: 'insertErrors' },
        { type: 'insertErrors' },
        { type: 'insertErrors' },
      ],
    });
    const sleepCalls = [];
    const ok = await writeMarketResearch(row, {
      bq: client,
      backoffMs: 10,
      sleep: (ms) => {
        sleepCalls.push(ms);
        return Promise.resolve();
      },
    });
    assert.equal(ok, false);
    assert.equal(captured.calls.length, 3);
    // Exponential-ish: 10 * 1 between attempts 1→2, 10 * 2 between 2→3,
    // and NO sleep after the final failed attempt.
    assert.deepEqual(sleepCalls, [10, 20]);
  });

  it('returns true on the 2nd attempt when the 1st throws', async () => {
    const row = buildDeepResearchRow(VALID);
    const { client, captured } = makeBqMock({
      outcomes: [
        { type: 'throw', payload: new Error('transient 503') },
        { type: 'ok' },
      ],
    });
    const ok = await writeMarketResearch(row, {
      bq: client,
      backoffMs: 0,
      sleep: () => Promise.resolve(),
    });
    assert.equal(ok, true);
    assert.equal(captured.calls.length, 2);
  });

  it('returns false after repeated thrown exceptions', async () => {
    const row = buildDeepResearchRow(VALID);
    const { client } = makeBqMock({
      outcomes: Array(3).fill({ type: 'throw', payload: new Error('boom') }),
    });
    const ok = await writeMarketResearch(row, {
      bq: client,
      backoffMs: 0,
      sleep: () => Promise.resolve(),
    });
    assert.equal(ok, false);
  });

  it('refuses to insert a row whose summary contains Section 5', async () => {
    const badRow = {
      ...buildDeepResearchRow(VALID),
      summary:
        '## 1. Macro\nA\n## 5. Jun Review Only\n$AAPL 150/148/155\n',
    };
    const { client, captured } = makeBqMock();
    await assert.rejects(
      () => writeMarketResearch(badRow, { bq: client }),
      /absolute-boundary/,
    );
    assert.equal(captured.calls.length, 0);
  });

  it('accepts a PartialFailureError whose .errors is non-enumerable-ish', async () => {
    const err = Object.assign(new Error('partial failure'), {
      errors: [{ reason: 'invalid' }],
    });
    const row = buildDeepResearchRow(VALID);
    const { client } = makeBqMock({
      outcomes: [
        { type: 'throw', payload: err },
        { type: 'ok' },
      ],
    });
    const ok = await writeMarketResearch(row, {
      bq: client,
      backoffMs: 0,
      sleep: () => Promise.resolve(),
    });
    assert.equal(ok, true);
  });
});

describe('end-to-end contract: strip → build → write', () => {
  it('round-trips a realistic brief through stripSection5 and buildDeepResearchRow', async () => {
    const raw = [
      '## 1. Macro',
      'VIX 17, LOW_FEAR.',
      '## 2. Sector',
      'Tech led.',
      '## 3. Risks',
      'CPI print.',
      '## 4. Watchlist',
      '- NVDA earnings.',
      '## 5. Jun Review Only',
      '$AAPL LONG 150 stop 148 target 155',
    ].join('\n');
    const { stripped, status } = stripSection5(raw);
    const row = buildDeepResearchRow({
      date: '2026-04-20',
      strippedSummary: stripped,
      status,
      sourceAgent: SOURCE_AGENT.FALLBACK,
      sentiment: 'BULLISH',
      riskLevel: 'MEDIUM',
      promptVersion: 'v2.0',
    });
    assert.doesNotMatch(row.summary, /Jun Review Only/);
    assert.doesNotMatch(row.summary, /\$AAPL/);

    const { client, captured } = makeBqMock();
    const ok = await writeMarketResearch(row, { bq: client });
    assert.equal(ok, true);
    assert.equal(captured.calls[0][0].research_type, RESEARCH_TYPE);
    assert.equal(captured.calls[0][0].status, 'success');
  });
});
