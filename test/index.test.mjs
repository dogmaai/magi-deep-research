import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MODE, runJob } from '../src/index.mjs';
import { SOURCE_AGENT, STATUS } from '../src/bigquery.mjs';

/**
 * A full set of injectable deps that never touch the network. Each
 * test can override individual fns to simulate failure modes.
 */
function makeDeps(overrides = {}) {
  const captured = {
    bqRows: [],
    gcsCalls: [],
    boxCalls: [],
  };
  const deps = {
    isTradingDay: () => true,
    etDateString: () => '2026-04-20',
    generateFallback: async () =>
      [
        '## 1. Macro',
        'VIX 17',
        '## 2. Sector',
        'Tech led',
        '## 3. Risks',
        'CPI print',
        '## 4. Watchlist',
        '- NVDA',
        '## 5. Jun Review Only',
        '$AAPL 150/148/155',
      ].join('\n'),
    stripSection5: (md) => ({
      stripped: md.replace(/## 5\. Jun Review Only[\s\S]*$/m, '').trim() + '\n',
      status: 'success',
      tickerLeakCount: 0,
    }),
    writeMarketResearch: async (row) => {
      captured.bqRows.push(row);
      return true;
    },
    uploadRawEnvelope: async (args) => {
      captured.gcsCalls.push(args);
      return {
        ok: true,
        gcsUri: `gs://magi-deep-research-raw/raw/${args.date}.json`,
        objectName: `raw/${args.date}.json`,
      };
    },
    uploadBrief: async (args) => {
      captured.boxCalls.push(args);
      return {
        ok: true,
        fileId: 'box-file-1',
        fileName: `${args.date}_deep_research_brief.md`,
        boxUrl: 'https://app.box.com/file/box-file-1',
      };
    },
    ...overrides,
  };
  return { deps, captured };
}

describe('runJob', () => {
  it('exits cleanly on non-trading days with status=skipped_holiday', async () => {
    const { deps, captured } = makeDeps({ isTradingDay: () => false });
    const result = await runJob({ deps });
    assert.equal(result.ok, true);
    assert.equal(result.status, STATUS.SKIPPED_HOLIDAY);
    assert.equal(result.reason, 'not_a_trading_day');
    assert.equal(result.bigquery, null);
    assert.equal(result.gcs, null);
    assert.equal(result.box, null);
    assert.equal(captured.bqRows.length, 0);
    assert.equal(captured.gcsCalls.length, 0);
    assert.equal(captured.boxCalls.length, 0);
  });

  it('runs the full fan-out on trading days in fallback mode (happy path)', async () => {
    const { deps, captured } = makeDeps();
    const result = await runJob({ deps, mode: MODE.FALLBACK });
    assert.equal(result.ok, true);
    assert.equal(result.status, STATUS.SUCCESS);
    assert.equal(result.sourceAgent, SOURCE_AGENT.FALLBACK);
    assert.equal(result.date, '2026-04-20');
    assert.equal(captured.bqRows.length, 1);
    assert.equal(captured.gcsCalls.length, 1);
    assert.equal(captured.boxCalls.length, 1);
    // Row is post-strip.
    assert.doesNotMatch(captured.bqRows[0].summary, /Jun Review Only/);
    assert.doesNotMatch(captured.bqRows[0].summary, /\$AAPL/);
    // Row carries the downstream URIs.
    assert.equal(
      captured.bqRows[0].gcs_uri,
      'gs://magi-deep-research-raw/raw/2026-04-20.json',
    );
    assert.equal(captured.bqRows[0].box_file_id, 'box-file-1');
    assert.equal(
      captured.bqRows[0].box_url,
      'https://app.box.com/file/box-file-1',
    );
  });

  it('sends the raw (pre-strip) markdown to GCS and the stripped markdown to Box', async () => {
    const { deps, captured } = makeDeps();
    await runJob({ deps });
    const gcsEnvelope = captured.gcsCalls[0].envelope;
    assert.match(gcsEnvelope.raw, /Jun Review Only/);
    assert.match(gcsEnvelope.raw, /\$AAPL/);

    const boxMarkdown = captured.boxCalls[0].markdown;
    assert.doesNotMatch(boxMarkdown, /Jun Review Only/);
    assert.doesNotMatch(boxMarkdown, /\$AAPL/);
  });

  it('falls back to status=partial when strip reports a ticker leak', async () => {
    const { deps, captured } = makeDeps({
      stripSection5: (md) => ({
        stripped: md.replace(/## 5\. Jun Review Only[\s\S]*$/m, '').trim() + '\n',
        status: 'partial',
        tickerLeakCount: 3,
      }),
    });
    const result = await runJob({ deps });
    assert.equal(result.status, STATUS.PARTIAL);
    assert.equal(result.tickerLeakCount, 3);
    assert.equal(captured.bqRows[0].status, STATUS.PARTIAL);
  });

  it('returns {ok:false, status:failed} when brief generation throws', async () => {
    const { deps, captured } = makeDeps({
      generateFallback: async () => {
        throw new Error('quota exceeded');
      },
    });
    const result = await runJob({ deps });
    assert.equal(result.ok, false);
    assert.equal(result.status, STATUS.FAILED);
    assert.equal(result.reason, 'brief_generation_failed');
    assert.match(result.error, /quota exceeded/);
    assert.equal(captured.bqRows.length, 0);
    assert.equal(captured.gcsCalls.length, 0);
    assert.equal(captured.boxCalls.length, 0);
  });

  it('returns ok=false when any fan-out leg fails but still writes the BQ row', async () => {
    const { deps, captured } = makeDeps({
      uploadBrief: async (args) => ({
        ok: false,
        fileId: null,
        fileName: `${args.date}_deep_research_brief.md`,
        boxUrl: null,
      }),
    });
    const result = await runJob({ deps });
    assert.equal(result.ok, false);
    assert.equal(result.status, STATUS.SUCCESS); // overall status set by strip
    assert.equal(result.box.ok, false);
    assert.equal(result.gcs.ok, true);
    assert.equal(result.bigquery.ok, true);
    assert.equal(captured.bqRows.length, 1);
    assert.equal(captured.bqRows[0].box_file_id, null);
  });

  it('throws on unknown mode', async () => {
    const { deps } = makeDeps();
    const result = await runJob({ deps, mode: 'bogus' });
    assert.equal(result.ok, false);
    assert.equal(result.reason, 'brief_generation_failed');
    assert.match(result.error, /unknown mode/);
  });

  it('fails deep-research mode with a clear error when generateDeepResearch is not wired', async () => {
    const { deps } = makeDeps();
    const result = await runJob({ deps, mode: MODE.DEEP_RESEARCH });
    assert.equal(result.ok, false);
    assert.equal(result.reason, 'brief_generation_failed');
    assert.match(result.error, /allowlist pending/);
  });

  it('uses generateDeepResearch when provided and marks sourceAgent=gemini_enterprise', async () => {
    const { deps, captured } = makeDeps({
      generateDeepResearch: async () =>
        [
          '## 1. Macro',
          'VIX 17',
          '## 2. Sector',
          'x',
          '## 3. Risks',
          'x',
          '## 4. Watchlist',
          'x',
          '## 5. Jun Review Only',
          '$MSFT',
        ].join('\n'),
    });
    const result = await runJob({ deps, mode: MODE.DEEP_RESEARCH });
    assert.equal(result.ok, true);
    assert.equal(result.sourceAgent, SOURCE_AGENT.GEMINI_ENTERPRISE);
    assert.equal(captured.bqRows[0].source_agent, SOURCE_AGENT.GEMINI_ENTERPRISE);
  });

  it('embeds promptVersion in the row', async () => {
    const { deps, captured } = makeDeps();
    await runJob({ deps, promptVersion: 'v2.1-experimental' });
    assert.equal(captured.bqRows[0].prompt_version, 'v2.1-experimental');
  });

  it('sets execution_duration_sec as a non-negative integer', async () => {
    const { deps, captured } = makeDeps();
    await runJob({ deps });
    const d = captured.bqRows[0].execution_duration_sec;
    assert.equal(typeof d, 'number');
    assert.ok(d >= 0);
    assert.ok(Number.isInteger(d));
  });
});
