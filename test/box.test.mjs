import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { fileNameFor, uploadBrief } from '../src/box.mjs';

/**
 * Build a BoxClient-shaped mock whose `files.uploadFile` behavior is
 * controlled by `outcomes` (one per expected attempt).
 *
 * @param {Object} [opts]
 * @param {Array<{type:'ok'|'throw'|'bad-shape', payload?: unknown}>} [opts.outcomes]
 */
function makeBoxMock({ outcomes = [{ type: 'ok' }] } = {}) {
  const captured = {
    calls: [],
  };
  let attempt = 0;
  const client = {
    files: {
      async uploadFile(folderId, name, body) {
        captured.calls.push({ folderId, name, body });
        const outcome = outcomes[attempt] ?? outcomes[outcomes.length - 1];
        attempt += 1;
        if (outcome.type === 'ok') {
          return (
            outcome.payload ?? {
              entries: [{ id: `file-${attempt}`, name }],
            }
          );
        }
        if (outcome.type === 'bad-shape') return outcome.payload ?? {};
        if (outcome.type === 'throw') {
          throw outcome.payload ?? new Error('stub box exception');
        }
        throw new Error(`unknown outcome ${outcome.type}`);
      },
    },
  };
  return { client, captured };
}

const STRIPPED =
  '## 1. Macro\nVIX 17.\n## 2. Sector\nTech led.\n## 3. Risks\nCPI.\n## 4. Watchlist\n- NVDA earnings.\n';

describe('fileNameFor', () => {
  it('composes <date>_deep_research_brief.md', () => {
    assert.equal(
      fileNameFor('2026-04-20'),
      '2026-04-20_deep_research_brief.md',
    );
  });

  it('rejects malformed dates', () => {
    assert.throws(() => fileNameFor('2026/04/20'), TypeError);
    assert.throws(() => fileNameFor(''), TypeError);
    assert.throws(() => fileNameFor(undefined), TypeError);
  });
});

describe('uploadBrief', () => {
  const BASE = {
    date: '2026-04-20',
    markdown: STRIPPED,
    status: 'success',
  };

  it('uploads the stripped markdown to the configured folder and returns {ok, fileId, boxUrl}', async () => {
    const { client, captured } = makeBoxMock({
      outcomes: [{ type: 'ok', payload: { entries: [{ id: '987654321' }] } }],
    });
    const result = await uploadBrief({
      ...BASE,
      opts: { box: client, folderId: '12345' },
    });
    assert.equal(result.ok, true);
    assert.equal(result.fileId, '987654321');
    assert.equal(result.fileName, '2026-04-20_deep_research_brief.md');
    assert.equal(result.boxUrl, 'https://app.box.com/file/987654321');
    assert.equal(captured.calls.length, 1);
    assert.equal(captured.calls[0].folderId, '12345');
    assert.equal(captured.calls[0].name, '2026-04-20_deep_research_brief.md');
    assert.equal(captured.calls[0].body.toString('utf8'), STRIPPED);
  });

  it('tolerates a response shape where the entry is returned directly (no `entries` array)', async () => {
    const { client } = makeBoxMock({
      outcomes: [{ type: 'ok', payload: { id: 'direct-id', name: 'x' } }],
    });
    const result = await uploadBrief({
      ...BASE,
      opts: { box: client, folderId: '12345' },
    });
    assert.equal(result.ok, true);
    assert.equal(result.fileId, 'direct-id');
  });

  it('retries up to maxRetries=3 on throws and returns ok=false', async () => {
    const { client, captured } = makeBoxMock({
      outcomes: Array(3).fill({ type: 'throw', payload: new Error('retryable 503') }),
    });
    const sleepCalls = [];
    const result = await uploadBrief({
      ...BASE,
      opts: {
        box: client,
        folderId: '12345',
        backoffMs: 10,
        sleep: (ms) => {
          sleepCalls.push(ms);
          return Promise.resolve();
        },
      },
    });
    assert.equal(result.ok, false);
    assert.equal(result.fileId, null);
    assert.equal(result.fileName, '2026-04-20_deep_research_brief.md');
    assert.equal(captured.calls.length, 3);
    assert.deepEqual(sleepCalls, [10, 20]);
  });

  it('returns ok=true when the 2nd attempt succeeds after a throw', async () => {
    const { client, captured } = makeBoxMock({
      outcomes: [
        { type: 'throw', payload: new Error('transient') },
        { type: 'ok' },
      ],
    });
    const result = await uploadBrief({
      ...BASE,
      opts: {
        box: client,
        folderId: '12345',
        backoffMs: 0,
        sleep: () => Promise.resolve(),
      },
    });
    assert.equal(result.ok, true);
    assert.equal(captured.calls.length, 2);
  });

  it('treats a malformed response (no id) as a retryable failure', async () => {
    const { client, captured } = makeBoxMock({
      outcomes: [
        { type: 'bad-shape', payload: { entries: [] } },
        { type: 'bad-shape', payload: {} },
        { type: 'ok', payload: { entries: [{ id: 'recovered' }] } },
      ],
    });
    const result = await uploadBrief({
      ...BASE,
      opts: {
        box: client,
        folderId: '12345',
        backoffMs: 0,
        sleep: () => Promise.resolve(),
      },
    });
    assert.equal(result.ok, true);
    assert.equal(result.fileId, 'recovered');
    assert.equal(captured.calls.length, 3);
  });

  it('refuses to upload markdown containing "## 5. Jun Review Only" (§2.3 guard)', async () => {
    const withS5 =
      STRIPPED + '## 5. Jun Review Only\n$AAPL 150/148/155\n';
    const { client, captured } = makeBoxMock();
    await assert.rejects(
      () =>
        uploadBrief({
          ...BASE,
          markdown: withS5,
          opts: { box: client, folderId: '12345' },
        }),
      /absolute-boundary/,
    );
    assert.equal(captured.calls.length, 0);
  });

  it('rejects invalid status values', async () => {
    await assert.rejects(
      () =>
        uploadBrief({
          ...BASE,
          status: 'WEIRD',
          opts: { folderId: '12345' },
        }),
      /status must be/,
    );
  });

  it('returns ok=false with a clear log when no Box client is provided', async () => {
    const result = await uploadBrief({
      ...BASE,
      opts: { folderId: '12345' },
    });
    assert.equal(result.ok, false);
    assert.equal(result.fileId, null);
    assert.equal(result.boxUrl, null);
    // fileName is still returned so the caller can log it.
    assert.equal(result.fileName, '2026-04-20_deep_research_brief.md');
  });

  it('returns ok=false when folderId is missing', async () => {
    const { client } = makeBoxMock();
    const result = await uploadBrief({
      ...BASE,
      opts: { box: client /* folderId omitted */ },
    });
    assert.equal(result.ok, false);
    assert.equal(result.fileId, null);
  });

  it('requires markdown to be a string', async () => {
    await assert.rejects(
      () =>
        uploadBrief({
          ...BASE,
          markdown: null,
          opts: { folderId: '12345' },
        }),
      TypeError,
    );
    await assert.rejects(
      () =>
        uploadBrief({
          ...BASE,
          markdown: 42,
          opts: { folderId: '12345' },
        }),
      TypeError,
    );
  });

  it('sends markdown bytes verbatim (no BOM, no rewrite)', async () => {
    const { client, captured } = makeBoxMock();
    await uploadBrief({
      ...BASE,
      opts: { box: client, folderId: '12345' },
    });
    const bodyBytes = captured.calls[0].body;
    assert.ok(Buffer.isBuffer(bodyBytes));
    assert.equal(bodyBytes.toString('utf8'), STRIPPED);
  });
});
