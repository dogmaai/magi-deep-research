import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_BUCKET,
  DEFAULT_PREFIX,
  gsUriFor,
  objectNameFor,
  uploadRawEnvelope,
} from '../src/gcs.mjs';

/**
 * Build a `@google-cloud/storage`-shaped client mock whose
 * `file.save` behavior is controlled by `outcomes` (one per
 * expected attempt).
 *
 * @param {Object} [opts]
 * @param {Array<{type:'ok'|'throw', payload?: unknown}>} [opts.outcomes]
 */
function makeGcsMock({ outcomes = [{ type: 'ok' }] } = {}) {
  const captured = {
    bucketArg: null,
    objectArg: null,
    saves: [],
  };
  let attempt = 0;
  const client = {
    bucket(name) {
      captured.bucketArg = name;
      return {
        file(objectName) {
          captured.objectArg = objectName;
          return {
            async save(body, opts) {
              captured.saves.push({ body, opts });
              const outcome = outcomes[attempt] ?? outcomes[outcomes.length - 1];
              attempt += 1;
              if (outcome.type === 'ok') return;
              if (outcome.type === 'throw') {
                throw outcome.payload ?? new Error('stub gcs exception');
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

describe('objectNameFor / gsUriFor', () => {
  it('builds raw/<date>.json by default', () => {
    assert.equal(objectNameFor('2026-04-20'), 'raw/2026-04-20.json');
  });

  it('accepts empty prefix', () => {
    assert.equal(objectNameFor('2026-04-20', ''), '2026-04-20.json');
  });

  it('accepts custom prefix ending in /', () => {
    assert.equal(
      objectNameFor('2026-04-20', 'envelopes/'),
      'envelopes/2026-04-20.json',
    );
  });

  it('rejects malformed dates', () => {
    assert.throws(() => objectNameFor('2026/04/20'), TypeError);
    assert.throws(() => objectNameFor('20260420'), TypeError);
    assert.throws(() => objectNameFor(''), TypeError);
  });

  it('rejects prefix that does not end with /', () => {
    assert.throws(() => objectNameFor('2026-04-20', 'envelopes'), TypeError);
  });

  it('composes gs:// URIs', () => {
    assert.equal(
      gsUriFor('magi-deep-research-raw', 'raw/2026-04-20.json'),
      'gs://magi-deep-research-raw/raw/2026-04-20.json',
    );
  });
});

describe('uploadRawEnvelope', () => {
  const BASE = {
    date: '2026-04-20',
    envelope: { ok: true, sections: 5, text: '## 1. Macro\nVIX 17\n...' },
    status: 'success',
  };

  it('uploads to magi-deep-research-raw/raw/<date>.json by default', async () => {
    const { client, captured } = makeGcsMock();
    const result = await uploadRawEnvelope({ ...BASE, opts: { gcs: client } });
    assert.equal(result.ok, true);
    assert.equal(result.objectName, 'raw/2026-04-20.json');
    assert.equal(
      result.gcsUri,
      `gs://${DEFAULT_BUCKET}/${DEFAULT_PREFIX}2026-04-20.json`,
    );
    assert.equal(captured.bucketArg, DEFAULT_BUCKET);
    assert.equal(captured.objectArg, 'raw/2026-04-20.json');
  });

  it('serialises the envelope as JSON with Content-Type application/json', async () => {
    const { client, captured } = makeGcsMock();
    await uploadRawEnvelope({ ...BASE, opts: { gcs: client } });
    const saved = captured.saves[0];
    assert.equal(saved.opts.metadata.contentType, 'application/json');
    assert.equal(saved.opts.resumable, false);
    const parsed = JSON.parse(saved.body.toString('utf8'));
    assert.deepEqual(parsed, BASE.envelope);
  });

  it('sets metadata.status / metadata.date / metadata.source', async () => {
    const { client, captured } = makeGcsMock();
    await uploadRawEnvelope({ ...BASE, opts: { gcs: client } });
    const saved = captured.saves[0];
    assert.equal(saved.opts.metadata.metadata.status, 'success');
    assert.equal(saved.opts.metadata.metadata.date, '2026-04-20');
    assert.equal(saved.opts.metadata.metadata.source, 'magi-deep-research');
  });

  it('merges extraMetadata without overwriting status/date/source', async () => {
    const { client, captured } = makeGcsMock();
    await uploadRawEnvelope({
      ...BASE,
      opts: { gcs: client, extraMetadata: { session_id: 'abc', prompt_version: 'v2.0' } },
    });
    const m = captured.saves[0].opts.metadata.metadata;
    assert.equal(m.session_id, 'abc');
    assert.equal(m.prompt_version, 'v2.0');
    assert.equal(m.status, 'success');
  });

  it('refuses to let extraMetadata clobber status/date/source', async () => {
    const { client, captured } = makeGcsMock();
    await uploadRawEnvelope({
      date: '2026-04-20',
      envelope: { ok: true },
      status: 'failed',
      opts: {
        gcs: client,
        extraMetadata: {
          // Malicious / buggy caller tries to relabel a failed run.
          status: 'success',
          date: '1970-01-01',
          source: 'impersonator',
          session_id: 'legit-value-still-survives',
        },
      },
    });
    const m = captured.saves[0].opts.metadata.metadata;
    assert.equal(m.status, 'failed', 'core status must win over extraMetadata');
    assert.equal(m.date, '2026-04-20', 'core date must win over extraMetadata');
    assert.equal(
      m.source,
      'magi-deep-research',
      'core source must win over extraMetadata',
    );
    assert.equal(
      m.session_id,
      'legit-value-still-survives',
      'non-conflicting extras are still preserved',
    );
  });

  it('respects bucket / prefix overrides', async () => {
    const { client, captured } = makeGcsMock();
    const result = await uploadRawEnvelope({
      ...BASE,
      opts: {
        gcs: client,
        bucket: 'magi-deep-research-raw-test',
        prefix: 'envelopes/',
      },
    });
    assert.equal(captured.bucketArg, 'magi-deep-research-raw-test');
    assert.equal(result.objectName, 'envelopes/2026-04-20.json');
    assert.equal(
      result.gcsUri,
      'gs://magi-deep-research-raw-test/envelopes/2026-04-20.json',
    );
  });

  it('retries up to maxRetries=3 on throws and returns ok=false', async () => {
    const { client, captured } = makeGcsMock({
      outcomes: Array(3).fill({ type: 'throw', payload: new Error('net blip') }),
    });
    const sleepCalls = [];
    const result = await uploadRawEnvelope({
      ...BASE,
      opts: {
        gcs: client,
        backoffMs: 10,
        sleep: (ms) => {
          sleepCalls.push(ms);
          return Promise.resolve();
        },
      },
    });
    assert.equal(result.ok, false);
    assert.equal(captured.saves.length, 3);
    assert.deepEqual(sleepCalls, [10, 20]);
    // gcsUri is still well-defined so the caller can log it.
    assert.equal(result.objectName, 'raw/2026-04-20.json');
  });

  it('returns ok=true when the 2nd attempt succeeds', async () => {
    const { client, captured } = makeGcsMock({
      outcomes: [
        { type: 'throw', payload: new Error('retryable') },
        { type: 'ok' },
      ],
    });
    const result = await uploadRawEnvelope({
      ...BASE,
      opts: {
        gcs: client,
        backoffMs: 0,
        sleep: () => Promise.resolve(),
      },
    });
    assert.equal(result.ok, true);
    assert.equal(captured.saves.length, 2);
  });

  it('rejects invalid status values', async () => {
    await assert.rejects(
      () => uploadRawEnvelope({ ...BASE, status: 'WEIRD' }),
      /status must be/,
    );
  });

  it('writes envelopes containing Section 5 verbatim (raw bucket is pre-strip)', async () => {
    const { client, captured } = makeGcsMock();
    const envelopeWithS5 = {
      raw: '## 1. Macro\n...\n## 5. Jun Review Only\n$AAPL 150/148/155\n',
    };
    await uploadRawEnvelope({
      date: '2026-04-20',
      envelope: envelopeWithS5,
      status: 'success',
      opts: { gcs: client },
    });
    const body = captured.saves[0].body.toString('utf8');
    assert.match(body, /Jun Review Only/);
    assert.match(body, /\$AAPL/);
  });
});
