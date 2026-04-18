import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_GENERATION_CONFIG,
  DEFAULT_MODEL,
  buildPrompt,
  generateDeepBrief,
} from '../src/fallback.mjs';
import { stripSection5 } from '../src/strip.mjs';

/**
 * Build a minimal `@google/genai`-shaped client mock that captures
 * the request and replies with a canned markdown body.
 *
 * @param {Object} [opts]
 * @param {string} [opts.text]         - Markdown body to return via `.text`.
 * @param {Error}  [opts.throwError]   - If set, `generateContent` rejects with it.
 * @param {Object} [opts.rawResponse]  - If set, returned verbatim (takes precedence over text).
 */
function makeMockClient({ text, throwError, rawResponse } = {}) {
  const captured = { calls: [] };
  const client = {
    models: {
      async generateContent(req) {
        captured.calls.push(req);
        if (throwError) throw throwError;
        if (rawResponse !== undefined) return rawResponse;
        return { text: text ?? '## 1. Macro\nstub\n## 5. Jun Review Only\nstub\n' };
      },
    },
  };
  return { client, captured };
}

describe('buildPrompt', () => {
  it('includes all five required H2 section headings, in order', () => {
    const p = buildPrompt({ dateIso: '2026-04-20' });
    const headings = [
      '## 1. Macro',
      '## 2. Sector',
      '## 3. Risks',
      '## 4. Watchlist',
      '## 5. Jun Review Only',
    ];
    let cursor = -1;
    for (const h of headings) {
      const idx = p.indexOf(h, cursor + 1);
      assert.ok(idx > cursor, `heading '${h}' not found after position ${cursor}`);
      cursor = idx;
    }
  });

  it('embeds the requested ET session date', () => {
    const p = buildPrompt({ dateIso: '2026-04-20' });
    assert.match(p, /2026-04-20/);
  });

  it('defaults dateIso to today (YYYY-MM-DD) when omitted', () => {
    const p = buildPrompt();
    const today = new Date().toISOString().slice(0, 10);
    assert.ok(p.includes(today), `expected today's date ${today} to appear in prompt`);
  });

  it('references the default MAGI ticker universe in Section 5 guidance', () => {
    const p = buildPrompt();
    for (const sym of ['AAPL', 'NVDA', 'SPY']) {
      assert.ok(p.includes(sym), `expected default universe to include ${sym}`);
    }
  });

  it('honours a custom ticker universe override', () => {
    const p = buildPrompt({ tickerUniverse: ['ZZZA', 'ZZZB'] });
    assert.match(p, /ZZZA/);
    assert.match(p, /ZZZB/);
    assert.doesNotMatch(p, /AAPL/);
  });

  it('instructs Section 3 not to contain individual ticker symbols', () => {
    const p = buildPrompt();
    // Phrasing is prescriptive so `stripSection5`'s ticker-leak
    // detector (≥5 → status=partial) does not fire on healthy briefs.
    assert.match(p, /## 3\. Risks[\s\S]*do\s+not[\s\S]*ticker/i);
  });

  it('is stable across adjacent calls with identical inputs', () => {
    const a = buildPrompt({ dateIso: '2026-04-20' });
    const b = buildPrompt({ dateIso: '2026-04-20' });
    assert.equal(a, b);
  });
});

describe('generateDeepBrief', () => {
  it('uses the injected ai client and returns its text body verbatim', async () => {
    const { client } = makeMockClient({
      text: '## 1. Macro\nbody\n## 2. Sector\nbody\n## 3. Risks\nbody\n## 4. Watchlist\nbody\n## 5. Jun Review Only\n$AAPL LONG 150 / 148 / 155\n',
    });
    const md = await generateDeepBrief({ dateIso: '2026-04-20', ai: client });
    assert.match(md, /## 1\. Macro/);
    assert.match(md, /## 5\. Jun Review Only/);
    assert.match(md, /\$AAPL/);
  });

  it('passes the default model and generation config', async () => {
    const { client, captured } = makeMockClient({ text: '## 1. Macro\nx\n' });
    await generateDeepBrief({ dateIso: '2026-04-20', ai: client });
    const call = captured.calls[0];
    assert.equal(call.model, DEFAULT_MODEL);
    assert.equal(call.config.temperature, DEFAULT_GENERATION_CONFIG.temperature);
    assert.equal(
      call.config.maxOutputTokens,
      DEFAULT_GENERATION_CONFIG.maxOutputTokens,
    );
  });

  it('honours model override', async () => {
    const { client, captured } = makeMockClient({ text: '## 1. Macro\nx\n' });
    await generateDeepBrief({
      dateIso: '2026-04-20',
      ai: client,
      model: 'gemini-test-override',
    });
    assert.equal(captured.calls[0].model, 'gemini-test-override');
  });

  it('attaches the Google Search grounding tool by default', async () => {
    const { client, captured } = makeMockClient({ text: '## 1. Macro\nx\n' });
    await generateDeepBrief({ dateIso: '2026-04-20', ai: client });
    const tools = captured.calls[0].config.tools;
    assert.ok(Array.isArray(tools), 'expected config.tools to be an array');
    assert.ok(
      tools.some((t) => t && typeof t === 'object' && 'googleSearch' in t),
      'expected googleSearch grounding tool',
    );
  });

  it('disables grounding when grounding=false', async () => {
    const { client, captured } = makeMockClient({ text: '## 1. Macro\nx\n' });
    await generateDeepBrief({
      dateIso: '2026-04-20',
      ai: client,
      grounding: false,
    });
    assert.equal(captured.calls[0].config.tools, undefined);
  });

  it('sends a prompt containing the requested date', async () => {
    const { client, captured } = makeMockClient({ text: '## 1. Macro\nx\n' });
    await generateDeepBrief({ dateIso: '2026-04-20', ai: client });
    assert.match(captured.calls[0].contents, /2026-04-20/);
  });

  it('extracts text from candidates[].content.parts when .text is absent', async () => {
    const { client } = makeMockClient({
      rawResponse: {
        candidates: [
          {
            content: {
              parts: [
                { text: '## 1. Macro\n' },
                { text: 'body\n' },
                { text: '## 5. Jun Review Only\npick\n' },
              ],
            },
          },
        ],
      },
    });
    const md = await generateDeepBrief({ dateIso: '2026-04-20', ai: client });
    assert.match(md, /## 1\. Macro/);
    assert.match(md, /## 5\. Jun Review Only/);
  });

  it('throws a descriptive error when the response has no text body', async () => {
    const { client } = makeMockClient({ rawResponse: { candidates: [] } });
    await assert.rejects(
      () => generateDeepBrief({ dateIso: '2026-04-20', ai: client }),
      /did not contain a text body/,
    );
  });

  it('throws when Gemini returns an empty markdown string', async () => {
    const { client } = makeMockClient({ text: '' });
    await assert.rejects(
      () => generateDeepBrief({ dateIso: '2026-04-20', ai: client }),
      /empty markdown body/,
    );
  });

  it('propagates SDK errors from generateContent', async () => {
    const { client } = makeMockClient({
      throwError: new Error('503 Service Unavailable'),
    });
    await assert.rejects(
      () => generateDeepBrief({ dateIso: '2026-04-20', ai: client }),
      /503 Service Unavailable/,
    );
  });

  it('produces a brief whose Section 5 is strippable (end-to-end contract)', async () => {
    // Simulate a realistic Gemini response and pipe through
    // stripSection5. This is the integration point of the Phase B
    // fallback: fallback.mjs produces, strip.mjs sanitises before
    // BigQuery insert / PLM. Both sides of the contract must line up.
    const raw = [
      '## 1. Macro',
      'VIX 17, LOW_FEAR; SPX +0.4% yesterday.',
      '',
      '## 2. Sector',
      'Tech led, energy lagged.',
      '',
      '## 3. Risks',
      'CPI print this morning; curve flattening risk.',
      '',
      '## 4. Watchlist',
      '- NVDA: earnings tomorrow.',
      '- SPY: macro flow sensitivity.',
      '',
      '## 5. Jun Review Only',
      '- $AAPL LONG entry 150 stop 148 target 155',
      '- $MSFT LONG entry 410 stop 405 target 420',
      '',
    ].join('\n');
    const { client } = makeMockClient({ text: raw });
    const md = await generateDeepBrief({ dateIso: '2026-04-20', ai: client });
    const { stripped, status } = stripSection5(md);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
    assert.doesNotMatch(stripped, /\$MSFT/);
    assert.match(stripped, /## 4\. Watchlist/);
    assert.match(stripped, /NVDA/);
    assert.equal(status, 'success');
  });
});
