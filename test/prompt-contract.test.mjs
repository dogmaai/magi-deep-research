/**
 * @file Phase A-10 prompt structural regression gate.
 *
 * `buildPrompt()` in `src/fallback.mjs` produces a load-bearing
 * instruction string: the downstream pipeline relies on the brief
 * having exactly five H2 sections with exact headings, in exact
 * order. `stripSection5()` (`src/strip.mjs`) matches
 * `/^## 5\. Jun Review Only\b/` — any drift in casing, numbering, or
 * wording of Section 5 would silently cause the strip to no-op and
 * leak Jun-only picks into BigQuery / PLM.
 *
 * These tests lock down the structural invariants of the prompt.
 * They do NOT call Gemini; they assert on the instruction text.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §2.3, §5.3, §5.5.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildPrompt } from '../src/fallback.mjs';
import {
  stripSection5,
  SECTION5_SENTINEL_BEGIN,
  SECTION5_SENTINEL_END,
} from '../src/strip.mjs';

const REQUIRED_HEADINGS = Object.freeze([
  '## 1. Macro',
  '## 2. Sector',
  '## 3. Risks',
  '## 4. Watchlist',
  '## 5. Jun Review Only',
]);

describe('buildPrompt() structural contract', () => {
  it('instructs the model to emit all five required H2 headings verbatim', () => {
    const prompt = buildPrompt();
    for (const heading of REQUIRED_HEADINGS) {
      // We assert the backtick-quoted heading appears — that is how
      // the prompt instructs the model to emit each heading.
      assert.ok(
        prompt.includes('`' + heading + '`'),
        `prompt must instruct verbatim heading: ${heading}`,
      );
    }
  });

  it('lists the five required headings in exact order (1→5)', () => {
    const prompt = buildPrompt();
    const offsets = REQUIRED_HEADINGS.map((h) => prompt.indexOf('`' + h + '`'));
    for (const offset of offsets) {
      assert.notEqual(offset, -1, 'every required heading must appear');
    }
    for (let i = 1; i < offsets.length; i += 1) {
      assert.ok(
        offsets[i - 1] < offsets[i],
        `heading ${REQUIRED_HEADINGS[i]} must appear after ${REQUIRED_HEADINGS[i - 1]} in the prompt`,
      );
    }
  });

  it('uses the load-bearing "## 5. Jun Review Only" heading compatible with stripSection5', () => {
    const prompt = buildPrompt();
    // The strip regex is /^## 5\. Jun Review Only\b/m. We simulate
    // the model actually emitting exactly what the prompt asks for,
    // then round-trip through stripSection5() to prove the contract.
    const simulatedBrief = [
      '## 1. Macro',
      'A',
      '## 2. Sector',
      'B',
      '## 3. Risks',
      'C',
      '## 4. Watchlist',
      'D',
      '## 5. Jun Review Only',
      'E $AAPL 150/148/155',
    ].join('\n');
    const { stripped, status } = stripSection5(simulatedBrief);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
    assert.equal(status, 'success');

    // Also confirm the prompt-emitted heading string matches the
    // regex boundary character class (`\b`) — i.e. the heading ends
    // cleanly at whitespace / EOL.
    assert.match('## 5. Jun Review Only', /^## 5\. Jun Review Only\b/);
    assert.ok(prompt.includes('`## 5. Jun Review Only`'));
  });

  it('instructs the model to wrap Section 5 in the sentinel fence', () => {
    const prompt = buildPrompt();
    assert.ok(
      prompt.includes(SECTION5_SENTINEL_BEGIN),
      'prompt must instruct the BEGIN sentinel verbatim',
    );
    assert.ok(
      prompt.includes(SECTION5_SENTINEL_END),
      'prompt must instruct the END sentinel verbatim',
    );
    // The BEGIN sentinel instruction must precede the END instruction.
    assert.ok(
      prompt.indexOf(SECTION5_SENTINEL_BEGIN) <
        prompt.indexOf(SECTION5_SENTINEL_END),
    );
  });

  it('round-trips a §-renumbered, sentinel-fenced Section 5 through stripSection5', () => {
    // Simulate a live-format model emission: section-sign numbering, a
    // localized Jun-only heading, bare-ticker picks, AND the sentinel
    // fence the prompt now requires. The strip must remove it cleanly.
    const simulatedBrief = [
      '## §1. Macro',
      'A',
      '## §2. Sector',
      'B',
      '## §3. Risks',
      'C',
      '## §4. Watchlist',
      '- AAPL: context only',
      SECTION5_SENTINEL_BEGIN,
      '## §5. Jun限定レビュー',
      'AAPL LONG entry 190 stop 185 target 210',
      SECTION5_SENTINEL_END,
    ].join('\n');
    const { stripped, status, leakSuspected } = stripSection5(simulatedBrief);
    assert.doesNotMatch(stripped, /Jun限定/);
    assert.doesNotMatch(stripped, /entry 190/);
    assert.match(stripped, /## §4\. Watchlist/);
    assert.equal(leakSuspected, false);
    assert.equal(status, 'success');
  });

  it('forbids the model from listing ticker symbols in Section 3 (§2.3 boundary)', () => {
    // Section 3 must not contain tickers — only Section 5 (Jun-only)
    // carries ticker picks. Regression gate for a subtle prompt-drift
    // failure mode where an earlier section starts leaking picks.
    const prompt = buildPrompt();
    assert.match(
      prompt,
      /`## 3\. Risks`[\s\S]*?\*\*do not\*\* list individual ticker symbols/i,
      'Section 3 instruction must explicitly forbid ticker symbols',
    );
  });

  it('describes Section 5 as CONFIDENTIAL and stripped before downstream LLMs', () => {
    const prompt = buildPrompt();
    assert.match(prompt, /`## 5\. Jun Review Only`[\s\S]*CONFIDENTIAL/);
    assert.match(
      prompt,
      /stripped before the brief reaches any downstream LLM/i,
    );
  });

  it('forbids code fences / JSON wrappers in the output', () => {
    const prompt = buildPrompt();
    assert.match(prompt, /No code fences, no JSON wrapper/);
  });

  it('forbids additional H2 headings beyond the five required', () => {
    const prompt = buildPrompt();
    assert.match(prompt, /Do NOT introduce other H2 headings/);
  });

  it('embeds the supplied dateIso verbatim', () => {
    const prompt = buildPrompt({ dateIso: '2026-04-20' });
    assert.match(prompt, /US equity session on 2026-04-20 \(ET\)/);
  });

  it('embeds the supplied tickerUniverse in Section 5 guidance', () => {
    const prompt = buildPrompt({
      tickerUniverse: ['AAPL', 'MSFT', 'NVDA'],
    });
    assert.match(prompt, /drawn from the MAGI universe \(AAPL, MSFT, NVDA\)/);
  });

  it('produces deterministic output for identical inputs', () => {
    const a = buildPrompt({ dateIso: '2026-04-20' });
    const b = buildPrompt({ dateIso: '2026-04-20' });
    assert.equal(a, b);
  });

  it('instructs analyst-report tone and forbids emojis / disclaimers / sign-offs', () => {
    const prompt = buildPrompt();
    assert.match(prompt, /analyst-report tone/);
    assert.match(prompt, /No emojis, no disclaimers, no sign-offs/);
  });
});
