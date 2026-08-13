/**
 * @file Section 5 strip hardening — Phase C live-format coverage.
 *
 * The original `stripSection5()` was keyed on the design's literal
 * `## 5. Jun Review Only` heading and the `$TICKER` notation. The real
 * `DAILY_DEEP_RESEARCH` rows already in `magi_core.market_research` use
 * section-sign numbering (`## §1` … `## §7`), localized titles, and
 * bare ticker names (AAPL / TSLA / NVDA, no `$`). These tests lock down
 * the hardened behaviour so the absolute boundary (design §2.3) holds
 * for BOTH the Phase B fallback format and the Phase C live format:
 *
 *   - sentinel-fence removal (primary, language/numbering-agnostic),
 *   - "Jun Review Only" heading-label removal across numbering variants,
 *   - the actionable-pick leak sensor (bare + `$` tickers), and
 *   - NO false positives on the legitimate bare-ticker Watchlist.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  stripSection5,
  containsSection5,
  SECTION5_SENTINEL_BEGIN,
  SECTION5_SENTINEL_END,
} from '../src/strip.mjs';

describe('stripSection5 — sentinel fence (Phase C primary contract)', () => {
  it('removes a sentinel-fenced block even when the heading is renumbered/localized', () => {
    const input = [
      '## §1. 市況サマリー',
      'VIX 17.',
      SECTION5_SENTINEL_BEGIN,
      '## §8. Jun限定レビュー', // localized heading with NO English label
      'AAPL LONG entry 190 stop 185 target 210',
      'TSLA SHORT entry 250 stop 260 target 230',
      SECTION5_SENTINEL_END,
      '## §6. ウォッチリスト',
      '- AAPL: iPhone需要は堅調',
    ].join('\n');
    const { stripped, status, section5Removed, leakSuspected } =
      stripSection5(input);
    assert.match(stripped, /市況サマリー/);
    assert.match(stripped, /ウォッチリスト/);
    assert.doesNotMatch(stripped, /Jun限定/);
    assert.doesNotMatch(stripped, /entry 190/);
    assert.doesNotMatch(stripped, /SHORT entry 250/);
    assert.doesNotMatch(stripped, /SECTION5/); // sentinels themselves gone
    assert.equal(section5Removed, true);
    assert.equal(leakSuspected, false);
    assert.equal(status, 'success');
  });

  it('drops an unterminated sentinel fence through to EOF (fail-safe)', () => {
    const input = [
      '## §1. Macro',
      'A',
      SECTION5_SENTINEL_BEGIN,
      '## §5. Jun限定',
      'AAPL LONG 190 185 210',
      // no END sentinel — must still strip everything after BEGIN
    ].join('\n');
    const { stripped, section5Removed } = stripSection5(input);
    assert.match(stripped, /## §1\. Macro/);
    assert.doesNotMatch(stripped, /Jun限定/);
    assert.doesNotMatch(stripped, /AAPL LONG/);
    assert.equal(section5Removed, true);
  });

  it('drops a stray END sentinel artifact while keeping surrounding content', () => {
    const input = ['## §1. Macro', 'keep me', SECTION5_SENTINEL_END, 'and me'].join(
      '\n',
    );
    const { stripped } = stripSection5(input);
    assert.match(stripped, /keep me/);
    assert.match(stripped, /and me/);
    assert.doesNotMatch(stripped, /SECTION5/);
  });

  it('handles a CRLF-authored sentinel fence', () => {
    const input =
      '## §1. Macro\r\nA\r\n' +
      `${SECTION5_SENTINEL_BEGIN}\r\n## §5. Jun Review Only\r\nAAPL LONG 1 2 3\r\n${SECTION5_SENTINEL_END}\r\n` +
      '## §6. Watchlist\r\nkeep\r\n';
    const { stripped } = stripSection5(input);
    assert.match(stripped, /## §6\. Watchlist/);
    assert.match(stripped, /keep/);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /AAPL LONG/);
  });
});

describe('stripSection5 — heading-label removal across numbering variants', () => {
  for (const heading of [
    '## §5. Jun Review Only',
    '## §5 Jun Review Only',
    '## 5) Jun Review Only',
    '## 8. Jun Review Only (CONFIDENTIAL)',
    '## Jun Review Only',
    '## §5. JUN REVIEW ONLY', // casing drift
  ]) {
    it(`strips heading "${heading}" via the label match`, () => {
      const input = [
        '## §1. Macro',
        'A',
        heading,
        'AAPL LONG entry 190 stop 185 target 210',
        '## §6. Watchlist',
        '- NVDA: AI demand intact',
      ].join('\n');
      const { stripped, section5Removed } = stripSection5(input);
      assert.match(stripped, /## §1\. Macro/);
      assert.match(stripped, /## §6\. Watchlist/);
      assert.doesNotMatch(stripped, /Jun Review Only/i);
      assert.doesNotMatch(stripped, /entry 190/);
      assert.equal(section5Removed, true);
    });
  }

  it('does NOT strip a §-numbered NON-Jun section (e.g. live "## §5. トピックス")', () => {
    const input = [
      '## §5. トピックス',
      'AI関連の話題が中心。',
      '## §6. ウォッチリスト',
      '- AAPL: 堅調',
    ].join('\n');
    const { stripped, section5Removed, status } = stripSection5(input);
    assert.match(stripped, /トピックス/);
    assert.match(stripped, /ウォッチリスト/);
    assert.equal(section5Removed, false);
    assert.equal(status, 'success');
  });
});

describe('stripSection5 — actionable-pick leak sensor (bare tickers)', () => {
  it('flags a bare-ticker pick line that survives the strip', () => {
    // No heading / sentinel to strip — the pick line leaks through and
    // must be caught by the sensor as a backstop.
    const input = [
      '## §4. Notes',
      'AAPL LONG entry 190 stop 185 target 210',
    ].join('\n');
    const { status, leakSuspected, leakSamples } = stripSection5(input);
    assert.equal(status, 'partial');
    assert.equal(leakSuspected, true);
    assert.ok(leakSamples.some((l) => /AAPL LONG/.test(l)));
  });

  it('flags a bare-ticker pick written as a markdown table row', () => {
    const input = ['## §4. Notes', '| AAPL | LONG | 150 | 148 | 155 |'].join('\n');
    const { status, leakSuspected } = stripSection5(input);
    assert.equal(leakSuspected, true);
    assert.equal(status, 'partial');
  });

  it('flags a bare-ticker pick that omits the direction but lists levels', () => {
    const input = ['## §4. Notes', 'NVDA entry 900 stop 880 target 960'].join('\n');
    const { leakSuspected } = stripSection5(input);
    assert.equal(leakSuspected, true);
  });

  it('flags Japanese-language pick lines (ショート + 損切り/目標株価)', () => {
    const input = [
      '## §4. メモ',
      'TSLA ショート エントリー250 損切り260 目標株価230',
    ].join('\n');
    const { leakSuspected, status } = stripSection5(input);
    assert.equal(leakSuspected, true);
    assert.equal(status, 'partial');
  });
});

describe('stripSection5 — no false positives on the live Watchlist format', () => {
  // A realistic §1–§7 brief with NO Jun-only section, bare tickers in
  // the §6 watchlist, and a "目標株価" mention in §3. None of these are
  // leaks; the strip must be a clean no-op with status=success.
  const liveBrief = [
    '## §1. 市況サマリー',
    'S&P 500は7,555.25で引け。VIXは17.2でLOW_FEAR。',
    '## §2. 本日の最大材料',
    'CPI発表を控えて様子見。',
    '## §3. 本日の決算・イベント',
    '**WDC (Western Digital)**: +16.1% — JPモルガンが目標株価引き上げ（HDD価格上昇）',
    '## §4. リスク',
    '金利上昇とAI設備投資の過熱感。',
    '## §5. トピックス',
    'データセンター需要の論点。',
    '## §6. ウォッチリスト各銘柄の論点',
    '- **AAPL (Apple)**: サービス収益が利益を牽引',
    '- **TSLA (Tesla)**: ロボタクシー期待、AI設備投資が重し',
    '- **NVDA (NVIDIA)**: AI需要は依然堅調',
    '- **QQQ / BABA / OXY**: 個別材料を注視',
    '## §7. 本日の米国経済指標',
    '08:30 ET CPI、14:00 ET FOMC議事要旨。',
  ].join('\n');

  it('returns status=success with no leak and no strip', () => {
    const { status, leakSuspected, section5Removed, stripped } =
      stripSection5(liveBrief);
    assert.equal(status, 'success');
    assert.equal(leakSuspected, false);
    assert.equal(section5Removed, false);
    // Content is preserved verbatim (modulo trailing-newline normalisation).
    assert.match(stripped, /ウォッチリスト/);
    assert.match(stripped, /AAPL \(Apple\)/);
    assert.match(stripped, /目標株価引き上げ/);
  });

  it('does not flag bare tickers in qualitative prose as picks', () => {
    const { leakSamples } = stripSection5(liveBrief);
    assert.deepEqual(leakSamples, []);
  });
});

describe('containsSection5 — shared writer-boundary predicate', () => {
  it('is true for the design heading, §-numbered headings, and the sentinel fence', () => {
    assert.equal(containsSection5('## 5. Jun Review Only\n$AAPL'), true);
    assert.equal(containsSection5('## §5. Jun Review Only\nAAPL'), true);
    assert.equal(containsSection5('## 8. Jun Review Only (CONFIDENTIAL)'), true);
    assert.equal(
      containsSection5(`pre\n${SECTION5_SENTINEL_BEGIN}\nx\n${SECTION5_SENTINEL_END}`),
      true,
    );
  });

  it('is false for clean briefs, non-Jun §5, and non-strings', () => {
    assert.equal(containsSection5('## §5. トピックス\nAAPL is up'), false);
    assert.equal(
      containsSection5('## 1. Macro\n## 6. Watchlist\n- NVDA earnings'),
      false,
    );
    assert.equal(containsSection5(''), false);
    assert.equal(containsSection5(null), false);
    assert.equal(containsSection5(42), false);
  });
});
