import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { stripSection5 } from '../src/strip.mjs';

describe('stripSection5', () => {
  it('removes Section 5 at end of document', () => {
    const input = [
      '## 1. Macro',
      'Macro content.',
      '',
      '## 5. Jun Review Only',
      'Ticker picks: $AAPL entry 150 stop 148 target 155.',
      '$MSFT $NVDA $TSLA $META',
    ].join('\n');
    const { stripped, status } = stripSection5(input);
    assert.match(stripped, /## 1\. Macro/);
    assert.match(stripped, /Macro content/);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
    assert.doesNotMatch(stripped, /\$MSFT/);
    assert.equal(status, 'success');
  });

  it('preserves Section 6 and beyond following Section 5', () => {
    const input = [
      '## 1. Macro',
      'A',
      '## 5. Jun Review Only',
      '$AAPL $MSFT',
      '## 6. Appendix',
      'Appendix text.',
    ].join('\n');
    const { stripped } = stripSection5(input);
    assert.match(stripped, /## 1\. Macro/);
    assert.match(stripped, /## 6\. Appendix/);
    assert.match(stripped, /Appendix text/);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
  });

  it('returns unchanged content when no Section 5 is present', () => {
    const input = '## 1. Macro\nBody.\n\n## 2. Risks\nBody 2.';
    const { stripped, tickersRemaining, status } = stripSection5(input);
    assert.match(stripped, /## 1\. Macro/);
    assert.match(stripped, /## 2\. Risks/);
    assert.match(stripped, /Body 2/);
    assert.equal(tickersRemaining, 0);
    assert.equal(status, 'success');
  });

  it('handles CRLF line endings', () => {
    const input =
      '## 1. Macro\r\nA\r\n## 5. Jun Review Only\r\n$AAPL\r\n## 6. End\r\nkeep\r\n';
    const { stripped } = stripSection5(input);
    assert.match(stripped, /## 1\. Macro/);
    assert.match(stripped, /## 6\. End/);
    assert.match(stripped, /keep/);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
  });

  it('swallows H3/H4 sub-headings and bodies inside Section 5', () => {
    const input = [
      '## 1. Macro',
      '## 5. Jun Review Only',
      '### 5.1 Longs',
      '$AAPL entry 150',
      '#### details',
      'stop 148',
      '## 6. Appendix',
      'End.',
    ].join('\n');
    const { stripped } = stripSection5(input);
    assert.doesNotMatch(stripped, /5\.1 Longs/);
    assert.doesNotMatch(stripped, /details/);
    assert.doesNotMatch(stripped, /\$AAPL/);
    assert.match(stripped, /## 6\. Appendix/);
    assert.match(stripped, /End\./);
  });

  it('strips multiple Section 5 blocks if they appear (defensive)', () => {
    const input = [
      '## 5. Jun Review Only',
      '$AAPL',
      '## 1. Macro',
      'A',
      '## 5. Jun Review Only',
      '$MSFT',
    ].join('\n');
    const { stripped, tickersRemaining } = stripSection5(input);
    assert.match(stripped, /## 1\. Macro/);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
    assert.doesNotMatch(stripped, /\$MSFT/);
    assert.equal(tickersRemaining, 0);
  });

  it('flags status=partial when 5+ tickers remain in stripped output', () => {
    const input = [
      '## 3. Risks',
      'Watch: $AAPL $MSFT $NVDA $TSLA $META leaking from Section 3.',
      '## 5. Jun Review Only',
      '$AMZN',
    ].join('\n');
    const { status, tickersRemaining } = stripSection5(input);
    assert.equal(status, 'partial');
    assert.ok(tickersRemaining >= 5, `expected >=5 tickers, got ${tickersRemaining}`);
  });

  it('returns status=success when fewer than 5 tickers remain', () => {
    const input = [
      '## 3. Risks',
      'Watch: $VIX $SPY.',
      '## 5. Jun Review Only',
      '$AMZN $GOOG $META $NVDA $AAPL',
    ].join('\n');
    const { status, tickersRemaining } = stripSection5(input);
    assert.ok(tickersRemaining < 5, `expected <5 tickers, got ${tickersRemaining}`);
    assert.equal(status, 'success');
  });

  it('ignores "## 5." headings that are not "## 5. Jun Review Only"', () => {
    const input = [
      '## 5. Appendix (not review)',
      'Keep me.',
      '## 6. Next',
      'Also keep.',
    ].join('\n');
    const { stripped } = stripSection5(input);
    assert.match(stripped, /Keep me/);
    assert.match(stripped, /Also keep/);
  });

  it('matches "## 5. Jun Review Only" with trailing text (defense-in-depth)', () => {
    const input = [
      '## 1. Macro',
      'A',
      '## 5. Jun Review Only (CONFIDENTIAL)',
      '$AAPL',
      '## 6. End',
      'keep',
    ].join('\n');
    const { stripped } = stripSection5(input);
    assert.doesNotMatch(stripped, /Jun Review Only/);
    assert.doesNotMatch(stripped, /\$AAPL/);
    assert.match(stripped, /## 6\. End/);
    assert.match(stripped, /keep/);
  });

  it('returns an effectively empty string for empty input', () => {
    const { stripped, tickersRemaining, status } = stripSection5('');
    assert.equal(stripped.trim(), '');
    assert.equal(tickersRemaining, 0);
    assert.equal(status, 'success');
  });

  it('throws TypeError for non-string input', () => {
    assert.throws(() => stripSection5(null), TypeError);
    assert.throws(() => stripSection5(42), TypeError);
    assert.throws(() => stripSection5(undefined), TypeError);
    assert.throws(() => stripSection5({}), TypeError);
  });
});
