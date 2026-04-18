/**
 * @file Phase B fallback Deep Research brief generator.
 *
 * Until the Gemini Enterprise Deep Research Agent allowlist clears
 * (Phase C), MAGI runs the morning brief through a thin Vertex AI
 * Gemini adapter instead. The adapter implements the same output
 * contract the downstream pipeline expects:
 *
 *   - Returns a single Markdown string with **five H2 sections**:
 *       `## 1. Macro`, `## 2. Sector`, `## 3. Risks`, `## 4. Watchlist`,
 *       `## 5. Jun Review Only`.
 *   - Section 5 holds the Jun-only ticker picks (entry / stop / target)
 *     and MUST be removed by `stripSection5()` before the brief reaches
 *     BigQuery or the PLM. This module deliberately **does not** strip
 *     Section 5 itself — the absolute boundary (design §2.3) is
 *     enforced by a single contractual point in `src/strip.mjs`, and
 *     folding it into the generator would violate single-responsibility
 *     and make the strip logic harder to audit.
 *
 * Auth: on Cloud Run the Gemini call uses ADC (the service account's
 * identity is attached to the job at deploy time). Locally, run with
 * `gcloud auth application-default login` or set
 * `GOOGLE_APPLICATION_CREDENTIALS`. The `@google/genai` SDK handles
 * both paths transparently when `vertexai: true`.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §5.5 (Phase B fallback)
 * and §2.3 / §5.3 (absolute boundary).
 */

import { GoogleGenAI } from '@google/genai';

/**
 * Default Vertex AI project / location. The Cloud Run Job
 * environment is expected to set `GOOGLE_CLOUD_PROJECT`, matching the
 * magi-core convention (see `sentiment-monitor.js`). `location:
 * 'global'` mirrors the magi-core settings so the two paths hit the
 * same routing tier.
 */
export const DEFAULT_PROJECT =
  process.env.GOOGLE_CLOUD_PROJECT || 'screen-share-459802';
export const DEFAULT_LOCATION = 'global';

/**
 * Default model. Aligned with magi-core `sentiment-monitor.js` so the
 * fallback and the sentiment path share the same behaviour profile and
 * upstream changes apply uniformly.
 */
export const DEFAULT_MODEL = 'gemini-3-flash-preview';

/**
 * Default generation config. The brief is long-form markdown (~2-3k
 * tokens typical, ~8k tokens pathological), so we keep a generous
 * `maxOutputTokens` ceiling. `temperature: 0.3` matches the
 * sentiment-monitor call site; higher temperatures are not desirable
 * for a brief that feeds a trading pipeline.
 */
export const DEFAULT_GENERATION_CONFIG = Object.freeze({
  temperature: 0.3,
  maxOutputTokens: 16384,
});

/**
 * Build the user prompt. Exposed as a named export so
 * `test/prompt-contract.test.mjs` (Phase A-10, landing in a later PR)
 * can assert structural invariants on it without invoking the SDK.
 *
 * @param {Object} [opts]
 * @param {string} [opts.dateIso] - ISO date (YYYY-MM-DD) the brief is
 *   for. Defaults to today in UTC so the Cloud Run Job (UTC) behaves
 *   deterministically — callers that want ET can override.
 * @param {string[]} [opts.tickerUniverse] - Hint tickers that must be
 *   considered for Section 5 ticker picks. Matches the magi-core
 *   `TARGET_SYMBOLS` list by default so the fallback brief references
 *   the same universe as the sentiment pipeline.
 * @returns {string}
 */
export function buildPrompt({ dateIso, tickerUniverse } = {}) {
  const date = dateIso ?? new Date().toISOString().slice(0, 10);
  const universe = tickerUniverse ?? [
    'AAPL', 'AMZN', 'MSFT', 'META', 'NVDA', 'TSLA', 'GOOGL', 'OXY', 'SPY',
  ];

  // NOTE: the exact H2 headings (e.g. "## 5. Jun Review Only") are
  // load-bearing — `stripSection5()` matches `/^## 5\. Jun Review
  // Only\b/` and `test/prompt-contract.test.mjs` will assert all five
  // section headings round-trip unchanged through the LLM. Do not
  // reword them in this prompt without updating both sites.
  return [
    `You are MAGI's morning market-research analyst. Produce a Deep Research brief for the US equity session on ${date} (ET).`,
    '',
    'OUTPUT FORMAT — VERY STRICT:',
    '',
    '- Markdown with **exactly five top-level (H2) sections**, in this order and with these exact headings:',
    '  - `## 1. Macro`',
    '  - `## 2. Sector`',
    '  - `## 3. Risks`',
    '  - `## 4. Watchlist`',
    '  - `## 5. Jun Review Only`',
    '- Use H3 (`###`) for sub-sections inside any H2 if needed. Do NOT introduce other H2 headings.',
    '- Do not add a preamble, title, or epilogue outside the five sections.',
    '- Language: English. Concise, analyst-report tone. No emojis, no disclaimers, no sign-offs.',
    '',
    'SECTION GUIDELINES:',
    '',
    '`## 1. Macro` — VIX level, regime (CALM / LOW_FEAR / HIGH_FEAR / EXTREME_FEAR / PANIC per MAGI-GE-DESIGN-001-v2 §5.2), yesterday\'s S&P 500 / NASDAQ / DOW closes and drivers, today\'s scheduled economic releases (CPI, FOMC, NFP, PCE, etc.), and a one-sentence overall sentiment (BULLISH / BEARISH / NEUTRAL).',
    '',
    '`## 2. Sector` — sector leaders and laggards from yesterday, rotation narrative, notable factor moves (momentum, quality, small-cap), and earnings concentration for today.',
    '',
    '`## 3. Risks` — the three to five most material risks specific to today\'s session: macro event risk, geopolitical, idiosyncratic earnings, liquidity, sector concentration. Discuss in prose; **do not** list individual ticker symbols here — Section 5 is the only place ticker picks belong.',
    '',
    '`## 4. Watchlist` — general-interest tickers the broader market is watching today (not MAGI\'s picks). Keep to a short bullet list with one-line rationale per ticker. Limit to 3-5 tickers total. Discuss their context in prose; avoid repeating `$TICKER` notation more than necessary.',
    '',
    `\`## 5. Jun Review Only\` — CONFIDENTIAL. Actionable ticker picks for today, drawn from the MAGI universe (${universe.join(', ')}). For each pick provide on one line: ticker, direction (LONG / SHORT), entry price, stop loss, target price, and a one-sentence thesis. Aim for 1-3 picks. This section is stripped before the brief reaches any downstream LLM; everything here is for Jun only.`,
    '',
    'Return ONLY the markdown body with the five H2 sections. No code fences, no JSON wrapper.',
  ].join('\n');
}

/**
 * Extract the plain-text body from a `@google/genai` `generateContent`
 * response. The SDK exposes `.text` as a convenience getter that
 * concatenates all text parts of the first candidate; we wrap it so
 * the rest of this module can treat the result as an opaque string.
 *
 * @param {unknown} response
 * @returns {string}
 */
function extractText(response) {
  // `response.text` is the documented convenience accessor in
  // @google/genai ≥1.0. Defensive fallback: walk candidates[0].content
  // .parts[*].text in case a future SDK version drops the accessor.
  if (response && typeof response === 'object') {
    const t = /** @type {{text?: unknown}} */ (response).text;
    if (typeof t === 'string') return t;
    const candidates = /** @type {{candidates?: unknown}} */ (response).candidates;
    if (Array.isArray(candidates) && candidates[0]) {
      const parts = candidates[0]?.content?.parts;
      if (Array.isArray(parts)) {
        return parts
          .map((p) => (typeof p?.text === 'string' ? p.text : ''))
          .join('');
      }
    }
  }
  throw new Error(
    'generateDeepBrief: Gemini response did not contain a text body',
  );
}

/**
 * Default `@google/genai` client factory. Factored into an exported
 * function so `generateDeepBrief()` can accept a pre-built client for
 * tests (and so the SDK is not constructed at import time — which
 * would require ADC to be available even for unit tests that never
 * call the network).
 *
 * @param {Object} [opts]
 * @param {string} [opts.project]
 * @param {string} [opts.location]
 * @returns {GoogleGenAI}
 */
export function createDefaultClient({
  project = DEFAULT_PROJECT,
  location = DEFAULT_LOCATION,
} = {}) {
  return new GoogleGenAI({ vertexai: true, project, location });
}

/**
 * Generate the fallback Deep Research brief via Vertex AI Gemini.
 *
 * Returns the **raw** markdown including Section 5. The caller is
 * expected to invoke `stripSection5()` (from `src/strip.mjs`) before
 * persisting the brief to BigQuery or surfacing it to the PLM.
 *
 * @param {Object} [opts]
 * @param {string} [opts.dateIso]        - Brief date, YYYY-MM-DD.
 * @param {string[]} [opts.tickerUniverse] - Overrides Section 5 universe.
 * @param {string} [opts.model]          - Model id override.
 * @param {Object} [opts.generationConfig] - Full config override.
 * @param {Object} [opts.ai]             - `@google/genai`-shaped client
 *   exposing `models.generateContent({ model, contents, config })`.
 *   Intended for dependency injection in tests.
 * @param {boolean} [opts.grounding=true] - When true (default), attach
 *   the Google Search grounding tool so the brief references live
 *   market data rather than parametric recall. Disable for unit tests
 *   that want a hermetic call.
 * @returns {Promise<string>}
 * @throws {Error} if the Gemini call fails or returns an empty body.
 */
export async function generateDeepBrief({
  dateIso,
  tickerUniverse,
  model = DEFAULT_MODEL,
  generationConfig = DEFAULT_GENERATION_CONFIG,
  ai,
  grounding = true,
} = {}) {
  const client = ai ?? createDefaultClient();
  const prompt = buildPrompt({ dateIso, tickerUniverse });

  const config = {
    ...generationConfig,
    ...(grounding ? { tools: [{ googleSearch: {} }] } : {}),
  };

  const response = await client.models.generateContent({
    model,
    contents: prompt,
    config,
  });

  const markdown = extractText(response);
  if (!markdown || markdown.trim() === '') {
    throw new Error(
      'generateDeepBrief: Gemini returned an empty markdown body',
    );
  }
  return markdown;
}
