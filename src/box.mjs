/**
 * @file Box full-text fan-out for Deep Research.
 *
 * Writes the stripped markdown brief (Section 5 already removed by
 * `stripSection5()`) as a `.md` file into a designated Box folder so
 * Jun and the team can browse the archive in a human-friendly UI.
 * The raw / pre-strip archive lives in GCS (`src/gcs.mjs`); Box is
 * the shareable full-text side of the fan-out.
 *
 * Contract (design §5.5, §7.3):
 *   File name:     `{YYYY-MM-DD}_deep_research_brief.md`
 *   MIME:          `text/markdown; charset=utf-8`
 *   Parent folder: `process.env.BOX_RESEARCH_FOLDER_ID`, or the
 *                  constructor-supplied id. Must be set for live use.
 *   Description:   `"MAGI Deep Research brief — <date> (ET) — <status>"`
 *
 * Safety invariant (design §2.3):
 *   `markdown` MUST be post-strip. The writer refuses any input
 *   containing `## 5. Jun Review Only` — defense-in-depth behind
 *   `src/bigquery.mjs` and `src/strip.mjs`.
 *
 * Auth: the Box SDK supports several auth modes. We rely on the JWT
 * /Server Auth path (`box-node-sdk` BoxSDK.getPreconfiguredInstance()
 * with the JWT config object), the same path magi-core would use if
 * it ever integrated Box. Actual construction is deferred to a
 * dependency-injection seam so unit tests never touch the network.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §3.1, §5.5, §7.3.
 */

export const DEFAULT_FOLDER_ID = process.env.BOX_RESEARCH_FOLDER_ID ?? null;
export const FILE_EXTENSION = '.md';
export const CONTENT_TYPE = 'text/markdown; charset=utf-8';

/**
 * Compose the canonical Box file name for a given ET calendar date.
 *
 * @param {string} date - YYYY-MM-DD (ET).
 * @returns {string}
 */
export function fileNameFor(date) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new TypeError(
      `box.mjs: date must be YYYY-MM-DD (got ${JSON.stringify(date)})`,
    );
  }
  return `${date}_deep_research_brief${FILE_EXTENSION}`;
}

/**
 * Defense-in-depth check: the markdown must not contain Section 5.
 * Mirrors `src/bigquery.mjs::assertStripped()` — same invariant,
 * enforced at each writer boundary.
 *
 * @param {unknown} markdown
 */
function assertStripped(markdown) {
  if (typeof markdown !== 'string') {
    throw new TypeError(
      'box.mjs: markdown must be a string (stripSection5 output)',
    );
  }
  if (/^## 5\. Jun Review Only\b/m.test(markdown)) {
    throw new Error(
      'box.mjs: markdown still contains "## 5. Jun Review Only". ' +
        'Call stripSection5() before uploadBrief(). ' +
        'This is a MAGI-GE-DESIGN-001-v2 §2.3 absolute-boundary violation.',
    );
  }
}

function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Upload a stripped markdown brief to Box under
 * `folderId`. Single attempt with optional retry; the outer retry
 * covers transport-level exceptions only — business-logic failures
 * (auth, folder not found, quota exceeded) are surfaced to the
 * caller via `{ ok: false, reason }`.
 *
 * The Box SDK client must expose `files.uploadFile(folderId, name,
 * contentBuffer)` → `{ entries: [{ id, name }] } | { id, name }`.
 * This matches the `box-node-sdk` v3.x `BoxClient` surface.
 *
 * @param {Object} args
 * @param {string} args.date            - YYYY-MM-DD (ET).
 * @param {string} args.markdown        - stripSection5 output (REQUIRED post-strip).
 * @param {'success'|'partial'|'failed'} args.status
 * @param {Object} [args.opts]
 * @param {{ files: { uploadFile: Function, update?: Function } }} [args.opts.box]
 *   - Injectable Box client (BoxClient-shaped).
 * @param {string} [args.opts.folderId]  - Overrides DEFAULT_FOLDER_ID.
 * @param {number} [args.opts.maxRetries=3]
 * @param {number} [args.opts.backoffMs=1000]
 * @param {(ms: number) => Promise<void>} [args.opts.sleep=defaultSleep]
 * @returns {Promise<{ok: boolean, fileId: string|null, fileName: string, boxUrl: string|null}>}
 */
export async function uploadBrief({
  date,
  markdown,
  status,
  opts = {},
} = {}) {
  const {
    box,
    folderId = DEFAULT_FOLDER_ID,
    maxRetries = 3,
    backoffMs = 1000,
    sleep = defaultSleep,
  } = opts;

  if (!['success', 'partial', 'failed'].includes(status)) {
    throw new Error(
      `box.mjs: status must be success|partial|failed (got ${JSON.stringify(status)})`,
    );
  }
  assertStripped(markdown);

  const fileName = fileNameFor(date);
  const empty = { ok: false, fileId: null, fileName, boxUrl: null };

  if (!folderId) {
    console.error(
      `[deep-research-box] missing folderId — set BOX_RESEARCH_FOLDER_ID or pass opts.folderId`,
    );
    return empty;
  }
  if (!box || !box.files || typeof box.files.uploadFile !== 'function') {
    console.error(
      '[deep-research-box] missing Box client — pass opts.box (box-node-sdk BoxClient-shaped)',
    );
    return empty;
  }

  const body = Buffer.from(markdown, 'utf8');

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const response = await box.files.uploadFile(folderId, fileName, body);
      const entry = unwrapUploadResponse(response);
      if (!entry || typeof entry.id !== 'string') {
        console.error(
          `[deep-research-box] attempt ${attempt}/${maxRetries} returned unexpected shape:`,
          JSON.stringify(response, replacerSafe),
        );
      } else {
        const fileId = entry.id;
        const boxUrl = `https://app.box.com/file/${fileId}`;
        console.log(
          `[deep-research-box] uploaded ${fileName} to folder ${folderId} (file id ${fileId}, attempt ${attempt}/${maxRetries}, status=${status}, bytes=${body.length})`,
        );
        return { ok: true, fileId, fileName, boxUrl };
      }
    } catch (err) {
      const payload =
        err instanceof Error
          ? { name: err.name, message: err.message }
          : err;
      console.error(
        `[deep-research-box] attempt ${attempt}/${maxRetries} threw:`,
        JSON.stringify(payload, replacerSafe),
      );
    }
    if (attempt < maxRetries) {
      await sleep(backoffMs * attempt);
    }
  }

  console.error(
    `[deep-research-box] FAILED after ${maxRetries} attempts (${fileName}, status=${status})`,
  );
  return empty;
}

/**
 * `box-node-sdk` returns either `{ entries: [entry] }` (when
 * uploading via the Upload API) or the entry directly in some
 * wrappers. Tolerate both shapes.
 *
 * @param {unknown} response
 * @returns {{id: string, name?: string} | null}
 */
function unwrapUploadResponse(response) {
  if (!response || typeof response !== 'object') return null;
  const r = /** @type {Record<string, unknown>} */ (response);
  if (Array.isArray(r.entries) && r.entries[0]) {
    return /** @type {{id: string}} */ (r.entries[0]);
  }
  if (typeof r.id === 'string') return /** @type {{id: string}} */ (r);
  return null;
}

function replacerSafe(_key, value) {
  if (value instanceof Error) {
    return { name: value.name, message: value.message, stack: value.stack };
  }
  return value;
}
