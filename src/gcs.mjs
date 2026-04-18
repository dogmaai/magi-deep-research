/**
 * @file GCS raw-envelope fan-out for Deep Research.
 *
 * Writes the raw (pre-strip) JSON envelope returned by Gemini
 * Enterprise / the Vertex fallback to `gs://magi-deep-research-raw/`.
 * This bucket holds the full brief including Section 5 — it is the
 * PLM-isolated side of the design §2.3 boundary and only Jun has
 * read access.
 *
 * Contract (design §5.5, §7.3):
 *   Object path:      `{prefix}{date}.json`          (defaults to `raw/`)
 *   Object content:   UTF-8 encoded `JSON.stringify(envelope)`.
 *   Content-Type:     `application/json`.
 *   Metadata.status:  `success | partial | failed`.
 *   URI returned:     `gs://{bucket}/{prefix}{date}.json`.
 *
 * This module deliberately does NOT strip Section 5 — the Bucket is
 * the "raw" archive; stripping happens only on the path to
 * BigQuery / PLM via `stripSection5()` in `src/strip.mjs`.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §3.1, §5.5, §7.3.
 */

import { Storage } from '@google-cloud/storage';

export const DEFAULT_PROJECT =
  process.env.GOOGLE_CLOUD_PROJECT || 'screen-share-459802';
export const DEFAULT_BUCKET =
  process.env.DEEP_RESEARCH_RAW_BUCKET || 'magi-deep-research-raw';
export const DEFAULT_PREFIX = 'raw/';

/**
 * Lazy default client factory. Exported as a seam so unit tests
 * never construct a real `Storage` client (which would attempt ADC
 * at call time, not import time).
 *
 * @param {Object} [opts]
 * @param {string} [opts.projectId]
 * @returns {Storage}
 */
export function createDefaultClient({ projectId = DEFAULT_PROJECT } = {}) {
  return new Storage({ projectId });
}

/**
 * Compose the canonical object name for a given ET calendar date.
 *
 * @param {string} date    - YYYY-MM-DD (ET). Enforced by the caller.
 * @param {string} prefix  - Object prefix, must end with `/` or be `''`.
 * @returns {string}
 */
export function objectNameFor(date, prefix = DEFAULT_PREFIX) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new TypeError(
      `gcs.mjs: date must be YYYY-MM-DD (got ${JSON.stringify(date)})`,
    );
  }
  if (prefix !== '' && !prefix.endsWith('/')) {
    throw new TypeError(
      `gcs.mjs: prefix must end with "/" or be empty (got ${JSON.stringify(prefix)})`,
    );
  }
  return `${prefix}${date}.json`;
}

/**
 * Build the `gs://` URI for a given bucket / object name.
 *
 * @param {string} bucket
 * @param {string} objectName
 * @returns {string}
 */
export function gsUriFor(bucket, objectName) {
  return `gs://${bucket}/${objectName}`;
}

/**
 * Upload a raw envelope (JSON-serialisable value) to GCS.
 *
 * Single attempt with optional retry. The `@google-cloud/storage`
 * SDK already performs its own idempotent retry inside
 * `file.save()`, so we only add an outer retry for transport-level
 * exceptions that escape the SDK (e.g. DNS blips during Cloud Run
 * cold start).
 *
 * @param {Object} args
 * @param {string} args.date            - YYYY-MM-DD (ET).
 * @param {unknown} args.envelope       - JSON-serialisable payload.
 * @param {'success'|'partial'|'failed'} args.status
 * @param {Object} [args.opts]
 * @param {Storage} [args.opts.gcs]     - Injectable SDK client.
 * @param {string} [args.opts.bucket]   - Overrides DEFAULT_BUCKET.
 * @param {string} [args.opts.prefix]   - Overrides DEFAULT_PREFIX.
 * @param {number} [args.opts.maxRetries=3]
 * @param {number} [args.opts.backoffMs=1000]
 * @param {(ms: number) => Promise<void>} [args.opts.sleep]
 * @param {Record<string, string>} [args.opts.extraMetadata]
 * @returns {Promise<{ ok: boolean, gcsUri: string, objectName: string }>}
 *   `ok` is true on successful upload. On failure after retries,
 *   `ok` is false and `gcsUri` / `objectName` are still returned
 *   (useful for logging) but no object exists.
 */
export async function uploadRawEnvelope({
  date,
  envelope,
  status,
  opts = {},
} = {}) {
  const {
    gcs,
    bucket = DEFAULT_BUCKET,
    prefix = DEFAULT_PREFIX,
    maxRetries = 3,
    backoffMs = 1000,
    sleep = defaultSleep,
    extraMetadata,
  } = opts;

  if (!['success', 'partial', 'failed'].includes(status)) {
    throw new Error(
      `gcs.mjs: status must be success|partial|failed (got ${JSON.stringify(status)})`,
    );
  }
  const objectName = objectNameFor(date, prefix);
  const gcsUri = gsUriFor(bucket, objectName);

  const client = gcs ?? createDefaultClient();
  const file = client.bucket(bucket).file(objectName);

  const body = Buffer.from(JSON.stringify(envelope), 'utf8');
  const metadata = {
    contentType: 'application/json',
    metadata: {
      // Spread extras FIRST so the core keys below always win — we
      // never want a caller to be able to clobber status/date/source
      // via extraMetadata (e.g. marking a failed run as success).
      ...(extraMetadata ?? {}),
      status,
      date,
      source: 'magi-deep-research',
    },
  };

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      await file.save(body, {
        resumable: false,
        metadata,
      });
      console.log(
        `[deep-research-gcs] uploaded ${gcsUri} (attempt ${attempt}/${maxRetries}, status=${status}, bytes=${body.length})`,
      );
      return { ok: true, gcsUri, objectName };
    } catch (err) {
      const payload =
        err && typeof err === 'object' && 'errors' in err
          ? /** @type {{errors: unknown}} */ (err).errors
          : err instanceof Error
            ? { name: err.name, message: err.message }
            : err;
      console.error(
        `[deep-research-gcs] attempt ${attempt}/${maxRetries} threw:`,
        JSON.stringify(payload, replacerSafe),
      );
      if (attempt < maxRetries) {
        await sleep(backoffMs * attempt);
      }
    }
  }
  console.error(
    `[deep-research-gcs] FAILED after ${maxRetries} attempts (${gcsUri}, status=${status})`,
  );
  return { ok: false, gcsUri, objectName };
}

function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function replacerSafe(_key, value) {
  if (value instanceof Error) {
    return { name: value.name, message: value.message, stack: value.stack };
  }
  return value;
}
