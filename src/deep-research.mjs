/**
 * @file Gemini Enterprise Deep Research Agent caller (Phase C).
 *
 * Calls `discoveryengine.googleapis.com/{v1alpha|v1beta}/...:streamAssist`
 * with `agentId='deep_research'` and Magi BigQuery-backed data stores as
 * the grounding corpus. This is the production replacement for the Phase B
 * Vertex AI Gemini `generateDeepBrief()` fallback.
 *
 * Design reference: `MAGI-GE-DESIGN-001-v2` §3.1, §5.5, §8, §11.
 *
 * Notes on the Node SDK:
 *   - `@google-cloud/discoveryengine` v2.x exposes `AssistantServiceClient`
 *     for the `v1` `streamAssist` API, but that surface does not include
 *     `agentsSpec` / `answerGenerationMode` required for the Deep Research
 *     agent. Therefore this module uses `google-auth-library` for access
 *     tokens and the standard `fetch` API to talk to the v1alpha REST
 *     endpoint directly, while still depending on `@google-cloud/discoveryengine`
 *     for the generated resource-path conventions.
 */

import { GoogleAuth } from 'google-auth-library';
import { buildPrompt } from './fallback.mjs';

export const DEFAULT_PROJECT =
  process.env.GOOGLE_CLOUD_PROJECT || 'screen-share-459802';
export const DEFAULT_LOCATION = process.env.GE_LOCATION || 'global';
export const DEFAULT_API_VERSION = process.env.GE_API_VERSION || 'v1alpha';
export const DEFAULT_AGENT_ID = process.env.GE_AGENT_ID || 'deep_research';
export const DEFAULT_START_RESEARCH_QUERY = 'Start Research';

/**
 * The OAuth scope needed for all Discovery Engine / Gemini Enterprise
 * `streamAssist` calls.
 */
const DISCOVERY_ENGINE_SCOPE = 'https://www.googleapis.com/auth/cloud-platform';

/**
 * Default generation config. Kept conservative (low temperature, large ceiling)
 * because the downstream pipeline expects a deterministic five-section
 * markdown brief.
 */
export const DEFAULT_GENERATION_CONFIG = Object.freeze({
  temperature: 0.3,
  maxOutputTokens: 16384,
});

/**
 * Create a `GoogleAuth` instance configured for Discovery Engine REST calls.
 *
 * Priority:
 *   1. `credentials` object passed in (intended for tests and for callers
 *      that already have a service-account JSON in memory).
 *   2. `GOOGLE_APPLICATION_CREDENTIALS_JSON` env var (JSON string).
 *   3. `GOOGLE_APPLICATION_CREDENTIALS` file path (standard ADC file).
 *   4. Application Default Credentials (Cloud Run / GCE / gcloud).
 *
 * @param {Object} [opts]
 * @param {string} [opts.projectId]
 * @param {Object} [opts.credentials] - google-auth-library `Credentials` object.
 * @returns {GoogleAuth}
 */
export function createDefaultClient({
  projectId = DEFAULT_PROJECT,
  credentials,
} = {}) {
  const envCredentials = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
  const parsedEnv = envCredentials ? JSON.parse(envCredentials) : undefined;

  return new GoogleAuth({
    projectId,
    credentials: credentials ?? parsedEnv,
    scopes: [DISCOVERY_ENGINE_SCOPE],
  });
}

/**
 * Build the canonical Discovery Engine assistant resource name and the
 * `streamAssist` URL.
 *
 * @param {Object} opts
 * @param {string} opts.projectId
 * @param {string} opts.location
 * @param {string} opts.apiVersion
 * @param {string} opts.engineId
 * @returns {{ name: string, url: string }}
 */
export function buildAssistantUrl({
  projectId,
  location,
  apiVersion,
  engineId,
}) {
  const collection = 'default_collection';
  const base =
    location === 'global'
      ? `https://discoveryengine.googleapis.com/${apiVersion}`
      : `https://${location}-discoveryengine.googleapis.com/${apiVersion}`;
  const name =
    `projects/${projectId}/locations/${location}/collections/${collection}/` +
    `engines/${engineId}/assistants/default_assistant`;
  const url = `${base}/${name}:streamAssist`;
  return { name, url };
}

/**
 * Build a `toolsSpec` that grounds the assistant in the configured Magi
 * data stores. If `dataStoreIds` is omitted, the assistant falls back to
 * whatever data stores are already attached to the engine (and to Google
 * Search if the engine has it enabled).
 *
 * @param {Object} opts
 * @param {string} opts.projectId
 * @param {string} opts.location
 * @param {string} [opts.dataStoreIds] - Comma-separated data store IDs.
 * @returns {Object|null}
 */
export function buildToolsSpec({ projectId, location, dataStoreIds }) {
  const tools = {
    webGroundingSpec: {},
  };

  if (dataStoreIds && dataStoreIds.trim() !== '') {
    const ids = dataStoreIds.split(',').map((s) => s.trim()).filter(Boolean);
    if (ids.length > 0) {
      const prefix =
        `projects/${projectId}/locations/${location}/collections/default_collection/datastores/`;
      tools.vertexAiSearchSpec = {
        dataStoreSpecs: ids.map((id) => ({ dataStore: `${prefix}${id}` })),
      };
    }
  }

  return tools;
}

/**
 * Build the request body for a `streamAssist` call. Exposed as a named export
 * so `test/deep-research.test.mjs` can assert structural invariants without
 * invoking the network.
 *
 * @param {Object} opts
 * @param {string} opts.queryText
 * @param {string} [opts.session] - For the continuation / "Start Research" call.
 * @param {string} opts.agentId
 * @param {Object} opts.toolsSpec
 * @param {Object} [opts.generationConfig]
 * @returns {Object}
 */
export function buildStreamAssistRequest({
  queryText,
  session,
  agentId,
  toolsSpec,
  generationConfig = DEFAULT_GENERATION_CONFIG,
}) {
  const body = {
    query: { text: queryText },
    agentsSpec: {
      agentSpecs: { agentId },
    },
    toolsSpec,
    generationSpec: generationConfig,
  };

  if (session) {
    body.session = session;
  }

  return body;
}

/**
 * Call `streamAssist` once and consume the response stream.
 *
 * The response is a stream of JSON objects (NDJSON or SSE-flavored). We
 * parse incrementally and extract:
 *   - `sessionInfo.session` (first call)
 *   - all `groundedContent.content.text` fragments from `answer.replies`
 *
 * @param {Object} opts
 * @param {string} opts.url
 * @param {Object} opts.body
 * @param {GoogleAuth} opts.auth
 * @param {AbortSignal} [opts.signal]
 * @param {typeof fetch} [opts.fetch] - DI seam for tests.
 * @returns {Promise<{ text: string, session: string|null, rawChunks: unknown[] }>}
 */
export async function callStreamAssist({
  url,
  body,
  auth,
  signal,
  fetch: fetchImpl = globalThis.fetch,
}) {
  const client = auth;
  const tokenResponse = await client.getAccessToken();
  const token =
    typeof tokenResponse === 'string'
      ? tokenResponse
      : tokenResponse?.token ?? tokenResponse?.access_token;
  if (!token) {
    throw new Error('deep-research.mjs: failed to obtain access token');
  }

  const response = await fetchImpl(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!response.ok || !response.body) {
    const text = await response.text();
    let message = `streamAssist returned ${response.status}`;
    try {
      const parsed = JSON.parse(text);
      const err = Array.isArray(parsed) ? parsed[0]?.error : parsed?.error;
      if (err?.message) {
        message = `streamAssist ${response.status}: ${err.message}`;
      }
    } catch {
      // ignore
    }
    throw new Error(message);
  }

  const result = await consumeStream(response.body, { signal });
  return result;
}

/**
 * Consume a streaming `streamAssist` response body.
 *
 * Handles:
 *   - NDJSON (one JSON object per line)
 *   - SSE (`data: {...}` per line)
 *   - A single JSON array returned at the end of the stream
 *   - Chunked UTF-8 split across multiple `read()` calls
 *
 * @param {ReadableStream} body
 * @param {Object} [opts]
 * @param {AbortSignal} [opts.signal]
 * @returns {Promise<{ text: string, session: string|null, rawChunks: unknown[] }>}
 */
export async function consumeStream(body, { signal } = {}) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const rawChunks = [];
  const textParts = [];
  let session = null;
  let buffer = '';

  if (signal) {
    signal.addEventListener('abort', () => reader.cancel(), { once: true });
  }

  while (true) {
    if (signal?.aborted) {
      throw new Error('deep-research.mjs: stream aborted');
    }
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() ?? '';
    for (const raw of lines) {
      const obj = parseStreamLine(raw);
      if (!obj) continue;
      rawChunks.push(obj);
      if (obj.sessionInfo?.session) {
        session = obj.sessionInfo.session;
      }
      extractTextFromStreamObject(obj, textParts);
    }
  }

  // Flush any final partial line.
  if (buffer.trim() !== '') {
    const obj = parseStreamLine(buffer);
    if (obj) {
      rawChunks.push(obj);
      if (obj.sessionInfo?.session) {
        session = obj.sessionInfo.session;
      }
      extractTextFromStreamObject(obj, textParts);
    }
  }

  // If the server returned a single JSON array (used for some error
  // payloads), each element is a stream event.
  if (rawChunks.length === 1 && Array.isArray(rawChunks[0])) {
    const arr = rawChunks[0];
    rawChunks.length = 0;
    rawChunks.push(...arr);
    for (const obj of arr) {
      if (obj?.sessionInfo?.session) {
        session = obj.sessionInfo.session;
      }
      extractTextFromStreamObject(obj, textParts);
    }
  }

  return {
    text: textParts.join('\n'),
    session,
    rawChunks,
  };
}

/**
 * Parse a single stream line into a JSON object, stripping SSE `data:`
 * prefix and handling empty lines.
 *
 * @param {string} line
 * @returns {unknown|null}
 */
function parseStreamLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return null;
  let text = trimmed;
  if (text.startsWith('data:')) {
    text = text.slice(5).trim();
  }
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

/**
 * Extract `groundedContent.content.text` fragments from a single
 * `StreamAssistResponse` object and append them to `textParts`.
 *
 * @param {unknown} obj
 * @param {string[]} textParts
 */
function extractTextFromStreamObject(obj, textParts) {
  if (!obj || typeof obj !== 'object') return;

  const answer = obj.answer;
  if (!answer || typeof answer !== 'object') return;

  for (const reply of answer.replies ?? []) {
    if (!reply || typeof reply !== 'object') continue;
    const content = reply.groundedContent?.content;
    if (
      content &&
      typeof content === 'object' &&
      typeof content.text === 'string' &&
      content.text.trim()
    ) {
      // Include all substantive text by default. Research-plan text is
      // useful for reasoning, but the final report is the main payload.
      textParts.push(content.text);
    }
  }
}

/**
 * Generate the morning Deep Research brief via the Gemini Enterprise Deep
 * Research Agent.
 *
 * This is the Phase C producer that replaces `fallback.mjs` once the
 * allowlist and data-store setup are complete. It performs the two-step
 * `streamAssist` dance documented for the Deep Research agent:
 *   1. Send the prompt and receive a session + research plan.
 *   2. Send `Start Research` in that session to stream the final report.
 *
 * @param {Object} [opts]
 * @param {string} [opts.dateIso] - ET calendar date for the brief.
 * @param {string[]} [opts.tickerUniverse] - Section 5 ticker universe.
 * @param {string} [opts.engineId=process.env.GE_ENGINE_ID]
 * @param {string} [opts.dataStoreIds=process.env.GE_DATA_STORE_IDS]
 * @param {string} [opts.projectId=DEFAULT_PROJECT]
 * @param {string} [opts.location=DEFAULT_LOCATION]
 * @param {string} [opts.apiVersion=DEFAULT_API_VERSION]
 * @param {string} [opts.agentId=DEFAULT_AGENT_ID]
 * @param {string} [opts.query] - Optional override query. Defaults to the
 *   five-section morning-brief prompt from `fallback.mjs`.
 * @param {string} [opts.promptVersion='v2.0']
 * @param {Object} [opts.generationConfig]
 * @param {GoogleAuth} [opts.auth] - Dependency-injected auth client.
 * @param {typeof fetch} [opts.fetch] - Dependency-injected fetch.
 * @param {AbortSignal} [opts.signal]
 * @returns {Promise<{ markdown: string, sessionId: string|null, searchQueryCount: number|null, estimatedCostUsd: number|null }>}
 */
export async function generateDeepResearch({
  dateIso,
  tickerUniverse,
  engineId = process.env.GE_ENGINE_ID,
  dataStoreIds = process.env.GE_DATA_STORE_IDS,
  projectId = DEFAULT_PROJECT,
  location = DEFAULT_LOCATION,
  apiVersion = DEFAULT_API_VERSION,
  agentId = DEFAULT_AGENT_ID,
  query,
  promptVersion = process.env.MAGI_PROMPT_VERSION ?? 'v2.0',
  generationConfig = DEFAULT_GENERATION_CONFIG,
  auth,
  fetch: fetchImpl,
  signal,
} = {}) {
  if (!engineId) {
    throw new Error(
      'deep-research.mjs: GE_ENGINE_ID is required to call streamAssist',
    );
  }

  const prompt =
    query ?? buildPrompt({ dateIso, tickerUniverse });

  const authClient = auth ?? createDefaultClient({ projectId });
  const { url } = buildAssistantUrl({
    projectId,
    location,
    apiVersion,
    engineId,
  });
  const toolsSpec = buildToolsSpec({ projectId, location, dataStoreIds });

  // Step 1 — obtain a session and the research plan.
  const initBody = buildStreamAssistRequest({
    queryText: prompt,
    agentId,
    toolsSpec,
    generationConfig,
  });

  const init = await callStreamAssist({
    url,
    body: initBody,
    auth: authClient,
    signal,
    fetch: fetchImpl,
  });

  if (!init.session) {
    throw new Error(
      'deep-research.mjs: streamAssist did not return a sessionId; cannot start research',
    );
  }

  // Step 2 — start the research and stream the final report.
  const startBody = buildStreamAssistRequest({
    queryText: DEFAULT_START_RESEARCH_QUERY,
    session: init.session,
    agentId,
    toolsSpec,
    generationConfig,
  });

  const report = await callStreamAssist({
    url,
    body: startBody,
    auth: authClient,
    signal,
    fetch: fetchImpl,
  });

  const markdown = report.text || init.text;
  if (!markdown || markdown.trim() === '') {
    throw new Error(
      'deep-research.mjs: Deep Research agent returned an empty markdown body',
    );
  }

  return {
    markdown,
    sessionId: init.session,
    searchQueryCount: null,
    estimatedCostUsd: null,
  };
}
