import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_PROJECT,
  buildAssistantUrl,
  buildStreamAssistRequest,
  buildToolsSpec,
  callStreamAssist,
  consumeStream,
  generateDeepResearch,
} from '../src/deep-research.mjs';

function makeFakeAuth(token = 'test-token') {
  return {
    getAccessToken: async () => token,
  };
}

function streamFromString(text) {
  const chunks = [];
  const encoder = new TextEncoder();
  for (let i = 0; i < text.length; i += 32) {
    chunks.push(encoder.encode(text.slice(i, i + 32)));
  }
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

function makeFetchStub(initResponse, startResponse) {
  let callIndex = 0;
  return async (_url, _opts) => {
    const response = callIndex === 0 ? initResponse : startResponse;
    callIndex += 1;
    if (response instanceof Error) throw response;
    return response;
  };
}

function makeResponse({ chunks = [], status = 200, statusText = 'OK' }) {
  const body = streamFromString(chunks.map((c) => JSON.stringify(c)).join('\n'));
  return {
    ok: status === 200,
    status,
    statusText,
    body,
    text: async () => 'error body',
  };
}

describe('deep-research.mjs builders', () => {
  it('buildAssistantUrl returns the v1alpha global URL by default', () => {
    const { url, name } = buildAssistantUrl({
      projectId: 'screen-share-459802',
      location: 'global',
      apiVersion: 'v1alpha',
      engineId: 'magi-app',
    });
    assert.equal(
      url,
      'https://discoveryengine.googleapis.com/v1alpha/projects/screen-share-459802/locations/global/collections/default_collection/engines/magi-app/assistants/default_assistant:streamAssist',
    );
    assert.ok(name.includes('engines/magi-app'));
  });

  it('buildAssistantUrl returns a regional URL for non-global locations', () => {
    const { url } = buildAssistantUrl({
      projectId: 'p',
      location: 'us-central1',
      apiVersion: 'v1beta',
      engineId: 'e',
    });
    assert.equal(
      url,
      'https://us-central1-discoveryengine.googleapis.com/v1beta/projects/p/locations/us-central1/collections/default_collection/engines/e/assistants/default_assistant:streamAssist',
    );
  });

  it('buildToolsSpec returns dataStoreSpecs for comma-separated data store IDs', () => {
    const spec = buildToolsSpec({
      projectId: 'p',
      location: 'global',
      dataStoreIds: 'ds1, ds2',
    });
    assert.equal(spec.vertexAiSearchSpec.dataStoreSpecs.length, 2);
    assert.ok(spec.vertexAiSearchSpec.dataStoreSpecs[0].dataStore.includes('ds1'));
    assert.ok(spec.webGroundingSpec);
  });

  it('buildToolsSpec omits vertexAiSearchSpec when no data store IDs are given', () => {
    const spec = buildToolsSpec({
      projectId: 'p',
      location: 'global',
      dataStoreIds: '',
    });
    assert.equal(spec.vertexAiSearchSpec, undefined);
    assert.ok(spec.webGroundingSpec);
  });

  it('buildStreamAssistRequest includes agentId and toolsSpec', () => {
    const req = buildStreamAssistRequest({
      queryText: 'Analyze MAGI thoughts',
      agentId: 'deep_research',
      toolsSpec: { vertexAiSearchSpec: { dataStoreSpecs: [] } },
    });
    assert.equal(req.query.text, 'Analyze MAGI thoughts');
    assert.equal(req.agentsSpec.agentSpecs.agentId, 'deep_research');
    assert.equal(req.generationSpec.temperature, 0.3);
    assert.equal(req.session, undefined);
  });

  it('buildStreamAssistRequest includes session when provided', () => {
    const req = buildStreamAssistRequest({
      queryText: 'Start Research',
      session: 'projects/p/locations/global/sessions/s123',
      agentId: 'deep_research',
      toolsSpec: {},
    });
    assert.equal(req.session, 'projects/p/locations/global/sessions/s123');
  });
});

describe('deep-research.mjs streaming', () => {
  it('consumeStream extracts session and text from NDJSON chunks', async () => {
    const events = [
      {
        answer: {
          state: 'SUCCEEDED',
          replies: [
            {
              groundedContent: {
                content: { role: 'model', text: 'Research plan.' },
                contentMetadata: { contentKind: 'RESEARCH_PLAN' },
              },
            },
          ],
        },
        sessionInfo: { session: 'projects/p/locations/global/sessions/s123' },
      },
      {
        answer: {
          state: 'SUCCEEDED',
          replies: [
            {
              groundedContent: {
                content: { role: 'model', text: 'Final report.' },
              },
            },
          ],
        },
      },
    ];
    const body = streamFromString(events.map((e) => JSON.stringify(e)).join('\n'));
    const result = await consumeStream(body);
    assert.equal(result.session, 'projects/p/locations/global/sessions/s123');
    assert.equal(result.text, 'Research plan.\nFinal report.');
  });

  it('consumeStream handles SSE data: prefix', async () => {
    const events = [
      { sessionInfo: { session: 's1' } },
      {
        answer: {
          replies: [
            { groundedContent: { content: { text: 'Hello from SSE' } } },
          ],
        },
      },
    ];
    const sseText = events.map((e) => `data: ${JSON.stringify(e)}`).join('\n\n');
    const body = streamFromString(sseText);
    const result = await consumeStream(body);
    assert.equal(result.session, 's1');
    assert.equal(result.text, 'Hello from SSE');
  });

  it('consumeStream parses a single JSON-array response', async () => {
    const arr = [
      {
        sessionInfo: { session: 's2' },
        answer: {
          replies: [{ groundedContent: { content: { text: 'One' } } }],
        },
      },
      {
        answer: {
          replies: [{ groundedContent: { content: { text: 'Two' } } }],
        },
      },
    ];
    const body = streamFromString(JSON.stringify(arr));
    const result = await consumeStream(body);
    assert.equal(result.session, 's2');
    assert.equal(result.text, 'One\nTwo');
  });
});

describe('deep-research.mjs callStreamAssist', () => {
  it('throws with server error message when the response is not OK', async () => {
    const fetch = async () => ({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      body: null,
      text: async () =>
        JSON.stringify([{ error: { message: 'license quota exceeded' } }]),
    });
    await assert.rejects(
      callStreamAssist({
        url: 'https://example.com/stream',
        body: {},
        auth: makeFakeAuth(),
        fetch,
      }),
      /license quota exceeded/,
    );
  });
});

describe('deep-research.mjs generateDeepResearch', () => {
  it('performs the two-step streamAssist dance and returns markdown', async () => {
    const initResponse = makeResponse({
      chunks: [
        {
          answer: {
            replies: [
              {
                groundedContent: {
                  content: { text: 'Plan: analyze recent thoughts.' },
                  contentMetadata: { contentKind: 'RESEARCH_PLAN' },
                },
              },
            ],
          },
          sessionInfo: { session: 'projects/p/sessions/s1' },
        },
      ],
    });
    const startResponse = makeResponse({
      chunks: [
        {
          answer: {
            replies: [
              {
                groundedContent: {
                  content: { text: '## 1. Macro\nVIX stable.' },
                },
              },
              {
                groundedContent: {
                  content: { text: '## 5. Jun Review Only\nTicker picks.' },
                },
              },
            ],
          },
        },
      ],
    });

    const fetch = makeFetchStub(initResponse, startResponse);
    const result = await generateDeepResearch({
      dateIso: '2026-04-20',
      engineId: 'magi-app',
      dataStoreIds: 'magicorethought',
      projectId: 'screen-share-459802',
      auth: makeFakeAuth(),
      fetch,
    });

    assert.equal(typeof result.markdown, 'string');
    assert.equal(result.sessionId, 'projects/p/sessions/s1');
    assert.equal(result.searchQueryCount, null);
    assert.equal(result.estimatedCostUsd, null);
    assert.ok(result.markdown.includes('## 1. Macro'));
    assert.ok(result.markdown.includes('## 5. Jun Review Only'));
    // The full raw markdown is preserved; downstream stripSection5 removes §5.
  });

  it('throws if the first call does not return a session', async () => {
    const fetch = makeFetchStub(
      makeResponse({
        chunks: [{ answer: { replies: [] } }],
      }),
      null,
    );
    await assert.rejects(
      generateDeepResearch({
        dateIso: '2026-04-20',
        engineId: 'magi-app',
        projectId: 'screen-share-459802',
        auth: makeFakeAuth(),
        fetch,
      }),
      /sessionId/,
    );
  });

  it('throws if GE_ENGINE_ID is missing', async () => {
    await assert.rejects(
      generateDeepResearch({ dateIso: '2026-04-20', auth: makeFakeAuth() }),
      /GE_ENGINE_ID/,
    );
  });
});
