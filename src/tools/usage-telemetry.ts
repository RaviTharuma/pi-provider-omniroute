// src/tools/usage-telemetry.ts
// Captures OmniRoute's `X-OmniRoute-*` cost telemetry from streaming chat
// completion responses. OmniRoute appends these as SSE comment lines
// (`: x-omniroute-...`) at the END of the stream body, right before
// `data: [DONE]`. The openai SDK's SSE parser ignores comment lines, so pi-ai
// never sees them — we intercept the byte stream and parse them ourselves.

import { createAssistantMessageEventStream, appendAssistantMessageDiagnostic } from "@earendil-works/pi-ai";
import type { AssistantMessageEventStream, AssistantMessage } from "@earendil-works/pi-ai";

export interface OmnirouteTelemetry {
  responseCost?: number;
  tokensIn?: number;
  tokensOut?: number;
  tokensPerSecond?: number;
  ttftMs?: number;
  model?: string;
  provider?: string;
  cacheHit?: boolean;
  latencyMs?: number;
}

const TELEMETRY_LINE_RE = /^: x-omniroute-([a-z-]+)=(.*)$/;

/** Parses a single SSE comment line; returns null for anything else. */
export function parseOmnirouteTelemetryLine(
  line: string,
): Partial<OmnirouteTelemetry> | null {
  const match = TELEMETRY_LINE_RE.exec(line);
  if (!match) return null;
  const key = match[1];
  const value = match[2];
  if (value === "") return {}; // empty value — recognized telemetry key but no data
  switch (key) {
    case "response-cost": {
      const n = Number(value);
      return Number.isFinite(n) ? { responseCost: n } : {};
    }
    case "tokens-in": {
      const n = Number(value);
      return Number.isFinite(n) ? { tokensIn: n } : {};
    }
    case "tokens-out": {
      const n = Number(value);
      return Number.isFinite(n) ? { tokensOut: n } : {};
    }
    case "latency-ms": {
      const n = Number(value);
      return Number.isFinite(n) ? { latencyMs: n } : {};
    }
    case "tokens-per-second": {
      const n = Number(value);
      return Number.isFinite(n) && n > 0 ? { tokensPerSecond: n } : {};
    }
    case "ttft-ms": {
      const n = Number(value);
      return Number.isFinite(n) && n > 0 ? { ttftMs: n } : {};
    }
    case "model":
      return { model: value };
    case "provider":
      return { provider: value };
    case "cache-hit":
      return { cacheHit: value === "true" };
    default:
      return null; // unknown key — not telemetry we care about
  }
}

/** Extracts merged telemetry from a full decoded text body. */
export function extractOmnirouteTelemetry(
  text: string,
): OmnirouteTelemetry | undefined {
  let result: OmnirouteTelemetry | undefined;
  for (const line of text.split(/\r?\n/)) {
    const parsed = parseOmnirouteTelemetryLine(line);
    if (parsed) {
      result = { ...(result ?? {}), ...parsed };
    }
  }
  return result;
}

export interface TelemetryTransform {
  stream: TransformStream<Uint8Array, Uint8Array>;
  getTelemetry: () => OmnirouteTelemetry | undefined;
}

/** Byte-transparent TransformStream that also parses OmniRoute telemetry lines. */
export function createTelemetryTransformStream(
  onTelemetry?: (t: OmnirouteTelemetry) => void,
): TelemetryTransform {
  let buffer = "";
  let telemetry: OmnirouteTelemetry | undefined;
  const decoder = new TextDecoder();
  const update = (parsed: Partial<OmnirouteTelemetry>) => {
    telemetry = { ...(telemetry ?? {}), ...parsed };
    // Report eagerly (telemetry lines precede `data: [DONE]`, so the consumer
    // sees the final value before the done event) rather than only after the
    // body has fully drained — avoids a race where done is processed before
    // the pipe resolves. Idempotent for callers that just store the value.
    if (onTelemetry && telemetry && Object.keys(telemetry).length > 0) {
      onTelemetry(telemetry);
    }
  };
  const transform = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      controller.enqueue(chunk); // pass bytes through untouched
      buffer += decoder.decode(chunk, { stream: true });
      // process complete lines
      let idx: number;
      while ((idx = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, idx).replace(/\r$/, ""); // tolerate CRLF
        buffer = buffer.slice(idx + 1);
        const parsed = parseOmnirouteTelemetryLine(line);
        if (parsed) update(parsed);
      }
    },
    flush() {
      // writable closed → readable ends naturally; nothing to terminate.
      const parsed = parseOmnirouteTelemetryLine(buffer.replace(/\r$/, "")); // tolerate CRLF without trailing newline
      if (parsed) update(parsed);
      buffer = "";
    },
  });
  return {
    stream: transform,
    getTelemetry: () => telemetry,
  };
}

/** Wraps a fetch impl: pipes response bodies through a telemetry transform. */
export function withOmnirouteFetch(
  fetchImpl: typeof fetch,
  onTelemetry?: (t: OmnirouteTelemetry) => void,
): typeof fetch {
  return async (input: RequestInfo | URL, init?: RequestInit) => {
    const res = await fetchImpl(input, init);
    if (!res.ok || !res.body) return res;
    const { stream, getTelemetry } = createTelemetryTransformStream((t) => onTelemetry?.(t));
    const pipe = res.body.pipeTo(stream.writable);
    // Read the transformed stream; report telemetry once the body is fully consumed.
    const consumed = (async () => {
      await pipe;
      // Idempotent fallback: the eager per-line callback above already reported
      // the final value; this re-reports only if the flush path found something.
      const t = getTelemetry();
      if (t && Object.keys(t).length > 0) onTelemetry?.(t);
    })();
    // Note: rejections from `pipe` (e.g. body aborted mid-stream) are caught here —
    // telemetry stays best-effort and the caller's response stream is unaffected.
    void consumed.catch(() => {});
    return new Response(stream.readable, res);
  };
}

/**
 * Wraps a pi-ai AssistantMessageEventStream so that on completion (`done`)
 * the OmniRoute-reported cost overwrites `message.usage.cost.total` and the
 * full telemetry is attached to `message.diagnostics`. Without telemetry the
 * stream is forwarded untouched.
 */
export function wrapStreamWithCost(
  stream: AssistantMessageEventStream,
  telemetry: OmnirouteTelemetry | undefined | (() => OmnirouteTelemetry | undefined),
): AssistantMessageEventStream {
  const out = createAssistantMessageEventStream();
  const pump = async () => {
    try {
      for await (const event of stream) {
        if (event.type === "done") {
          const t =
            typeof telemetry === "function" ? telemetry() : telemetry;
          if (t && Object.keys(t).length > 0) {
            const message = event.message as AssistantMessage;
            if (t.responseCost !== undefined) {
              message.usage.cost.total = t.responseCost;
            }
            appendAssistantMessageDiagnostic(message, {
              type: "omniroute-telemetry",
              timestamp: Date.now(),
              details: {
                responseCost: t.responseCost,
                tokensIn: t.tokensIn,
                tokensOut: t.tokensOut,
                tokensPerSecond: t.tokensPerSecond,
                ttftMs: t.ttftMs,
                model: t.model,
                provider: t.provider,
                cacheHit: t.cacheHit,
              },
            });
          }
        }
        out.push(event);
      }
    } catch {
      // Best-effort: if the source stream errors mid-way, terminate output.
    } finally {
      out.end();
    }
  };
  void pump();
  return out;
}
