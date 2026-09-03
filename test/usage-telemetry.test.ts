import { test } from "node:test";
import assert from "node:assert/strict";
import { parseOmnirouteTelemetryLine, extractOmnirouteTelemetry } from "../src/tools/usage-telemetry.ts";

test("parseOmnirouteTelemetryLine parses a full comment line", () => {
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-response-cost=0.0000190400"),
    { responseCost: 0.00001904 },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-tokens-in=88"),
    { tokensIn: 88 },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-tokens-out=13"),
    { tokensOut: 13 },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-model=deepseek-v4-flash"),
    { model: "deepseek-v4-flash" },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-provider=opencode-go"),
    { provider: "opencode-go" },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-cache-hit=false"),
    { cacheHit: false },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-latency-ms=1161"),
    { latencyMs: 1161 },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-tokens-per-second=80.5"),
    { tokensPerSecond: 80.5 },
  );
  assert.deepEqual(
    parseOmnirouteTelemetryLine(": x-omniroute-ttft-ms=300"),
    { ttftMs: 300 },
  );
});

test("parseOmnirouteTelemetryLine does not invent tok/s from latency", () => {
  assert.deepEqual(parseOmnirouteTelemetryLine(": x-omniroute-latency-ms=2000"), { latencyMs: 2000 });
  assert.deepEqual(parseOmnirouteTelemetryLine(": x-omniroute-tokens-per-second=0"), {});
});

test("parseOmnirouteTelemetryLine returns null for non-comment lines", () => {
  assert.equal(parseOmnirouteTelemetryLine("data: {\"choices\":[]}"), null);
  assert.equal(parseOmnirouteTelemetryLine("data: [DONE]"), null);
  assert.equal(parseOmnirouteTelemetryLine(""), null);
  assert.equal(parseOmnirouteTelemetryLine(": x-omniroute-route-class=standard"), null); // unknown key ignored
});

test("parseOmnirouteTelemetryLine tolerates NaN and empty values", () => {
  assert.deepEqual(parseOmnirouteTelemetryLine(": x-omniroute-response-cost=abc"), {});
  assert.deepEqual(parseOmnirouteTelemetryLine(": x-omniroute-latency-ms="), {});
});

test("extractOmnirouteTelemetry merges multiple lines", () => {
  const text = [
    "data: {\"choices\":[]}",
    ": x-omniroute-cache-hit=false",
    ": x-omniroute-latency-ms=1161",
    ": x-omniroute-response-cost=0.0000190400",
    ": x-omniroute-tokens-in=88",
    ": x-omniroute-tokens-out=13",
    "data: [DONE]",
  ].join("\n");
  assert.deepEqual(extractOmnirouteTelemetry(text), {
    cacheHit: false,
    latencyMs: 1161,
    responseCost: 0.00001904,
    tokensIn: 88,
    tokensOut: 13,
  });
});

test("extractOmnirouteTelemetry handles CRLF line endings", () => {
  const text = [
    "data: {\"choices\":[]}",
    ": x-omniroute-cache-hit=true",
    ": x-omniroute-response-cost=0.0000190400",
    "data: [DONE]",
  ].join("\r\n");
  assert.deepEqual(extractOmnirouteTelemetry(text), {
    cacheHit: true,
    responseCost: 0.00001904,
  });
});

test("extractOmnirouteTelemetry returns undefined when no telemetry", () => {
  assert.equal(extractOmnirouteTelemetry("data: [DONE]"), undefined);
  assert.equal(extractOmnirouteTelemetry(""), undefined);
});
