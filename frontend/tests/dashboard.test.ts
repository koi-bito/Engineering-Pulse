import assert from "node:assert/strict";
import test from "node:test";
import { AxiosError, AxiosHeaders } from "axios";
import {
  nextReconnectDelayMs,
  parseLiveFeedMessage,
  prependLiveEvent,
} from "../src/hooks/useWebSocket.ts";
import {
  extractApiErrorMessage,
  getWebSocketUrl,
  groupTicketsByStatus,
} from "../src/lib/api.ts";

test("getWebSocketUrl converts http and https base URLs", () => {
  assert.equal(getWebSocketUrl("/ws/feed", "http://localhost:8000"), "ws://localhost:8000/ws/feed");
  assert.equal(getWebSocketUrl("/ws/feed", "https://api.pulse.dev/"), "wss://api.pulse.dev/ws/feed");
});

test("nextReconnectDelayMs grows exponentially and caps at maxMs", () => {
  assert.equal(nextReconnectDelayMs(0), 1000);
  assert.equal(nextReconnectDelayMs(1), 2000);
  assert.equal(nextReconnectDelayMs(4), 16000);
  assert.equal(nextReconnectDelayMs(10), 30000);
});

test("prependLiveEvent prepends newest first and enforces 50-item cap", () => {
  const items = Array.from({ length: 55 }, (_, i) => ({
    type: "event" as const,
    timestamp: `2026-11-12T00:00:${String(i).padStart(2, "0")}Z`,
    summary: `evt-${i}`,
    data: {},
  }));

  let acc: typeof items = [];
  for (const item of items) {
    acc = prependLiveEvent(acc, item, 50);
  }
  assert.equal(acc.length, 50);
  assert.equal(acc[0]?.summary, "evt-54");
  assert.equal(acc[49]?.summary, "evt-5");
});

test("parseLiveFeedMessage handles event, alert, and invalid JSON", () => {
  assert.equal(parseLiveFeedMessage("{bad"), null);

  const evt = parseLiveFeedMessage(
    JSON.stringify({
      type: "event",
      source: "github",
      event_type: "pull_request",
      data: { summary: "Merged PR #12", timestamp: "2026-11-12T10:00:00Z" },
    }),
  );
  assert.equal(evt?.type, "event");
  assert.equal(evt?.summary, "Merged PR #12");
  assert.equal(evt?.timestamp, "2026-11-12T10:00:00Z");

  const alert = parseLiveFeedMessage(
    JSON.stringify({
      type: "alert",
      severity: "critical",
      timestamp: "2026-11-12T10:05:00Z",
      data: { metric: "cycle_time" },
    }),
  );
  assert.equal(alert?.type, "alert");
  assert.equal(alert?.severity, "critical");
});

test("groupTicketsByStatus preserves canonical order and dynamic statuses", () => {
  const columns = groupTicketsByStatus(
    [
      {
        id: "ENG-2",
        project: "ENG",
        assignee: null,
        summary: "In review",
        status: "Code Review",
        created_at: "2026-11-12T08:00:00Z",
        resolved_at: null,
        cycle_time_hours: null,
      },
      {
        id: "ENG-1",
        project: "ENG",
        assignee: "alice",
        summary: "Done item",
        status: "Done",
        created_at: "2026-11-12T07:00:00Z",
        resolved_at: "2026-11-12T09:00:00Z",
        cycle_time_hours: 2,
      },
    ],
    { Done: 1, "Code Review": 1 },
  );

  assert.deepEqual(
    columns.map((c) => c.status),
    ["To Do", "In Progress", "Done", "Code Review"],
  );
  assert.equal(columns[2]?.tickets[0]?.id, "ENG-1");
  assert.equal(columns[3]?.tickets[0]?.id, "ENG-2");
});

test("extractApiErrorMessage formats backend detail and retry_after", () => {
  const err = new AxiosError(
    "Too Many Requests",
    "ERR_BAD_REQUEST",
    undefined,
    undefined,
    {
      status: 429,
      statusText: "Too Many Requests",
      headers: new AxiosHeaders(),
      config: { headers: new AxiosHeaders() },
      data: { error: "rate_limit_exceeded", detail: "100 req/min exceeded", retry_after: 42 },
    },
  );
  assert.equal(extractApiErrorMessage(err), "100 req/min exceeded (retry after 42s)");
});
