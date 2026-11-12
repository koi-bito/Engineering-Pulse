import { useEffect, useRef, useState } from "react";
import { getWebSocketUrl } from "../lib/api.ts";

export type WebSocketStatus = "idle" | "connecting" | "open" | "closed";

export interface LiveFeedEvent {
  type: "event" | "alert";
  source?: "github" | "jira" | string;
  event_type?: string;
  severity?: string;
  summary?: string;
  timestamp: string;
  data: Record<string, unknown>;
}

export interface UseWebSocketOptions {
  url?: string;
  enabled?: boolean;
  token?: string | null;
  refreshToken?: () => Promise<string | null>;
  onAuthExpired?: () => void;
  maxEvents?: number;
}

const DEFAULT_MAX_EVENTS = 50;
const BASE_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30_000;
const WS_AUTH_FAILED_CODE = 4401;

export function nextReconnectDelayMs(
  attempt: number,
  baseMs = BASE_BACKOFF_MS,
  maxMs = MAX_BACKOFF_MS,
): number {
  const safeAttempt = Math.max(0, attempt);
  return Math.min(maxMs, baseMs * 2 ** safeAttempt);
}

export function prependLiveEvent(
  prev: LiveFeedEvent[],
  incoming: LiveFeedEvent,
  maxItems = DEFAULT_MAX_EVENTS,
): LiveFeedEvent[] {
  return [incoming, ...prev].slice(0, maxItems);
}

export function parseLiveFeedMessage(raw: string): LiveFeedEvent | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return null;
  }
  const obj = parsed as Record<string, unknown>;
  const msgType = obj.type === "alert" ? "alert" : "event";
  const rawData =
    obj.data && typeof obj.data === "object" && !Array.isArray(obj.data)
      ? (obj.data as Record<string, unknown>)
      : {};
  const timestamp =
    typeof obj.timestamp === "string"
      ? obj.timestamp
      : typeof rawData.timestamp === "string"
        ? rawData.timestamp
        : new Date().toISOString();
  const summary =
    typeof obj.summary === "string"
      ? obj.summary
      : typeof rawData.summary === "string"
        ? rawData.summary
        : undefined;

  return {
    type: msgType,
    source: typeof obj.source === "string" ? obj.source : undefined,
    event_type: typeof obj.event_type === "string" ? obj.event_type : undefined,
    severity: typeof obj.severity === "string" ? obj.severity : undefined,
    summary,
    timestamp,
    data: rawData,
  };
}

export function useWebSocket(options: UseWebSocketOptions = {}) {
  const {
    url = getWebSocketUrl("/ws/feed"),
    enabled = true,
    token = null,
    refreshToken,
    onAuthExpired,
    maxEvents = DEFAULT_MAX_EVENTS,
  } = options;

  const [status, setStatus] = useState<WebSocketStatus>(enabled ? "connecting" : "idle");
  const [events, setEvents] = useState<LiveFeedEvent[]>([]);

  const tokenRef = useRef<string | null>(token);
  const refreshRef = useRef(refreshToken);
  const authExpiredRef = useRef(onAuthExpired);
  tokenRef.current = token;
  refreshRef.current = refreshToken;
  authExpiredRef.current = onAuthExpired;

  useEffect(() => {
    if (!enabled) {
      setStatus("idle");
      return;
    }

    let disposed = false;
    let attempt = 0;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | null = null;

    const clearTimer = () => {
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    const connect = () => {
      if (disposed) return;
      setStatus("connecting");

      const ws = new WebSocket(url);
      socket = ws;

      ws.onopen = () => {
        if (disposed) {
          ws.close();
          return;
        }
        attempt = 0;
        setStatus("open");
        if (tokenRef.current) {
          ws.send(JSON.stringify({ type: "auth", token: tokenRef.current }));
        }
      };

      ws.onmessage = (event) => {
        if (disposed || typeof event.data !== "string") return;
        const parsed = parseLiveFeedMessage(event.data);
        if (parsed) {
          setEvents((prev) => prependLiveEvent(prev, parsed, maxEvents));
        }
      };

      ws.onerror = () => {
        // Browser fires onclose after onerror; reconnect is scheduled there.
      };

      ws.onclose = (closeEvent) => {
        if (disposed) return;
        setStatus("closed");

        // Avoid infinite 4401 loops when no token refresher is configured.
        if (closeEvent.code === WS_AUTH_FAILED_CODE && !refreshRef.current) {
          authExpiredRef.current?.();
          return;
        }

        const delay = nextReconnectDelayMs(attempt);
        attempt += 1;
        reconnectTimer = window.setTimeout(async () => {
          if (disposed) return;
          if (refreshRef.current) {
            try {
              const freshToken = await refreshRef.current();
              if (!freshToken) {
                setStatus("closed");
                authExpiredRef.current?.();
                return;
              }
              tokenRef.current = freshToken;
            } catch {
              setStatus("closed");
              authExpiredRef.current?.();
              return;
            }
          }
          connect();
        }, delay);
      };
    };

    connect();

    return () => {
      disposed = true;
      clearTimer();
      if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
        socket.close(1000, "client unmount");
      }
    };
  }, [url, enabled, maxEvents]);

  return { status, events };
}
