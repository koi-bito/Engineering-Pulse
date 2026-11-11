export type WebSocketStatus = "idle" | "connecting" | "open" | "closed";

export interface LiveFeedEvent {
  type: "event" | "alert";
  source?: "github" | "jira";
  event_type?: string;
  timestamp: string;
  data: Record<string, unknown>;
}

/**
 * WebSocket hook stub — implemented in Phase 1.4.
 */
export function useWebSocket(_url?: string) {
  return {
    status: "idle" as WebSocketStatus,
    events: [] as LiveFeedEvent[],
  };
}

