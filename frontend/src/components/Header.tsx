import type { WebSocketStatus } from "../hooks/useWebSocket.ts";

interface HeaderProps {
  apiHealthy: boolean | null;
  environment?: string;
  wsStatus: WebSocketStatus;
}

export function Header({ apiHealthy, environment, wsStatus }: HeaderProps) {
  const statusLabel =
    apiHealthy === null
      ? "API: checking"
      : apiHealthy
        ? `API: online (${environment ?? "local"})`
        : "API: offline";

  return (
    <header className="ep-header">
      <div className="ep-brand">
        <span className="ep-dot" data-healthy={String(apiHealthy)} />
        <h1>Engineering Pulse</h1>
      </div>
      <div className="ep-header-meta">
        <span className="ep-pill" data-state={wsStatus}>
          Live Feed: {wsStatus}
        </span>
        <span className="ep-status">{statusLabel}</span>
      </div>
    </header>
  );
}
