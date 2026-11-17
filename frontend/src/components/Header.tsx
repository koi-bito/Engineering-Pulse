import type { WebSocketStatus } from "../hooks/useWebSocket.ts";
import type { Team } from "../lib/api.ts";

interface HeaderProps {
  apiHealthy: boolean | null;
  environment?: string;
  wsStatus: WebSocketStatus;
  teams?: Team[];
  activeTeamId?: string | null;
  onSelectTeam?: (teamId: string) => void;
  activeAlertsCount?: number;
  onToggleAlerts?: () => void;
}

export function Header({
  apiHealthy,
  environment,
  wsStatus,
  teams = [],
  activeTeamId,
  onSelectTeam,
  activeAlertsCount = 0,
  onToggleAlerts,
}: HeaderProps) {
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
        {onToggleAlerts && (
          <button
            type="button"
            className={`ep-alerts-btn ${activeAlertsCount > 0 ? "has-alerts" : ""}`}
            onClick={onToggleAlerts}
            title="View Workflow Alerts"
          >
            ⚠️ {activeAlertsCount} {activeAlertsCount === 1 ? "Alert" : "Alerts"}
          </button>
        )}
        {teams.length > 0 && onSelectTeam && (
          <div className="ep-team-switcher">
            <label htmlFor="team-select" className="ep-team-label">
              Team:
            </label>
            <select
              id="team-select"
              className="ep-team-select"
              value={activeTeamId || ""}
              onChange={(e) => onSelectTeam(e.target.value)}
            >
              <option value="">All Teams</option>
              {teams.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name}
                </option>
              ))}
            </select>
          </div>
        )}
        <span className="ep-pill" data-state={wsStatus}>
          Live Feed: {wsStatus}
        </span>
        <span className="ep-status">{statusLabel}</span>
      </div>
    </header>
  );
}
