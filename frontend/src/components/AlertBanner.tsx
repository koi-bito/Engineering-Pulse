import type { WorkflowAlert } from "../lib/api.ts";

interface AlertBannerProps {
  alerts: WorkflowAlert[];
  onDismiss?: (id: string) => void;
}

export function AlertBanner({ alerts, onDismiss }: AlertBannerProps) {
  const criticalAlerts = alerts.filter(
    (a) => a.severity === "critical" && a.status !== "resolved",
  );

  if (criticalAlerts.length === 0) return null;

  return (
    <div className="ep-alert-banner">
      <div className="ep-alert-banner-content">
        <span className="ep-alert-badge critical">CRITICAL ALERT</span>
        <div className="ep-alert-list">
          {criticalAlerts.map((alert) => (
            <div key={alert.id} className="ep-alert-item">
              <span>
                <strong>{alert.metric_name}</strong>: {alert.actual_value} (expected ~
                {alert.expected_value})
              </span>
              {alert.group_id && <span className="ep-pill-grouped">Grouped Incident</span>}
              {onDismiss && (
                <button
                  type="button"
                  className="ep-alert-dismiss"
                  onClick={() => onDismiss(alert.id)}
                  aria-label="Dismiss alert"
                >
                  ✕
                </button>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
