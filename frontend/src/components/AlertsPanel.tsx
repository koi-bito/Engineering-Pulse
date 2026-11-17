import type { WorkflowAlert } from "../lib/api.ts";

interface AlertsPanelProps {
  alerts: WorkflowAlert[];
  isOpen: boolean;
  onClose: () => void;
}

export function AlertsPanel({ alerts, isOpen, onClose }: AlertsPanelProps) {
  if (!isOpen) return null;

  return (
    <div className="ep-alerts-backdrop" onClick={onClose}>
      <aside className="ep-alerts-drawer" onClick={(e) => e.stopPropagation()}>
        <div className="ep-drawer-header">
          <h3>Workflow Alerts ({alerts.length})</h3>
          <button type="button" className="ep-close-btn" onClick={onClose} aria-label="Close alerts">
            ✕
          </button>
        </div>
        <div className="ep-drawer-content">
          {alerts.length === 0 ? (
            <p className="ep-empty-text">No active alerts. All metrics within standard operational bands.</p>
          ) : (
            alerts.map((a) => (
              <div key={a.id} className={`ep-alert-card ${a.severity}`}>
                <div className="ep-alert-card-header">
                  <span className={`ep-alert-tag ${a.severity}`}>{a.severity.toUpperCase()}</span>
                  <span className="ep-alert-metric-title">{a.metric_name}</span>
                  {a.group_id && <span className="ep-pill-grouped">Grouped</span>}
                </div>
                <div className="ep-alert-metrics">
                  <div>
                    Actual: <strong>{a.actual_value}</strong>
                  </div>
                  <div>
                    Expected: <strong>{a.expected_value}</strong>
                  </div>
                </div>
                {a.created_at && (
                  <div className="ep-alert-timestamp">
                    Triggered: {new Date(a.created_at).toLocaleTimeString()}
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </aside>
    </div>
  );
}
