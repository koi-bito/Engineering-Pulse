import { useQuery } from "@tanstack/react-query";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { LiveFeedEvent, WebSocketStatus } from "../hooks/useWebSocket.ts";
import {
  fetchCycleTimeMetrics,
  fetchMetricsSummary,
  fetchPrMetrics,
} from "../lib/api.ts";
import { ErrorBanner, SkeletonBlock } from "./PanelState.tsx";

const AUTO_REFRESH_MS = 60_000;
const TOOLTIP_STYLE = {
  backgroundColor: "#0f172a",
  border: "1px solid #334155",
  borderRadius: "6px",
  color: "#e6ebf5",
};

interface OverviewPanelProps {
  wsStatus: WebSocketStatus;
  events: LiveFeedEvent[];
}

function formatShortDate(isoDate: string): string {
  return isoDate.length >= 10 ? isoDate.slice(5, 10) : isoDate;
}

function formatLocalTimestamp(isoTs: string): string {
  const parsed = new Date(isoTs);
  return Number.isNaN(parsed.getTime()) ? isoTs : parsed.toLocaleTimeString();
}

export function OverviewPanel({ wsStatus, events }: OverviewPanelProps) {
  const summaryQuery = useQuery({
    queryKey: ["metrics", "summary"],
    queryFn: fetchMetricsSummary,
    refetchInterval: AUTO_REFRESH_MS,
  });

  const prsQuery = useQuery({
    queryKey: ["metrics", "prs", "14d"],
    queryFn: () => fetchPrMetrics("14d"),
    refetchInterval: AUTO_REFRESH_MS,
  });

  const cycleTimeQuery = useQuery({
    queryKey: ["metrics", "cycle-time", "30d"],
    queryFn: () => fetchCycleTimeMetrics("30d"),
    refetchInterval: AUTO_REFRESH_MS,
  });

  const summary = summaryQuery.data;

  return (
    <div className="ep-stack">
      {summaryQuery.isPending ? (
        <div className="ep-kpi-grid">
          {Array.from({ length: 5 }).map((_, idx) => (
            <SkeletonBlock key={idx} height={82} />
          ))}
        </div>
      ) : summaryQuery.isError ? (
        <ErrorBanner error={summaryQuery.error} onRetry={() => summaryQuery.refetch()} />
      ) : (
        <div className="ep-kpi-grid">
          <div className="ep-card ep-kpi">
            <span className="ep-kpi-label">PRs Merged Today</span>
            <strong className="ep-kpi-value">{summary?.prs_merged_today ?? 0}</strong>
          </div>
          <div className="ep-card ep-kpi">
            <span className="ep-kpi-label">7d Avg PR Cycle Time</span>
            <strong className="ep-kpi-value">
              {summary?.avg_cycle_time_7d_hours !== null &&
              summary?.avg_cycle_time_7d_hours !== undefined
                ? `${summary.avg_cycle_time_7d_hours}h`
                : "—"}
            </strong>
          </div>
          <div className="ep-card ep-kpi">
            <span className="ep-kpi-label">Open PRs</span>
            <strong className="ep-kpi-value">{summary?.open_prs ?? 0}</strong>
          </div>
          <div className="ep-card ep-kpi">
            <span className="ep-kpi-label">Active Tickets</span>
            <strong className="ep-kpi-value">{summary?.active_tickets ?? 0}</strong>
          </div>
          <div className="ep-card ep-kpi">
            <span className="ep-kpi-label">Commits Today</span>
            <strong className="ep-kpi-value">{summary?.commits_today ?? 0}</strong>
          </div>
        </div>
      )}

      <div className="ep-chart-grid">
        <section className="ep-card">
          <div className="ep-card-head">
            <h3>PRs Merged / Day (Last 14 Days)</h3>
            <span className="ep-muted">Auto-refresh 60s</span>
          </div>
          {prsQuery.isPending ? (
            <SkeletonBlock height={260} />
          ) : prsQuery.isError ? (
            <ErrorBanner error={prsQuery.error} onRetry={() => prsQuery.refetch()} />
          ) : (
            <div className="ep-chart-box">
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={prsQuery.data.series}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatShortDate}
                    stroke="#94a3b8"
                    fontSize={12}
                  />
                  <YAxis allowDecimals={false} stroke="#94a3b8" fontSize={12} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Legend />
                  <Bar
                    dataKey="merged_count"
                    name="Merged"
                    fill="#22c55e"
                    radius={[4, 4, 0, 0]}
                  />
                  <Bar
                    dataKey="opened_count"
                    name="Opened"
                    fill="#3b82f6"
                    radius={[4, 4, 0, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>

        <section className="ep-card">
          <div className="ep-card-head">
            <h3>Ticket Cycle Time (30d Rolling Average)</h3>
            <span className="ep-muted">Hours</span>
          </div>
          {cycleTimeQuery.isPending ? (
            <SkeletonBlock height={260} />
          ) : cycleTimeQuery.isError ? (
            <ErrorBanner
              error={cycleTimeQuery.error}
              onRetry={() => cycleTimeQuery.refetch()}
            />
          ) : (
            <div className="ep-chart-box">
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={cycleTimeQuery.data.series}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatShortDate}
                    stroke="#94a3b8"
                    fontSize={12}
                  />
                  <YAxis stroke="#94a3b8" fontSize={12} unit="h" />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="rolling_avg_hours"
                    name="Rolling Avg (h)"
                    stroke="#f59e0b"
                    strokeWidth={2.5}
                    dot={false}
                    connectNulls
                  />
                  <Line
                    type="monotone"
                    dataKey="avg_ticket_cycle_time_hours"
                    name="Daily Avg (h)"
                    stroke="#38bdf8"
                    strokeWidth={1.5}
                    dot={{ r: 2 }}
                    connectNulls
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </section>
      </div>

      <section className="ep-card">
        <div className="ep-card-head">
          <h3>Live Event Feed</h3>
          <span className="ep-pill" data-state={wsStatus}>
            WS: {wsStatus} · {events.length}/50
          </span>
        </div>
        {events.length === 0 ? (
          <p className="ep-muted">
            Waiting for live events on <code>/ws/feed</code>...
          </p>
        ) : (
          <ul className="ep-feed-list">
            {events.map((evt, idx) => (
              <li
                key={`${evt.timestamp}-${evt.event_type ?? evt.type}-${idx}`}
                className={evt.type === "alert" ? "ep-feed-item alert" : "ep-feed-item"}
              >
                <span className="ep-badge">
                  {evt.source ?? evt.type}
                  {evt.event_type ? `:${evt.event_type}` : ""}
                </span>
                <span className="ep-feed-summary">
                  {evt.summary ?? JSON.stringify(evt.data)}
                </span>
                <span className="ep-feed-time">{formatLocalTimestamp(evt.timestamp)}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}

