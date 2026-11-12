import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchActiveSprint, groupTicketsByStatus } from "../lib/api.ts";
import { ErrorBanner, SkeletonBlock } from "./PanelState.tsx";

export function SprintBoard() {
  const [project, setProject] = useState<string>("");

  const sprintQuery = useQuery({
    queryKey: ["sprint", "active", project || "all"],
    queryFn: () => fetchActiveSprint(project || undefined),
    refetchInterval: 60_000,
  });

  const projects = Array.from(
    new Set((sprintQuery.data?.tickets ?? []).map((t) => t.project)),
  ).sort();

  if (sprintQuery.isPending) {
    return (
      <div className="ep-board-grid">
        {Array.from({ length: 3 }).map((_, idx) => (
          <SkeletonBlock key={idx} height={280} />
        ))}
      </div>
    );
  }

  if (sprintQuery.isError) {
    return <ErrorBanner error={sprintQuery.error} onRetry={() => sprintQuery.refetch()} />;
  }

  const columns = groupTicketsByStatus(
    sprintQuery.data.tickets,
    sprintQuery.data.status_counts,
  );

  return (
    <div className="ep-stack">
      <div className="ep-toolbar">
        <span className="ep-muted">
          Showing {sprintQuery.data.tickets.length} recent tickets (project:{" "}
          <code>{sprintQuery.data.project}</code>)
        </span>
        <label className="ep-filter-label">
          Project:
          <select
            className="ep-select"
            value={project}
            onChange={(e) => setProject(e.target.value)}
          >
            <option value="">All projects</option>
            {projects.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="ep-board-grid">
        {columns.map((col) => (
          <section key={col.status} className="ep-card ep-column">
            <div className="ep-card-head">
              <h3>{col.status}</h3>
              <span className="ep-pill">{col.count}</span>
            </div>
            {col.tickets.length === 0 ? (
              <p className="ep-muted">No tickets in this column.</p>
            ) : (
              <ul className="ep-ticket-list">
                {col.tickets.map((ticket) => (
                  <li key={ticket.id} className="ep-ticket-card">
                    <div className="ep-ticket-top">
                      <span className="ep-badge">{ticket.id}</span>
                      <span className="ep-muted">{ticket.assignee ?? "unassigned"}</span>
                    </div>
                    <p className="ep-ticket-summary">{ticket.summary}</p>
                    <span className="ep-ticket-meta">
                      Created {new Date(ticket.created_at).toLocaleDateString()}
                      {ticket.cycle_time_hours !== null
                        ? ` · ${ticket.cycle_time_hours}h`
                        : ""}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </section>
        ))}
      </div>
    </div>
  );
}
