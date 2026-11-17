import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertBanner } from "./components/AlertBanner.tsx";
import { AlertsPanel } from "./components/AlertsPanel.tsx";
import { Header } from "./components/Header.tsx";
import { OverviewPanel } from "./components/OverviewPanel.tsx";
import { SearchPanel } from "./components/SearchPanel.tsx";
import { Sidebar, type Section } from "./components/Sidebar.tsx";
import { SprintBoard } from "./components/SprintBoard.tsx";
import { useWebSocket } from "./hooks/useWebSocket.ts";
import { api, fetchActiveAlerts, fetchTeams, getActiveTeamId, setActiveTeamId } from "./lib/api.ts";
import "./App.css";

interface HealthResponse {
  status: string;
  environment: string;
  timestamp: string;
}

const SECTION_TITLES: Record<Section, string> = {
  overview: "Engineering Overview",
  sprint: "Active Sprint Board",
  search: "Event Search",
};

export function App() {
  const [section, setSection] = useState<Section>("overview");
  const [alertsOpen, setAlertsOpen] = useState(false);
  const queryClient = useQueryClient();
  const [activeTeam, setActiveTeam] = useState<string | null>(() => getActiveTeamId());
  const { status: wsStatus, events } = useWebSocket();

  const healthQuery = useQuery({
    queryKey: ["health"],
    queryFn: async () => {
      const { data } = await api.get<HealthResponse>("/health");
      return data;
    },
    refetchInterval: 60_000,
    retry: false,
  });

  const teamsQuery = useQuery({
    queryKey: ["teams"],
    queryFn: fetchTeams,
    staleTime: 5 * 60_000,
    retry: false,
  });

  const alertsQuery = useQuery({
    queryKey: ["alerts", activeTeam],
    queryFn: fetchActiveAlerts,
    refetchInterval: 30_000,
    retry: false,
  });

  const alerts = alertsQuery.data ?? [];

  const handleSelectTeam = (teamId: string) => {
    const next = teamId || null;
    setActiveTeam(next);
    setActiveTeamId(next);
    queryClient.invalidateQueries();
  };

  const apiHealthy = healthQuery.isPending
    ? null
    : healthQuery.isSuccess && healthQuery.data.status === "ok";

  return (
    <div className="ep-shell">
      <Header
        apiHealthy={apiHealthy}
        environment={healthQuery.data?.environment}
        wsStatus={wsStatus}
        teams={teamsQuery.data ?? []}
        activeTeamId={activeTeam}
        onSelectTeam={handleSelectTeam}
        activeAlertsCount={alerts.length}
        onToggleAlerts={() => setAlertsOpen((prev) => !prev)}
      />
      <AlertBanner alerts={alerts} />
      <div className="ep-body">
        <Sidebar active={section} onSelect={setSection} />
        <main className="ep-main">
          <div className="ep-page-head">
            <h2>{SECTION_TITLES[section]}</h2>
            {healthQuery.data?.timestamp && (
              <span className="ep-muted">
                Last API check:{" "}
                {new Date(healthQuery.data.timestamp).toLocaleTimeString()}
              </span>
            )}
          </div>

          {section === "overview" && (
            <OverviewPanel wsStatus={wsStatus} events={events} />
          )}
          {section === "sprint" && <SprintBoard />}
          {section === "search" && <SearchPanel />}
        </main>
      </div>
      <AlertsPanel
        alerts={alerts}
        isOpen={alertsOpen}
        onClose={() => setAlertsOpen(false)}
      />
    </div>
  );
}

export default App;
