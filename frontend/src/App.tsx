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
import {
  api,
  extractApiErrorMessage,
  fetchActiveAlerts,
  fetchTeams,
  getActiveTeamId,
  getAuthToken,
  loginUser,
  logoutUser,
  refreshAuthToken,
  setActiveTeamId,
  setAuthToken,
} from "./lib/api.ts";
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
  const [token, setToken] = useState<string | null>(() => getAuthToken());
  const [userEmail, setUserEmail] = useState<string | null>(() => {
    return typeof window !== "undefined" ? localStorage.getItem("ep_user_email") : null;
  });

  const [loginEmail, setLoginEmail] = useState("demo@pulse.io");
  const [loginPassword, setLoginPassword] = useState("demoPassword123!");
  const [loginError, setLoginError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const { status: wsStatus, events } = useWebSocket({
    enabled: Boolean(token),
    token,
    refreshToken: refreshAuthToken,
    onAuthExpired: () => {
      setAuthToken(null);
      setToken(null);
    },
  });

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
    enabled: Boolean(token),
    staleTime: 5 * 60_000,
    retry: false,
  });

  const alertsQuery = useQuery({
    queryKey: ["alerts", activeTeam],
    queryFn: fetchActiveAlerts,
    enabled: Boolean(token),
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

  const handleLogin = async (e?: React.FormEvent, customEmail?: string, customPassword?: string) => {
    if (e) e.preventDefault();
    setLoginError(null);
    setIsSubmitting(true);
    const emailToUse = customEmail || loginEmail;
    const passwordToUse = customPassword || loginPassword;
    try {
      const res = await loginUser(emailToUse, passwordToUse);
      setToken(res.access_token);
      const email = res.user?.email || emailToUse;
      setUserEmail(email);
      if (typeof window !== "undefined") {
        localStorage.setItem("ep_user_email", email);
      }
      queryClient.invalidateQueries();
    } catch (err) {
      setLoginError(extractApiErrorMessage(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleLogout = async () => {
    await logoutUser();
    setToken(null);
    setUserEmail(null);
    if (typeof window !== "undefined") {
      localStorage.removeItem("ep_user_email");
    }
    queryClient.clear();
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
        userEmail={userEmail}
        onLogout={token ? handleLogout : undefined}
      />

      {!token ? (
        <div className="ep-login-container">
          <div className="ep-login-card">
            <div className="ep-login-header">
              <h2>Welcome to Engineering Pulse</h2>
              <p>Sign in to access real-time metrics, cycle times, and sprint analytics.</p>
            </div>
            {loginError && <div className="ep-login-error">{loginError}</div>}
            <form onSubmit={handleLogin} className="ep-login-form">
              <div className="ep-form-group">
                <label htmlFor="login-email">Email</label>
                <input
                  id="login-email"
                  type="email"
                  value={loginEmail}
                  onChange={(e) => setLoginEmail(e.target.value)}
                  required
                  disabled={isSubmitting}
                />
              </div>
              <div className="ep-form-group">
                <label htmlFor="login-password">Password</label>
                <input
                  id="login-password"
                  type="password"
                  value={loginPassword}
                  onChange={(e) => setLoginPassword(e.target.value)}
                  required
                  disabled={isSubmitting}
                />
              </div>
              <button type="submit" className="ep-btn ep-btn-primary" disabled={isSubmitting}>
                {isSubmitting ? "Signing in..." : "Sign In"}
              </button>
              <button
                type="button"
                className="ep-btn ep-btn-secondary"
                disabled={isSubmitting}
                onClick={() => handleLogin(undefined, "demo@pulse.io", "demoPassword123!")}
              >
                Quick Demo Login (demo@pulse.io)
              </button>
            </form>
          </div>
        </div>
      ) : (
        <>
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
        </>
      )}
    </div>
  );
}

export default App;
