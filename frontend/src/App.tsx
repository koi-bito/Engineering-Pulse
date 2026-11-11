import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Header } from "./components/Header";
import { Sidebar, type Section } from "./components/Sidebar";
import { useWebSocket } from "./hooks/useWebSocket";
import { api } from "./lib/api";
import "./App.css";

interface HealthResponse {
  status: string;
  environment: string;
  timestamp: string;
}

export function App() {
  const [section, setSection] = useState<Section>("overview");
  const { status: wsStatus } = useWebSocket();

  const healthQuery = useQuery({
    queryKey: ["health"],
    queryFn: async () => {
      const { data } = await api.get<HealthResponse>("/health");
      return data;
    },
    retry: false,
  });

  const apiHealthy = healthQuery.isPending
    ? null
    : healthQuery.isSuccess && healthQuery.data.status === "ok";

  return (
    <div className="ep-shell">
      <Header
        apiHealthy={apiHealthy}
        environment={healthQuery.data?.environment}
      />
      <div className="ep-body">
        <Sidebar active={section} onSelect={setSection} />
        <main className="ep-main">
          <h2>{section.charAt(0).toUpperCase() + section.slice(1)}</h2>
          <p className="ep-muted">
            Phase 0 shell ready. WebSocket status: <code>{wsStatus}</code>.
            {healthQuery.data?.timestamp && (
              <>
                {" "}
                Last API check (local time):{" "}
                {new Date(healthQuery.data.timestamp).toLocaleString()}.
              </>
            )}
          </p>
        </main>
      </div>
    </div>
  );
}

export default App;

