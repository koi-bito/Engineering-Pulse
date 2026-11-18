import axios, { type AxiosError } from "axios";

export interface ApiErrorBody {
  error: string;
  detail: unknown;
  retry_after?: number;
}

export interface MetricsSummary {
  prs_merged_today: number;
  avg_cycle_time_7d_hours: number | null;
  avg_ticket_cycle_time_7d_hours: number | null;
  open_prs: number;
  active_tickets: number;
  commits_today: number;
}

export interface PrDailyPoint {
  date: string;
  merged_count: number;
  opened_count: number;
}

export interface PrMetricsResponse {
  range: string;
  days: number;
  series: PrDailyPoint[];
}

export interface CycleTimePoint {
  date: string;
  avg_ticket_cycle_time_hours: number | null;
  rolling_avg_hours: number | null;
  avg_pr_cycle_time_hours: number | null;
  resolved_tickets: number;
  merged_prs: number;
}

export interface CycleTimeResponse {
  range: string;
  days: number;
  series: CycleTimePoint[];
}

export interface SprintTicket {
  id: string;
  project: string;
  assignee: string | null;
  summary: string;
  status: string;
  created_at: string;
  resolved_at: string | null;
  cycle_time_hours: number | null;
}

export interface SprintActiveResponse {
  project: string;
  status_counts: Record<string, number>;
  tickets: SprintTicket[];
}

export interface SearchHit {
  id: string;
  type: string | null;
  source: string | null;
  author: string | null;
  repo: string | null;
  timestamp: string | null;
  snippet: string;
  highlight?: string | null;
  score: number | null;
}

export interface SearchResponse {
  query: string;
  total_hits: number;
  results: SearchHit[];
}

export interface Team {
  id: string;
  org_id: string;
  name: string;
  created_at: string;
}

export interface WorkflowAlert {
  id: string;
  team_id: string;
  metric_name: string;
  actual_value: number;
  expected_value: number;
  severity: "warning" | "critical" | string;
  status: "active" | "grouped" | "suppressed" | "resolved" | string;
  group_id: string | null;
  details: Record<string, unknown>;
  created_at: string | null;
  resolved_at: string | null;
}

let activeTeamId: string | null = null;
let authToken: string | null = null;

export function setAuthToken(token: string | null): void {
  authToken = token;
  if (typeof window !== "undefined") {
    if (token) {
      localStorage.setItem("ep_auth_token", token);
    } else {
      localStorage.removeItem("ep_auth_token");
    }
  }
}

export function getAuthToken(): string | null {
  if (authToken === null && typeof window !== "undefined") {
    authToken = localStorage.getItem("ep_auth_token");
  }
  return authToken;
}

export function setActiveTeamId(teamId: string | null): void {
  activeTeamId = teamId;
  if (teamId) {
    localStorage.setItem("ep_active_team_id", teamId);
  } else {
    localStorage.removeItem("ep_active_team_id");
  }
}

export function getActiveTeamId(): string | null {
  if (activeTeamId === null && typeof window !== "undefined") {
    activeTeamId = localStorage.getItem("ep_active_team_id");
  }
  return activeTeamId;
}

export function getApiBaseUrl(): string {
  return import.meta.env?.VITE_API_URL || "http://localhost:8000";
}

export function getWebSocketUrl(path = "/ws/feed", baseUrl = getApiBaseUrl()): string {
  const normalizedBase = baseUrl.replace(/\/+$/, "");
  const wsBase = normalizedBase.startsWith("https://")
    ? normalizedBase.replace(/^https:\/\//, "wss://")
    : normalizedBase.replace(/^http:\/\//, "ws://");
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${wsBase}${normalizedPath}`;
}

export function extractApiErrorMessage(err: unknown): string {
  if (axios.isAxiosError(err)) {
    const data = err.response?.data as Partial<ApiErrorBody> | undefined;
    if (data && typeof data === "object" && data.detail !== undefined) {
      const detail =
        typeof data.detail === "string"
          ? data.detail
          : JSON.stringify(data.detail);
      return data.retry_after !== undefined
        ? `${detail} (retry after ${data.retry_after}s)`
        : detail;
    }
    return err.message || "Request failed";
  }
  return err instanceof Error ? err.message : "Unexpected error";
}

export const api = axios.create({
  baseURL: getApiBaseUrl(),
  withCredentials: true,
});

api.interceptors.request.use((config) => {
  const token = getAuthToken();
  if (token) {
    config.headers.set("Authorization", `Bearer ${token}`);
  }
  const teamId = getActiveTeamId();
  if (teamId) {
    config.headers.set("X-Team-ID", teamId);
  }
  return config;
});

let isRefreshing = false;

api.interceptors.response.use(
  (response) => response,
  async (error: AxiosError<ApiErrorBody>) => {
    const originalRequest = error.config;
    if (
      error.response?.status === 401 &&
      originalRequest &&
      !(originalRequest as unknown as { _retry?: boolean })._retry &&
      !originalRequest.url?.includes("/auth/jwt/")
    ) {
      if (!isRefreshing) {
        isRefreshing = true;
        (originalRequest as unknown as { _retry: boolean })._retry = true;
        try {
          const newToken = await refreshAuthToken();
          isRefreshing = false;
          if (newToken) {
            originalRequest.headers.set("Authorization", `Bearer ${newToken}`);
            return api(originalRequest);
          }
        } catch {
          isRefreshing = false;
        }
      }
    }
    error.message = extractApiErrorMessage(error);
    return Promise.reject(error);
  },
);

export interface LoginUserResponse {
  access_token: string;
  token_type: string;
  user?: {
    id: string;
    email: string;
    role: string;
    org_id: string;
    team_ids: string[];
  };
}

export async function loginUser(email: string, password: string): Promise<LoginUserResponse> {
  const params = new URLSearchParams();
  params.append("username", email);
  params.append("password", password);
  const { data } = await api.post<LoginUserResponse>("/auth/jwt/login", params, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });
  setAuthToken(data.access_token);
  return data;
}

export async function refreshAuthToken(): Promise<string | null> {
  try {
    const { data } = await api.post<LoginUserResponse>("/auth/jwt/refresh");
    setAuthToken(data.access_token);
    return data.access_token;
  } catch {
    setAuthToken(null);
    return null;
  }
}

export async function logoutUser(): Promise<void> {
  try {
    await api.post("/auth/jwt/logout");
  } catch {
    // ignore
  } finally {
    setAuthToken(null);
  }
}

export async function fetchTeams(): Promise<Team[]> {
  try {
    const { data } = await api.get<Team[]>("/api/teams");
    return data;
  } catch {
    return [];
  }
}

export async function fetchActiveAlerts(): Promise<WorkflowAlert[]> {
  try {
    const { data } = await api.get<WorkflowAlert[]>("/api/alerts/active");
    return data;
  } catch {
    return [];
  }
}

export async function fetchMetricsSummary(): Promise<MetricsSummary> {
  const { data } = await api.get<MetricsSummary>("/api/metrics/summary");
  return data;
}

export async function fetchPrMetrics(range = "14d"): Promise<PrMetricsResponse> {
  const { data } = await api.get<PrMetricsResponse>("/api/metrics/prs", {
    params: { range },
  });
  return data;
}

export async function fetchCycleTimeMetrics(range = "30d"): Promise<CycleTimeResponse> {
  const { data } = await api.get<CycleTimeResponse>("/api/metrics/cycle-time", {
    params: { range },
  });
  return data;
}

export async function fetchActiveSprint(project?: string): Promise<SprintActiveResponse> {
  const { data } = await api.get<SprintActiveResponse>("/api/sprint/active", {
    params: project ? { project } : undefined,
  });
  return data;
}

export interface SearchOptions {
  type?: string;
  author?: string;
  range?: string;
}

export async function searchEvents(
  query: string,
  options?: SearchOptions
): Promise<SearchResponse> {
  const params: Record<string, string> = { q: query };
  if (options?.type) params.type = options.type;
  if (options?.author) params.author = options.author;
  if (options?.range) params.range = options.range;

  const { data } = await api.get<SearchResponse>("/api/search", { params });
  return data;
}

const DEFAULT_STATUS_ORDER = ["To Do", "In Progress", "Done"];

export interface SprintColumn {
  status: string;
  count: number;
  tickets: SprintTicket[];
}

export function groupTicketsByStatus(
  tickets: SprintTicket[],
  statusCounts: Record<string, number>,
): SprintColumn[] {
  const grouped = new Map<string, SprintTicket[]>();
  for (const status of DEFAULT_STATUS_ORDER) {
    grouped.set(status, []);
  }
  for (const status of Object.keys(statusCounts)) {
    if (!grouped.has(status)) {
      grouped.set(status, []);
    }
  }
  for (const ticket of tickets) {
    const list = grouped.get(ticket.status) ?? [];
    list.push(ticket);
    grouped.set(ticket.status, list);
  }

  return Array.from(grouped.entries()).map(([status, columnTickets]) => ({
    status,
    count: statusCounts[status] ?? columnTickets.length,
    tickets: columnTickets,
  }));
}
