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
  score: number | null;
}

export interface SearchResponse {
  query: string;
  total_hits: number;
  results: SearchHit[];
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

api.interceptors.response.use(
  (response) => response,
  (error: AxiosError<ApiErrorBody>) => {
    error.message = extractApiErrorMessage(error);
    return Promise.reject(error);
  },
);

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

export async function searchEvents(query: string): Promise<SearchResponse> {
  const { data } = await api.get<SearchResponse>("/api/search", {
    params: { q: query },
  });
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
