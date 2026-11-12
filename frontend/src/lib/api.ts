import axios, { type AxiosError } from "axios";

export interface ApiErrorBody {
  error: string;
  detail: unknown;
  retry_after?: number;
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
  baseURL: import.meta.env.VITE_API_URL || "http://localhost:8000",
  withCredentials: true,
});

api.interceptors.response.use(
  (response) => response,
  (error: AxiosError<ApiErrorBody>) => {
    error.message = extractApiErrorMessage(error);
    return Promise.reject(error);
  },
);
