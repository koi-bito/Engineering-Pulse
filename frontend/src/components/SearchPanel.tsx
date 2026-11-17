import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { searchEvents } from "../lib/api.ts";
import { ErrorBanner, SkeletonBlock } from "./PanelState.tsx";

const SEARCH_DEBOUNCE_MS = 300;

export function SearchPanel() {
  const [input, setInput] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<string>("");
  const [rangeFilter, setRangeFilter] = useState<string>("");

  useEffect(() => {
    const trimmed = input.trim();
    const timer = window.setTimeout(() => {
      setDebouncedQuery(trimmed);
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [input]);

  const searchQuery = useQuery({
    queryKey: ["search", debouncedQuery, typeFilter, rangeFilter],
    queryFn: () =>
      searchEvents(debouncedQuery, {
        type: typeFilter || undefined,
        range: rangeFilter || undefined,
      }),
    enabled: debouncedQuery.length > 0,
  });

  return (
    <div className="ep-stack">
      <div className="ep-card">
        <label htmlFor="ep-search-input" className="ep-search-label">
          Search PRs, commits, and Jira tickets
        </label>
        <input
          id="ep-search-input"
          type="search"
          className="ep-input"
          placeholder="Search e.g. auth, kafka, cycle time..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
        />
        <div className="ep-search-filters">
          <label className="ep-filter-label">
            Type:
            <select
              className="ep-select"
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
            >
              <option value="">All Types</option>
              <option value="pull_request">Pull Requests</option>
              <option value="commit">Commits</option>
              <option value="ticket">Jira Tickets</option>
            </select>
          </label>
          <label className="ep-filter-label">
            Range:
            <select
              className="ep-select"
              value={rangeFilter}
              onChange={(e) => setRangeFilter(e.target.value)}
            >
              <option value="">All Time</option>
              <option value="7d">Last 7 Days</option>
              <option value="14d">Last 14 Days</option>
              <option value="30d">Last 30 Days</option>
              <option value="90d">Last 90 Days</option>
            </select>
          </label>
        </div>
      </div>

      {debouncedQuery.length === 0 ? (
        <p className="ep-muted">Type a keyword to search indexed engineering events.</p>
      ) : searchQuery.isPending ? (
        <div className="ep-stack">
          <SkeletonBlock height={68} />
          <SkeletonBlock height={68} />
          <SkeletonBlock height={68} />
        </div>
      ) : searchQuery.isError ? (
        <ErrorBanner error={searchQuery.error} onRetry={() => searchQuery.refetch()} />
      ) : (
        <section className="ep-card">
          <div className="ep-card-head">
            <h3>Results for &ldquo;{searchQuery.data.query}&rdquo;</h3>
            <span className="ep-muted">{searchQuery.data.total_hits} matches</span>
          </div>
          {searchQuery.data.results.length === 0 ? (
            <p className="ep-muted">No matching events found.</p>
          ) : (
            <ul className="ep-search-list">
              {searchQuery.data.results.map((hit) => (
                <li key={hit.id} className="ep-search-item">
                  <div className="ep-search-meta">
                    <span className="ep-badge">
                      {hit.source ?? "unknown"}:{hit.type ?? "event"}
                    </span>
                    {hit.score !== null && (
                      <span className="ep-badge ep-badge-score">
                        Score: {hit.score.toFixed(2)}
                      </span>
                    )}
                    {hit.repo && <span className="ep-muted">{hit.repo}</span>}
                    {hit.author && <span className="ep-muted">by {hit.author}</span>}
                    {hit.timestamp && (
                      <span className="ep-feed-time">
                        {new Date(hit.timestamp).toLocaleString()}
                      </span>
                    )}
                  </div>
                  {hit.highlight ? (
                    <p
                      className="ep-search-snippet"
                      dangerouslySetInnerHTML={{ __html: hit.highlight }}
                    />
                  ) : (
                    <p className="ep-search-snippet">{hit.snippet}</p>
                  )}
                </li>
              ))}
            </ul>
          )}
        </section>
      )}
    </div>
  );
}
