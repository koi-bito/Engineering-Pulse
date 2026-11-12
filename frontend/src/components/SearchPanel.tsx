import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { searchEvents } from "../lib/api.ts";
import { ErrorBanner, SkeletonBlock } from "./PanelState.tsx";

const SEARCH_DEBOUNCE_MS = 300;

export function SearchPanel() {
  const [input, setInput] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");

  useEffect(() => {
    const trimmed = input.trim();
    const timer = window.setTimeout(() => {
      setDebouncedQuery(trimmed);
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [input]);

  const searchQuery = useQuery({
    queryKey: ["search", debouncedQuery],
    queryFn: () => searchEvents(debouncedQuery),
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
                    {hit.repo && <span className="ep-muted">{hit.repo}</span>}
                    {hit.author && <span className="ep-muted">by {hit.author}</span>}
                    {hit.timestamp && (
                      <span className="ep-feed-time">
                        {new Date(hit.timestamp).toLocaleString()}
                      </span>
                    )}
                  </div>
                  <p className="ep-search-snippet">{hit.snippet}</p>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}
    </div>
  );
}

