import { extractApiErrorMessage } from "../lib/api.ts";

export function SkeletonBlock({ height = 120 }: { height?: number }) {
  return <div className="ep-skeleton" style={{ height }} aria-busy="true" />;
}

export function ErrorBanner({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry?: () => void;
}) {
  return (
    <div className="ep-error" role="alert">
      <span>{extractApiErrorMessage(error)}</span>
      {onRetry && (
        <button type="button" className="ep-btn-secondary" onClick={onRetry}>
          Retry
        </button>
      )}
    </div>
  );
}

