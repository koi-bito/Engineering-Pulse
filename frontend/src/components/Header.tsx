interface HeaderProps {
  apiHealthy: boolean | null;
  environment?: string;
}

export function Header({ apiHealthy, environment }: HeaderProps) {
  const statusLabel =
    apiHealthy === null
      ? "API: checking"
      : apiHealthy
        ? `API: online (${environment ?? "local"})`
        : "API: offline";

  return (
    <header className="ep-header">
      <div className="ep-brand">
        <span className="ep-dot" data-healthy={String(apiHealthy)} />
        <h1>Engineering Pulse</h1>
      </div>
      <span className="ep-status">{statusLabel}</span>
    </header>
  );
}

