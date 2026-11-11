export type Section = "overview" | "sprint" | "search";

interface SidebarProps {
  active: Section;
  onSelect: (section: Section) => void;
}

const NAV_ITEMS: { id: Section; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "sprint", label: "Active Sprint" },
  { id: "search", label: "Search" },
];

export function Sidebar({ active, onSelect }: SidebarProps) {
  return (
    <aside className="ep-sidebar">
      <nav aria-label="Dashboard sections">
        {NAV_ITEMS.map((item) => (
          <button
            key={item.id}
            type="button"
            className={active === item.id ? "ep-nav-item active" : "ep-nav-item"}
            onClick={() => onSelect(item.id)}
          >
            {item.label}
          </button>
        ))}
      </nav>
    </aside>
  );
}

