import { navigate, type RouteName } from '../router/useHashRoute';
import { Button } from './ui/Button';
import { cn } from '../lib/cn';

const LABELS: Record<RouteName, string> = {
  flows: 'Flows',
  sessions: 'Sessions',
  throughput: 'Throughput',
  activity: 'Activity',
  chat: 'Chat',
  account: 'Account',
  topology: 'Topology',
  sankey: 'Sankey',
  theater: 'Theater',
  overview: 'Overview',
  providers: 'Providers',
  access: 'Access',
};

const SECTIONS: Array<{ label: string; landing: RouteName; routes: RouteName[] }> = [
  { label: 'Chat', landing: 'chat', routes: ['chat'] },
  { label: 'Observe', landing: 'overview', routes: ['overview', 'flows', 'sessions', 'throughput', 'activity'] },
  { label: 'Infrastructure', landing: 'providers', routes: ['providers', 'topology', 'sankey', 'theater'] },
  { label: 'Admin', landing: 'access', routes: ['access', 'account'] },
];

/** The Argus eye — the hundred-eyed watchman's iris, the brand mark. Keeps a slow watch. */
function ArgusEye({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} aria-hidden="true">
      <path
        d="M1.6 12S5.2 5.6 12 5.6 22.4 12 22.4 12 18.8 18.4 12 18.4 1.6 12 1.6 12Z"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
      <circle cx="12" cy="12" r="3.4" stroke="currentColor" strokeWidth="1.5" />
      <circle cx="12" cy="12" r="1.25" fill="currentColor" />
    </svg>
  );
}

export function NavTabs({ active, onLogout }: { active: RouteName; onLogout: () => void }) {
  const activeSection = SECTIONS.find((section) => section.routes.includes(active)) ?? SECTIONS[0]!;
  return (
    <nav className="flex flex-wrap items-center gap-x-6 gap-y-2 border-b border-line bg-panel px-5 py-2.5" aria-label="Dashboard">
      {/* Masthead: the Argus eye + tracked wordmark; llmconduit rides below as the eyebrow. */}
      <div className="flex shrink-0 items-center gap-2.5 pr-1">
        <ArgusEye className="argus-eye h-[18px] w-[18px] text-accent" />
        <div className="leading-none">
          <div className="font-ui text-sm font-bold tracking-[0.24em] text-text">ARGUS</div>
          <div className="mt-1 font-mono text-[9px] uppercase tracking-[0.22em] text-text-muted">
            llmconduit · watch
          </div>
        </div>
      </div>
      <div className="order-3 flex w-full flex-wrap items-center gap-1 lg:order-none lg:w-auto" aria-label="Dashboard sections">
        {SECTIONS.map((section) => (
          <button
            key={section.label}
            onClick={() => navigate(section.landing)}
            aria-pressed={section === activeSection}
            className={cn(
              'rounded-md px-3 py-1.5 text-xs font-medium uppercase tracking-[0.14em] transition-colors',
              section === activeSection
                ? 'bg-accent/12 text-accent'
                : 'text-text-muted hover:bg-line/40 hover:text-text',
            )}
          >
            {section.label}
          </button>
        ))}
      </div>
      <Button variant="ghost" className="ml-auto shrink-0" onClick={onLogout}>
        Logout
      </Button>
      {activeSection.routes.length > 1 && (
        <div className="order-4 flex w-full flex-wrap items-center gap-1 border-t border-line/70 pt-2" aria-label={`${activeSection.label} views`}>
          {activeSection.routes.map((route) => (
            <button
              key={route}
              onClick={() => navigate(route)}
              aria-current={route === active ? 'page' : undefined}
              className={cn(
                'rounded px-2.5 py-1 text-[10px] font-medium uppercase tracking-[0.12em] transition-colors',
                route === active ? 'bg-line/60 text-text' : 'text-text-muted hover:text-text',
              )}
            >
              {LABELS[route]}
            </button>
          ))}
        </div>
      )}
    </nav>
  );
}
