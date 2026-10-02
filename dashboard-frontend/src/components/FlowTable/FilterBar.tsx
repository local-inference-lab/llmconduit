/**
 * Quick-chip filter bar for the FlowTable: status (the four FlowStatus values), model, and
 * upstream target. Each group is a row of toggle chips; the active chip filters the list. The
 * model/upstream options are derived from the rows in view (only values actually present), so
 * the bar stays relevant as flows arrive.
 *
 * ALWAYS-VISIBLE ACTIVE FILTER (D12 R5 MED): a Topology/Sankey cross-link can SET a model/upstream
 * filter to a value with NO matching flow in view (e.g. an idle provider, or a model whose flows
 * aged out). Since the chip options are derived ONLY from rows in view, that active value would
 * render no chip — an INVISIBLE filter the user cannot toggle off, leaving the table stuck empty.
 * So we fold the currently-SELECTED value into each group's option list even when no row matches it,
 * guaranteeing it always shows as an active (toggle-off-able) chip; a "clear" control also appears
 * whenever any facet is active as a single-click escape hatch.
 */
import { FLOW_STATUSES } from '../../api/types';
import { cn } from '../../lib/cn';
import { EMPTY_FILTERS, type FlowFilters } from './filterTypes';

/** Union the derived options with the active value (if any) so an unmatched selection stays visible. */
function withSelected(options: string[], selected: string | null): string[] {
  if (!selected || options.includes(selected)) return options;
  // Surface the orphaned active value FIRST so it reads as "the filter you set" before the in-view
  // options; the rest keep their sorted (caller-supplied) order.
  return [selected, ...options];
}

/**
 * Max client-filter chips (gap 15 review MEDIUM). Client attribution is HIGH-CARDINALITY — rendering a
 * chip per distinct `client_label` could create thousands of buttons and wrap the bar unusable. So the
 * client facet caps to the top-N busiest clients (`clients` arrives volume-ordered from `useFlowRows`).
 */
export const CLIENT_CHIP_CAP = 8;

/**
 * Cap a volume-ordered option list to the top-N, ALWAYS keeping the active selection visible even when
 * it is NOT in the top-N (an out-of-top-N active client must stay selected + toggle-off-able — never
 * silently dropped). The active value is surfaced FIRST (as "the filter you set"); the remaining top-N
 * (minus the active, to avoid a duplicate) follow in their volume order, total ≤ `cap`.
 */
function capWithSelected(options: string[], selected: string | null, cap: number): string[] {
  if (!selected) return options.slice(0, cap);
  // The active value leads; fill the rest of the cap with the busiest OTHERS (deduping the active).
  const rest = options.filter((o) => o !== selected).slice(0, Math.max(0, cap - 1));
  return [selected, ...rest];
}

/**
 * `truncateLabel` (gap 15 review round 2 MEDIUM): a high-cardinality `client_label` can be an
 * UNBOUNDED ~4 KiB string (UA / configured-header values), so a SINGLE long label would blow up / wrap
 * the bar even with the top-8 chip cap. A truncated chip renders its label in a bounded `max-w` +
 * `truncate` (ellipsis) span with a `title` carrying the full value on hover — applied to EVERY client
 * chip (incl. the always-folded-in active selection). The chip stays clickable/toggle-off-able.
 */
function Chip({
  active,
  onClick,
  children,
  truncateLabel,
  title,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
  truncateLabel?: boolean;
  title?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      title={title}
      className={cn(
        'flex items-center rounded-full border px-2.5 py-0.5 text-xs transition-colors',
        active
          ? 'border-accent/40 bg-accent/15 text-accent'
          : 'border-line bg-panel text-text-muted hover:text-text',
      )}
    >
      {truncateLabel ? (
        // `max-w` + `truncate` (white-space:nowrap; overflow:hidden; text-overflow:ellipsis) is a NO-OP
        // on an INLINE box — `max-width` does not apply, so a ~4 KiB label would still expand the bar
        // (review round 3). `inline-block` gives it a block formatting context so the width cap + ellipsis
        // take effect; `min-w-0` lets it shrink inside the flex button (overriding the min-content floor).
        <span className="inline-block min-w-0 max-w-[160px] truncate align-bottom" data-testid="flow-filter-chip-label">
          {children}
        </span>
      ) : (
        children
      )}
    </button>
  );
}

export function FilterBar({
  filters,
  models,
  upstreams,
  clients,
  harnesses = [],
  total,
  shown,
  onChange,
}: {
  filters: FlowFilters;
  models: string[];
  upstreams: string[];
  /** Gap 15 — distinct `client_label`s in view, for the per-client filter chips. */
  clients: string[];
  /** Sessions — distinct detected harnesses in view. */
  harnesses?: string[];
  total: number;
  shown: number;
  onChange: (next: FlowFilters) => void;
}) {
  const toggle = <K extends keyof FlowFilters>(key: K, value: FlowFilters[K]) =>
    onChange({ ...filters, [key]: filters[key] === value ? null : value });

  // Fold the active value into each list so a cross-linked selection with no matching row in view
  // still renders an (active, toggle-off-able) chip instead of becoming an invisible filter.
  const modelOptions = withSelected(models, filters.model);
  const upstreamOptions = withSelected(upstreams, filters.upstream);
  // Client attribution is HIGH-CARDINALITY (review MEDIUM): cap to the top-N busiest clients (volume-
  // ordered by `useFlowRows`) while ALWAYS keeping the active selection visible/toggle-off-able even
  // when it falls outside the top-N — never silently drop the user's current filter.
  const clientOptions = capWithSelected(clients, filters.client, CLIENT_CHIP_CAP);
  const clientsHidden = Math.max(0, clients.filter((c) => c !== filters.client).length - clientOptions.filter((c) => c !== filters.client).length);
  const harnessOptions = withSelected(harnesses, filters.harness);
  const anyActive =
    filters.status !== null || filters.model !== null || filters.upstream !== null || filters.client !== null ||
    filters.harness !== null || filters.session !== null || filters.cacheBust !== null;

  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-line bg-panel px-3 py-2" data-testid="flow-filter-bar">
      <FilterGroup label="status">
        {FLOW_STATUSES.map((s) => (
          <Chip key={s} active={filters.status === s} onClick={() => toggle('status', s)}>
            {s}
          </Chip>
        ))}
      </FilterGroup>
      {modelOptions.length > 0 && (
        <FilterGroup label="model">
          {modelOptions.map((m) => (
            <Chip key={m} active={filters.model === m} onClick={() => toggle('model', m)}>
              {m}
            </Chip>
          ))}
        </FilterGroup>
      )}
      {upstreamOptions.length > 0 && (
        <FilterGroup label="upstream">
          {upstreamOptions.map((u) => (
            <Chip key={u} active={filters.upstream === u} onClick={() => toggle('upstream', u)}>
              {u}
            </Chip>
          ))}
        </FilterGroup>
      )}
      {/* Gap 15: the per-client facet — capped to the top-N BUSIEST `client_label`s (client attribution
          is high-cardinality; review MEDIUM). The active selection (incl. a cross-link from the "by
          client" roll-up) is ALWAYS folded in + toggle-off-able even when outside the top-N; a `+N more`
          hint flags that lower-volume clients are not chip-listed (filter via the roll-up's cross-link). */}
      {clientOptions.length > 0 && (
        <FilterGroup label="client">
          {clientOptions.map((c) => (
            // `truncateLabel` bounds an UNBOUNDED ~4 KiB `client_label` (UA / configured-header) to a
            // `max-w` ellipsis span (full value in the `title`), so a single long label can't blow up
            // the bar — applied to EVERY client chip incl. the active selection (review round 2 MEDIUM).
            <Chip key={c} active={filters.client === c} onClick={() => toggle('client', c)} truncateLabel title={c}>
              {c}
            </Chip>
          ))}
          {clientsHidden > 0 && (
            <span
              className="text-[10px] tabular-nums text-text-muted"
              data-testid="flow-filter-client-overflow"
              title={`${clientsHidden} lower-volume client${clientsHidden === 1 ? '' : 's'} not listed — filter via the "by client" roll-up below`}
            >
              +{clientsHidden} more
            </span>
          )}
        </FilterGroup>
      )}
      {/* Sessions: the harness facet (bounded — a handful of profiles) + the cache-bust toggle. The
          session facet is set only by the Sessions view cross-link; it renders as an active chip so it
          is visible + toggle-off-able, never an invisible filter. */}
      {harnessOptions.length > 0 && (
        <FilterGroup label="harness">
          {harnessOptions.map((h) => (
            <Chip key={h} active={filters.harness === h} onClick={() => toggle('harness', h)}>
              {h}
            </Chip>
          ))}
        </FilterGroup>
      )}
      <FilterGroup label="cache">
        <Chip active={filters.cacheBust === true} onClick={() => toggle('cacheBust', true)} title="only flows whose prefix diverged from their chain predecessor">
          busts
        </Chip>
      </FilterGroup>
      {filters.session && (
        <FilterGroup label="session">
          <Chip active onClick={() => toggle('session', filters.session)} truncateLabel title={filters.session}>
            {filters.session}
          </Chip>
        </FilterGroup>
      )}
      {/* Single-click escape hatch: always available while any facet is active, so even an
          off-screen / unmatched selection is clearable without hunting for its chip. */}
      {anyActive && (
        <button
          type="button"
          onClick={() => onChange(EMPTY_FILTERS)}
          className="rounded-full border border-line bg-panel px-2.5 py-0.5 text-xs text-text-muted transition-colors hover:text-text"
          data-testid="flow-filter-clear"
        >
          clear
        </button>
      )}
      <span className="ml-auto tabular-nums text-xs text-text-muted" data-testid="flow-count">
        {shown === total ? `${total} flows` : `${shown} / ${total}`}
      </span>
    </div>
  );
}

function FilterGroup({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-[10px] uppercase tracking-wide text-text-muted">{label}</span>
      {children}
    </div>
  );
}
