/**
 * SessionsView — the dashboard's PRIMARY view: ACTIVE sessions first.
 *
 * Left/main: the live board (`GET /dashboard/api/sessions/active`, pushed by the
 * sessions-domain `session_update` WS frames — real time, no polling race): every
 * session with activity in the last 15 minutes, WHO owns it (user + key labels,
 * resolved via the users/keys queries with id-prefix / key-hash fallbacks), the
 * trailing 1/5/10/15-minute request windows, lifetime token totals (SQL aggregate
 * when history is on; the live ring otherwise), and the total request count.
 * A row UNFOLDS into the session's actual requests (the newest stubs with status,
 * tokens, and errors).
 *
 * The durable 7-day session TREE (lineage, sub-sessions, chain divergence) stays
 * reachable behind the "7-day tree" toggle — it answers different questions
 * (where did this conversation fork?) than the live board (what is running now?).
 * With no SQL store the tree endpoints answer 503 and the toggle says so.
 */
import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import type { ActiveSessionsResponse, HistoryRequest, SessionRow } from '../../api/types';
import { flowFilterStore } from '../../store/flowFilterStore';
import { useAuth } from '../../store/hooks';
import { navigate } from '../../router/useHashRoute';
import { Panel } from '../../components/ui/Panel';
import { fmtClock, fmtTokens } from '../../components/FlowTable/format';
import { shortId } from '../../components/FlowTable/flowModel';
import { cn } from '../../lib/cn';
import { divergenceLabel, fmtAge, kindBadge, rollupRequests, sessionLabel, sortByActivity } from './sessionsModel';
import { activeSessionRows, sessionAttribution, tokenTotals, type ActiveSessionRow } from './activeSessionsModel';

const DASH = '—';

export function SessionsView() {
  const { client } = getConnection();
  const [showTree, setShowTree] = useState(false);

  // -- the live board (WS-pushed via session_update → invalidateForDomain) ----
  const active = useQuery({
    queryKey: queryKeys.activeSessions,
    queryFn: (): Promise<ActiveSessionsResponse> => client.activeSessions(),
    refetchInterval: 30_000, // safety net; the WS push is the primary driver
  });
  // Attribution resolution: users/keys are admin-gated; a non-admin (or a
  // 503/403) simply falls back to id prefixes / the key-hash label.
  const isAdmin = useAuth((s) => s.user?.is_admin ?? s.authMode !== 'users');
  const users = useQuery({ queryKey: queryKeys.users, queryFn: () => client.listUsers(), enabled: isAdmin, retry: false });
  const keys = useQuery({ queryKey: queryKeys.keys('all'), queryFn: () => client.listKeys(isAdmin ? 'all' : undefined), retry: false });
  const userName = (id: string | null) => (id == null ? null : users.data?.users.find((u) => u.id === id)?.username ?? `${id.slice(0, 8)}…`);
  const keyLabel = (id: string | null) => (id == null ? null : keys.data?.keys.find((k) => k.id === id)?.label ?? null);

  const rows = useMemo(() => activeSessionRows(active.data), [active.data]);

  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden" data-testid="sessions-view">
      <div className="flex items-baseline gap-2 border-b border-line bg-panel px-3 py-2">
        <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">sessions</h1>
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">active in the last 15 minutes · user · key</span>
        <span className="ml-auto tabular-nums text-xs text-text-muted" data-testid="sessions-count">
          {active.data ? `${rows.length} active` : ''}
        </span>
        <button
          type="button"
          onClick={() => setShowTree((value) => !value)}
          className={cn(
            'rounded-md border border-line px-2 py-0.5 text-[10px] uppercase tracking-[0.12em] transition-colors',
            showTree ? 'bg-accent/15 text-accent' : 'text-text-muted hover:text-text',
          )}
          data-testid="sessions-tree-toggle"
          data-open={showTree || undefined}
        >
          7-day tree
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {active.isError && (
          <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid="sessions-unavailable" data-reason="error">
            {`Could not load active sessions: ${active.error instanceof Error ? active.error.message : String(active.error)}`}
          </div>
        )}
        {active.data && rows.length === 0 && (
          <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid="sessions-empty">
            No sessions active in the last 15 minutes.
          </div>
        )}
        {rows.length > 0 && (
          <div className="rounded-md border-line">
            <div className="grid grid-cols-[minmax(140px,1.2fr)_minmax(90px,0.9fr)_minmax(90px,0.9fr)_48px_56px_56px_56px_56px_64px_24px] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
              <span>session</span>
              <span>user</span>
              <span>key</span>
              <span className="text-right">msgs</span>
              <span className="text-right" title="requests in the last 1 minute">1m</span>
              <span className="text-right" title="requests in the last 5 minutes">5m</span>
              <span className="text-right" title="requests in the last 10 minutes">10m</span>
              <span className="text-right" title="requests in the last 15 minutes">15m</span>
              <span className="text-right" title="lifetime reported tokens">in·out·cached</span>
              <span />
            </div>
            {rows.map((row) => (
              <ActiveSessionCard
                key={row.session.id}
                row={row}
                userName={userName}
                keyLabel={keyLabel}
              />
            ))}
          </div>
        )}
        {showTree && <DurableTreePanel />}
      </div>
    </div>
  );
}

function ActiveSessionCard({
  row,
  userName,
  keyLabel,
}: {
  row: ActiveSessionRow;
  userName: (id: string | null) => string | null;
  keyLabel: (id: string | null) => string | null;
}) {
  const [open, setOpen] = useState(false);
  const session = row.session;
  const attribution = sessionAttribution(session, userName, keyLabel);
  const tokens = tokenTotals(row.aggregate, row.requests);
  const tokensValue =
    tokens.input == null && tokens.output == null && tokens.cached == null
      ? DASH
      : `${fmtTokens(tokens.input ?? 0)}·${fmtTokens(tokens.output ?? 0)}·${fmtTokens(tokens.cached ?? 0)}`;

  return (
    <div className="border-b border-line/50" data-testid="active-session">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        data-testid="active-session-row"
        data-open={open || undefined}
        title={`${session.harness}${session.external_id ? ` session ${session.external_id}` : ' (no session id on the wire)'} · last activity ${new Date(session.last_seen_ms).toISOString()}`}
        className="grid w-full grid-cols-[minmax(140px,1.2fr)_minmax(90px,0.9fr)_minmax(90px,0.9fr)_48px_56px_56px_56px_56px_64px_24px] items-center gap-2 px-3 py-1.5 text-left text-xs transition-colors hover:bg-accent/[0.06]"
      >
        <span className="truncate font-mono text-text">{sessionLabel(session)}</span>
        <span
          className={cn('truncate', attribution.strong ? 'text-text' : 'text-text-muted')}
          data-testid="active-session-user"
          data-quality={attribution.strong ? 'measured' : 'unavailable'}
          title={attribution.strong ? `user ${session.user_id}` : 'no user attribution'}
        >
          {attribution.user}
        </span>
        <span
          className="truncate text-text-muted"
          data-testid="active-session-key"
          title={session.virtual_key_id ? `virtual key ${session.virtual_key_id}` : session.client_label ?? 'no key attribution'}
        >
          {attribution.key}
        </span>
        <span className="text-right tabular-nums text-text" data-testid="active-session-total" title="total requests linked to this node (measured)">
          {session.request_count}
        </span>
        <span className="text-right tabular-nums text-text-muted">{row.requests_1m}</span>
        <span className="text-right tabular-nums text-text-muted">{row.requests_5m}</span>
        <span className="text-right tabular-nums text-text-muted">{row.requests_10m}</span>
        <span className="text-right tabular-nums text-text-muted">{row.requests_15m}</span>
        <span
          className="text-right tabular-nums text-text-muted"
          data-testid="active-session-tokens"
          data-quality={tokens.quality}
          title={`lifetime reported tokens (${tokens.quality})`}
        >
          {tokensValue}
        </span>
        <span className={cn('text-center text-[10px] text-text-muted transition-transform', open && 'rotate-90')}>›</span>
      </button>
      {open && (
        <div className="bg-panel px-3 pb-2" data-testid="active-session-requests">
          {row.requests.length === 0 ? (
            <div className="py-2 text-xs italic text-text-muted">No requests on the live ring (older than 15 minutes).</div>
          ) : (
            <div>
              {row.requests.map((stub) => (
                <div
                  key={stub.api_call_id}
                  className="grid grid-cols-[72px_96px_minmax(120px,1fr)_72px_72px_72px_minmax(80px,0.8fr)] items-center gap-2 py-1 text-xs"
                  data-testid="active-request"
                  data-status={stub.status}
                >
                  <span className="tabular-nums text-text-muted">{fmtClock(stub.created_at_ms)}</span>
                  <span className="truncate font-mono text-text-muted" title={stub.api_call_id}>{shortId(stub.api_call_id)}</span>
                  <span className="truncate">{stub.client_model}</span>
                  <span
                    className={cn(
                      'w-fit rounded-sm px-1 text-[9px] uppercase tracking-wide',
                      stub.status === 'running' && 'bg-accent/15 text-accent',
                      stub.status === 'failed' && 'bg-status-error/15 text-status-error',
                      stub.status === 'completed' && 'bg-line/40 text-text-muted',
                    )}
                  >
                    {stub.status}
                  </span>
                  <span className="text-right tabular-nums text-text-muted" title="input tokens">{fmtTokens(stub.input_tokens ?? null)}</span>
                  <span className="text-right tabular-nums text-text-muted" title="cached tokens">{fmtTokens(stub.cached_tokens ?? null)}</span>
                  <span className="text-right tabular-nums text-text-muted" title="output tokens">{fmtTokens(stub.output_tokens ?? null)}</span>
                </div>
              ))}
            </div>
          )}
          <button
            type="button"
            onClick={() => {
              flowFilterStore.getState().setSession(session.id);
              navigate('flows');
            }}
            className="mt-1 rounded-md border border-line px-2 py-0.5 text-[10px] uppercase tracking-[0.12em] text-text-muted transition-colors hover:text-text"
            data-testid="active-session-show-in-flows"
          >
            show in flows
          </button>
        </div>
      )}
    </div>
  );
}

/** The durable 7-day session tree (the previous primary view), now a panel. */
function DurableTreePanel() {
  const { client } = getConnection();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const roots = useQuery({
    queryKey: queryKeys.sessions,
    queryFn: () => client.historySessions({ roots: true }),
    refetchInterval: 30_000,
  });
  const detail = useQuery({
    queryKey: selectedId ? queryKeys.session(selectedId) : ['history', 'sessions', '__none__'],
    queryFn: () => client.historySession(selectedId as string),
    enabled: !!selectedId,
    refetchInterval: 30_000,
  });
  const rootRows = useMemo(() => sortByActivity(roots.data?.sessions ?? []), [roots.data]);
  const nowMs = Date.now();

  return (
    <div className="flex min-h-0 border-t border-line" data-testid="sessions-tree">
      <div className="flex min-h-0 w-[38%] min-w-[320px] flex-col border-r border-line">
        <div className="border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
          7-day tree · roots
        </div>
        <div className="min-h-0 flex-1 overflow-auto">
          {roots.isError && <Unavailable testId="sessions-tree-unavailable" error={roots.error} />}
          {roots.data && rootRows.length === 0 && (
            <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid="sessions-tree-empty">
              No sessions in the last 7 days.
            </div>
          )}
          {rootRows.map((node) => (
            <TreeRowView key={node.id} node={node} nowMs={nowMs} selected={node.id === selectedId} onSelect={setSelectedId} />
          ))}
        </div>
      </div>
      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-auto p-3">
        {!selectedId && (
          <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid="sessions-tree-detail-empty">
            Select a session to see its sub-sessions and requests.
          </div>
        )}
        {selectedId && detail.isError && <Unavailable testId="session-detail-unavailable" error={detail.error} />}
        {selectedId && detail.data && <TreeDetailView data={detail.data} nowMs={nowMs} onSelect={setSelectedId} />}
      </div>
    </div>
  );
}

function Unavailable({ testId, error }: { testId: string; error: unknown }) {
  const message = error instanceof Error ? error.message : String(error);
  const disabled = message.includes('503');
  return (
    <div className="px-3 py-6 text-center text-xs text-text-muted" data-testid={testId} data-reason={disabled ? 'disabled' : 'error'}>
      {disabled
        ? 'Durable history is disabled: configure control_plane.storage (sqlite or postgres) to see the 7-day tree.'
        : `Could not load sessions: ${message}`}
    </div>
  );
}

function HarnessBadge({ node }: { node: SessionRow }) {
  return (
    <span
      className="shrink-0 rounded-sm bg-accent/15 px-1 text-[9px] uppercase tracking-wide text-accent"
      data-testid="session-harness"
      title={node.harness_version ? `${node.harness} ${node.harness_version}` : node.harness}
    >
      {node.harness}
    </span>
  );
}

function KindBadge({ node }: { node: SessionRow }) {
  const badge = kindBadge(node);
  return (
    <span
      className={cn('shrink-0 rounded-sm px-1 text-[9px] uppercase tracking-wide', badge.inferred ? 'bg-status-cooling/15 text-status-cooling' : 'bg-line/40 text-text-muted')}
      data-testid="session-kind"
      data-kind={node.kind}
      title={badge.title}
    >
      {badge.label}
    </span>
  );
}

function TreeRowView({ node, nowMs, selected, onSelect }: { node: SessionRow; nowMs: number; selected: boolean; onSelect: (id: string) => void }) {
  return (
    <button
      type="button"
      onClick={() => onSelect(node.id)}
      data-testid="session-row"
      data-selected={selected || undefined}
      title={node.external_id ? `${node.harness} session ${node.external_id}` : `${node.harness} (no session id on the wire)`}
      className={cn(
        'grid w-full grid-cols-[minmax(120px,1fr)_auto_auto_56px_48px] items-center gap-2 border-b border-line/50 px-3 py-1.5 text-left text-xs transition-colors',
        selected ? 'bg-accent/12' : 'hover:bg-accent/[0.06]',
      )}
    >
      <span className="truncate font-mono text-text">{sessionLabel(node)}</span>
      <HarnessBadge node={node} />
      <KindBadge node={node} />
      <span className="text-right tabular-nums text-text-muted" data-testid="session-request-count" title="requests linked to this node (measured)">
        {node.request_count}
      </span>
      <span className="text-right tabular-nums text-text-muted" title={`last activity ${new Date(node.last_seen_ms).toISOString()}`}>
        {fmtAge(node.last_seen_ms, nowMs)}
      </span>
    </button>
  );
}

function TreeDetailView({
  data,
  nowMs,
  onSelect,
}: {
  data: { session: SessionRow; ancestors: SessionRow[]; children: SessionRow[]; requests: HistoryRequest[]; requests_truncated: boolean };
  nowMs: number;
  onSelect: (id: string) => void;
}) {
  const { session, ancestors, children, requests } = data;
  const rollup = useMemo(() => rollupRequests(requests), [requests]);
  const showInFlows = () => {
    flowFilterStore.getState().setSession(session.id);
    navigate('flows');
  };
  return (
    <div className="flex flex-col gap-3" data-testid="session-detail">
      {/* breadcrumb: root … parent › this */}
      <nav className="flex flex-wrap items-center gap-1 text-xs" aria-label="session ancestors" data-testid="session-breadcrumb">
        {[...ancestors].reverse().map((node) => (
          <span key={node.id} className="flex items-center gap-1">
            <button type="button" className="font-mono text-text-muted hover:text-text" onClick={() => onSelect(node.id)}>
              {sessionLabel(node)}
            </button>
            <span className="text-text-muted">›</span>
          </span>
        ))}
        <span className="font-mono text-text">{sessionLabel(session)}</span>
      </nav>

      <Panel raised className="flex flex-wrap items-center gap-x-4 gap-y-1 px-3 py-2">
        <HarnessBadge node={session} />
        <KindBadge node={session} />
        {session.session_kind && (
          <span className="text-xs text-text-muted" data-testid="session-session-kind" title="session kind declared by the harness">
            {session.session_kind}
          </span>
        )}
        <Stat testId="session-stat-requests" label="requests" value={String(session.request_count)} quality="measured" />
        <Stat testId="session-stat-depth" label="depth" value={String(session.depth)} quality="measured" />
        <Stat testId="session-stat-busts" label={`cache busts / ${rollup.count}`} value={rollup.quality === 'unavailable' ? DASH : String(rollup.busts)} quality={rollup.quality} accent={rollup.busts > 0 ? 'text-status-cooling' : undefined} />
        <Stat testId="session-stat-tokens" label="in · out · cached" value={rollup.inputTokens == null ? DASH : `${fmtTokens(rollup.inputTokens)} · ${fmtTokens(rollup.outputTokens)} · ${fmtTokens(rollup.cachedTokens)}`} quality={rollup.inputTokens == null ? 'unavailable' : 'derived'} />
        <span className="text-xs text-text-muted" title={session.client_label ?? 'no client attribution'}>
          {session.client_label ?? DASH}
        </span>
        <button
          type="button"
          onClick={showInFlows}
          className="ml-auto rounded-md border border-line px-2.5 py-1 text-xs text-text-muted transition-colors hover:text-text"
          data-testid="session-show-in-flows"
        >
          show in flows
        </button>
      </Panel>

      {children.length > 0 && (
        <section data-testid="session-children">
          <h2 className="mb-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">sub-sessions · {children.length}</h2>
          <div className="rounded-md border border-line">
            {children.map((child) => (
              <TreeRowView key={child.id} node={child} nowMs={nowMs} selected={false} onSelect={onSelect} />
            ))}
          </div>
        </section>
      )}

      <section data-testid="session-requests">
        <h2 className="mb-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">
          requests · {requests.length}{data.requests_truncated ? ' (newest shown)' : ''}
        </h2>
        {requests.length === 0 ? (
          <div className="px-3 py-3 text-xs italic text-text-muted" data-testid="session-requests-empty">No requests recorded on this node yet.</div>
        ) : (
          <div className="rounded-md border border-line">
            <div className="grid grid-cols-[88px_112px_minmax(120px,1fr)_96px_72px_72px_72px] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
              <span>time</span><span>id</span><span>model</span><span>lineage</span>
              <span className="text-right">in</span><span className="text-right">cached</span><span className="text-right">out</span>
            </div>
            {requests.map((request) => (
              <TreeRequestRowView key={request.id} request={request} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function Stat({ testId, label, value, quality, accent }: { testId: string; label: string; value: string; quality: 'measured' | 'derived' | 'unavailable'; accent?: string }) {
  return (
    <span className="flex flex-col" data-testid={testId} data-quality={quality} title={`${label}: ${quality}`}>
      <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{label}</span>
      <span className={cn('font-mono text-sm tabular-nums', quality === 'unavailable' ? 'text-text-muted' : accent ?? 'text-text')}>{value}</span>
    </span>
  );
}

function TreeRequestRowView({ request }: { request: HistoryRequest }) {
  const lineage = divergenceLabel(request.divergence_kind);
  return (
    <div
      className="grid grid-cols-[88px_112px_minmax(120px,1fr)_96px_72px_72px_72px] items-center gap-2 border-b border-line/50 px-3 py-1.5 text-xs"
      data-testid="session-request"
      data-cache-bust={request.cache_bust === true ? 'true' : 'false'}
      title={request.chain_parent_request_id ? `extends ${request.chain_parent_request_id}` : 'chain start'}
    >
      <span className="tabular-nums text-text-muted">{fmtClock(request.created_at_ms)}</span>
      <span className="truncate font-mono text-text-muted">{shortId(request.id)}</span>
      <span className="truncate">{request.resolved_model ?? request.client_model}</span>
      <span
        className={cn('w-fit rounded-sm px-1 text-[9px] uppercase tracking-wide', lineage.bust ? 'bg-status-cooling/15 text-status-cooling' : 'bg-line/40 text-text-muted')}
        data-testid="session-request-lineage"
        data-kind={request.divergence_kind ?? undefined}
        title={`${lineage.title}${request.divergence_index != null ? ` (item ${request.divergence_index})` : ''}`}
      >
        {lineage.bust ? `bust · ${lineage.label}` : lineage.label}
      </span>
      <span className="text-right tabular-nums text-text-muted">{fmtTokens(request.input_tokens)}</span>
      <span className="text-right tabular-nums text-text-muted">{fmtTokens(request.cached_tokens)}</span>
      <span className="text-right tabular-nums text-text-muted">{fmtTokens(request.output_tokens)}</span>
    </div>
  );
}
