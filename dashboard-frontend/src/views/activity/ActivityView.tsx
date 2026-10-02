/**
 * ActivityView — who is using the gateway: requests, failures and tokens per user and per
 * key over a window, with a per-key request sparkline. Names come from the accounts API
 * (an admin sees everyone; a user sees their own keys); unattributed traffic (no key) is
 * listed as such rather than hidden.
 */
import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import { useAuth } from '../../store/hooks';
import { Panel } from '../../components/ui/Panel';
import { LineChart } from '../../viz/LineChart';
import { fmtTokens } from '../../components/FlowTable/format';
import { cn } from '../../lib/cn';
import { activityRows } from '../throughput/throughputModel';

const DASH = '—';
const WINDOWS: Array<{ label: string; ms: number; bucketSecs: number }> = [
  { label: '1h', ms: 60 * 60_000, bucketSecs: 300 },
  { label: '24h', ms: 24 * 60 * 60_000, bucketSecs: 3600 },
  { label: '7d', ms: 7 * 24 * 60 * 60_000, bucketSecs: 6 * 3600 },
];

export function ActivityView() {
  const { client } = getConnection();
  const currentUser = useAuth((s) => s.user);
  const isAdmin = useAuth((s) => s.user?.is_admin ?? s.authMode !== 'users');
  const [windowIdx, setWindowIdx] = useState(1);
  const win = WINDOWS[windowIdx]!;
  const sinceMs = Date.now() - win.ms;
  const activity = useQuery({
    queryKey: [...queryKeys.activity, win.label],
    queryFn: () => client.historyActivity({ since_ms: sinceMs, bucket_secs: win.bucketSecs }),
    refetchInterval: 30_000,
  });
  const users = useQuery({ queryKey: queryKeys.users, queryFn: () => client.listUsers(), enabled: isAdmin, retry: false });
  const keys = useQuery({ queryKey: queryKeys.keys('all'), queryFn: () => client.listKeys(isAdmin ? 'all' : undefined), retry: false });
  const authUsers = useQuery({ queryKey: queryKeys.authUsers, queryFn: () => client.authUsers(), retry: false });
  const authKeys = useQuery({ queryKey: queryKeys.authApiKeys, queryFn: () => client.authApiKeys(), retry: false });
  const rows = useMemo(() => activityRows(activity.data?.buckets ?? []), [activity.data]);
  const userName = (id: string | null) => (id == null ? 'unattributed' : authUsers.data?.users.find((u) => u.id === id)?.display_name ?? users.data?.users.find((u) => u.id === id)?.username ?? (currentUser?.id === id ? currentUser.username : null) ?? id.slice(0, 8));
  const keyLabel = (id: string | null) => (id == null ? DASH : authKeys.data?.api_keys.find((k) => k.id === id)?.name ?? keys.data?.keys.find((k) => k.id === id)?.label ?? id.slice(0, 8));
  const totals = rows.reduce((a, r) => ({ requests: a.requests + r.requests, failed: a.failed + r.failed }), { requests: 0, failed: 0 });

  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-auto p-4" data-testid="activity-view">
      <div className="mb-3 flex flex-wrap items-baseline gap-2">
        <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">activity</h1>
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">requests · failures · tokens per user and key</span>
        <div className="ml-auto flex items-center gap-1" data-testid="activity-window">
          {WINDOWS.map((w, i) => (
            <button key={w.label} type="button" onClick={() => setWindowIdx(i)} className={cn('rounded-full border px-2.5 py-0.5 text-xs', i === windowIdx ? 'border-accent/40 bg-accent/15 text-accent' : 'border-line bg-panel text-text-muted')}>
              {w.label}
            </button>
          ))}
        </div>
      </div>
      {activity.isError && (
        <Panel className="mb-3 px-3 py-3 text-xs text-text-muted" data-testid="activity-unavailable">
          {String(activity.error).includes('503') ? 'Durable history is disabled: configure control_plane.storage (sqlite or postgres).' : `Could not load activity: ${String(activity.error)}`}
        </Panel>
      )}
      <Panel raised className="mb-3 flex flex-wrap gap-6 px-3 py-2" data-testid="activity-headline">
        <Stat testId="act-requests" label={`requests · ${win.label}`} value={totals.requests ? String(totals.requests) : DASH} quality={totals.requests ? 'measured' : 'unavailable'} />
        <Stat testId="act-failed" label="failed" value={totals.requests ? String(totals.failed) : DASH} quality={totals.requests ? 'measured' : 'unavailable'} />
        <Stat testId="act-principals" label="users · keys" value={rows.length ? `${new Set(rows.map((r) => r.user_id)).size} · ${new Set(rows.map((r) => r.virtual_key_id)).size}` : DASH} quality={rows.length ? 'measured' : 'unavailable'} />
      </Panel>
      {rows.length === 0 ? (
        <Panel className="px-3 py-3 text-xs text-text-muted" data-testid="activity-empty">No requests in this window.</Panel>
      ) : (
        <div className="rounded-md border border-line">
          <div className="grid grid-cols-[minmax(100px,1fr)_minmax(100px,1fr)_72px_72px_64px_80px_80px_80px_160px] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
            <span>user</span><span>key</span><span className="text-right">reqs</span><span className="text-right">failed</span><span className="text-right">err %</span>
            <span className="text-right">in</span><span className="text-right">cached</span><span className="text-right">out</span><span>requests / bucket</span>
          </div>
          {rows.map((row) => (
            <div key={`${row.user_id}/${row.virtual_key_id}`} className="grid grid-cols-[minmax(100px,1fr)_minmax(100px,1fr)_72px_72px_64px_80px_80px_80px_160px] items-center gap-2 border-b border-line/50 px-3 py-1 text-xs" data-testid="activity-row" data-user={row.user_id ?? 'none'}>
              <span className={cn('truncate', row.user_id == null && 'italic text-text-muted')} data-testid="activity-user">{userName(row.user_id)}</span>
              <span className="truncate font-mono text-text-muted" title={row.virtual_key_id ?? undefined} data-testid="activity-key">{keyLabel(row.virtual_key_id)}</span>
              <span className="text-right tabular-nums">{row.requests}</span>
              <span className={cn('text-right tabular-nums', row.failed > 0 && 'text-status-down')}>{row.failed}</span>
              <span className="text-right tabular-nums text-text-muted">{row.error_pct == null ? DASH : row.error_pct.toFixed(1)}</span>
              <span className="text-right tabular-nums text-text-muted">{fmtTokens(row.input_tokens)}</span>
              <span className="text-right tabular-nums text-text-muted">{fmtTokens(row.cached_tokens)}</span>
              <span className="text-right tabular-nums text-text-muted">{fmtTokens(row.output_tokens)}</span>
              <span className="h-8">
                <LineChart height={32} width={160} series={[{ key: 'r', label: 'requests', points: row.series }]} formatY={() => ''} />
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Stat({ testId, label, value, quality }: { testId: string; label: string; value: string; quality: 'measured' | 'derived' | 'unavailable' }) {
  return (
    <span className="flex flex-col" data-testid={testId} data-quality={quality} title={`${label}: ${quality}`}>
      <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{label}</span>
      <span className={cn('font-mono text-lg font-semibold tabular-nums', quality === 'unavailable' ? 'text-text-muted' : 'text-text')}>{value}</span>
    </span>
  );
}
