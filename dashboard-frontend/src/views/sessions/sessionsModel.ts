/**
 * sessionsModel — pure, DOM-free derivations for the Sessions view (sibling of the other surface
 * models). The view composes these; it never invents data.
 *
 * SOURCE — the durable history API (`/history/sessions`, `/history/sessions/:id`). Session nodes
 * are the gateway's session tree (declared by the harness on the wire, or inferred when a request
 * started a new conversation inside its parent). Requests carry their chain lineage: the
 * predecessor, the divergence kind, and whether it fell inside the predecessor's prefix (a
 * cache bust).
 *
 * DATA-QUALITY INVARIANTS:
 *  - a node's request count is `measured` (the durable counter) — `0` for a placeholder parent seen
 *    only through its child is a REAL zero (the node has served no request yet), rendered as `0`;
 *  - the cache-bust count is DERIVED over the requests actually listed (the newest N), so it is
 *    labelled with the sample size rather than presented as the session total;
 *  - token totals sum only reported classes; a session whose listed requests never reported usage
 *    renders `—`, never a fabricated `0`.
 */
import type { DivergenceKind, HistoryRequest, SessionRow } from '../../api/types';

export type Quality = 'measured' | 'derived' | 'unavailable';

/** Compact label for a session node: the harness's own id when declared, else the gateway id. */
export function sessionLabel(node: SessionRow): string {
  const id = node.external_id ?? node.id;
  return id.length > 18 ? `${id.slice(0, 8)}…${id.slice(-6)}` : id;
}

/** `declared` nodes came from the wire; `inferred` ones were opened by the gateway. */
export function kindBadge(node: SessionRow): { label: string; title: string; inferred: boolean } {
  const inferred = node.kind === 'inferred';
  return {
    label: inferred ? 'inferred' : 'declared',
    title: inferred
      ? 'opened by the gateway: this request started a new conversation inside its parent'
      : 'the harness put this session id on the wire',
    inferred,
  };
}

/** Relative age of a node's last activity (`12s`, `4m`, `3h`, `2d`). */
export function fmtAge(ms: number, nowMs: number): string {
  const delta = Math.max(0, nowMs - ms);
  if (delta < 60_000) return `${Math.round(delta / 1000)}s`;
  if (delta < 3_600_000) return `${Math.round(delta / 60_000)}m`;
  if (delta < 86_400_000) return `${Math.round(delta / 3_600_000)}h`;
  return `${Math.round(delta / 86_400_000)}d`;
}

export interface RequestRollup {
  /** Requests in the listed sample. */
  count: number;
  /** Cache-busting requests in the sample (derived over the sample, not the session total). */
  busts: number;
  /** Divergence-kind histogram over the sample. */
  kinds: Partial<Record<DivergenceKind, number>>;
  /** Summed input/output/cached tokens over requests that REPORTED them; `null` when none did. */
  inputTokens: number | null;
  outputTokens: number | null;
  cachedTokens: number | null;
  quality: Quality;
}

/** Roll up the listed requests of a node. */
export function rollupRequests(requests: HistoryRequest[]): RequestRollup {
  if (requests.length === 0) {
    return { count: 0, busts: 0, kinds: {}, inputTokens: null, outputTokens: null, cachedTokens: null, quality: 'unavailable' };
  }
  const kinds: Partial<Record<DivergenceKind, number>> = {};
  let busts = 0;
  let input: number | null = null;
  let output: number | null = null;
  let cached: number | null = null;
  for (const r of requests) {
    if (r.cache_bust === true) busts += 1;
    if (r.divergence_kind) kinds[r.divergence_kind] = (kinds[r.divergence_kind] ?? 0) + 1;
    if (r.input_tokens != null) input = (input ?? 0) + r.input_tokens;
    if (r.output_tokens != null) output = (output ?? 0) + r.output_tokens;
    if (r.cached_tokens != null) cached = (cached ?? 0) + r.cached_tokens;
  }
  return { count: requests.length, busts, kinds, inputTokens: input, outputTokens: output, cachedTokens: cached, quality: 'derived' };
}

/** Copy for a divergence kind (table chips + the inspector's chain tab). */
export function divergenceLabel(kind: DivergenceKind | null | undefined): { label: string; bust: boolean; title: string } {
  switch (kind) {
    case 'append':
      return { label: 'append', bust: false, title: 'extends the predecessor: only new items were added' };
    case 'instructions_changed':
      return { label: 'instructions', bust: true, title: 'the system/instructions block changed inside the shared prefix' };
    case 'tools_changed':
      return { label: 'tools', bust: true, title: 'the tool list changed inside the shared prefix' };
    case 'history_rewritten':
      return { label: 'rewritten', bust: true, title: 'earlier conversation history changed or was removed' };
    case 'new_chain':
      return { label: 'new', bust: false, title: 'the start of a conversation: nothing in common with a known chain' };
    default:
      return { label: '—', bust: false, title: 'lineage not computed' };
  }
}

/** Order root nodes by last activity, newest first (the API already does; keep it deterministic). */
export function sortByActivity(nodes: SessionRow[]): SessionRow[] {
  return [...nodes].sort((a, b) => b.last_seen_ms - a.last_seen_ms || a.id.localeCompare(b.id));
}
