/**
 * activeSessionsModel — pure, DOM-free derivations for the ACTIVE-sessions board
 * (the dashboard's primary live view). Sibling of `sessionsModel.ts`; the view
 * composes these and never invents data.
 *
 * SOURCE — `GET /dashboard/api/sessions/active` (the live SessionHub cut,
 * WS-pushed via `session_update` frames). A row is a session-tree node with
 * activity in the last 15 minutes, its trailing 1/5/10/15-minute request
 * counts, and the newest request stubs.
 *
 * ATTRIBUTION (the operator's "who is this?"):
 *  - `user_id` + `virtual_key_id` ride the row (migration 0012); the view
 *    resolves display names via the users/keys queries and falls back to an
 *    id prefix — never a fabricated name.
 *  - `client_label` (the gap-04 key-hash `key-<hex>` / configured-id / UA) is
 *    the fallback identity when no SQL user/key matches.
 *  - `user_agent`-sourced labels are WEAK (`derived`); everything else strong.
 *
 * DON'T-LIE-WITH-ZEROS:
 *  - window counts are hub-measured (`requests_Nm`) — a real zero is a zero.
 *  - token totals come from `aggregate` (SQL, reported-classes-only) or the
 *    live ring; `null` ⇒ `—`, never a fabricated `0`.
 */
import type { ActiveSessionsResponse, SessionAggregate, SessionRequestStub, SessionRow } from '../../api/types';

/** One row of the board: the hub cut + resolved attribution labels. */
export interface ActiveSessionRow {
  session: SessionRow;
  requests: SessionRequestStub[];
  requests_1m: number;
  requests_5m: number;
  requests_10m: number;
  requests_15m: number;
  aggregate: SessionAggregate | null;
}

/** The unfolded request list entry (a hub stub, display-ready). */
export interface ActiveRequestRow {
  api_call_id: string;
  client_model: string;
  created_at_ms: number;
  status: 'running' | 'completed' | 'failed' | string;
  input_tokens: number | null;
  output_tokens: number | null;
  cached_tokens: number | null;
  error: string | null;
}

/** Project the REST body onto board rows (identity + a stable order). */
export function activeSessionRows(body: ActiveSessionsResponse | undefined): ActiveSessionRow[] {
  if (!body) return [];
  return body.sessions.map((entry) => ({
    session: entry as SessionRow,
    requests: entry.requests,
    requests_1m: entry.requests_1m,
    requests_5m: entry.requests_5m,
    requests_10m: entry.requests_10m,
    requests_15m: entry.requests_15m,
    aggregate: entry.aggregate ?? null,
  }));
}

/**
 * The attribution cell for one session: which user + which key. Resolution is
 * the CALLER's job (it owns the users/keys queries); this model only defines
 * the fallback + strength rules.
 */
export function sessionAttribution(
  session: SessionRow,
  userName: (id: string | null) => string | null,
  keyLabel: (id: string | null) => string | null,
): { user: string; key: string; strong: boolean } {
  const user = userName(session.user_id ?? null) ?? DASH;
  const key = keyLabel(session.virtual_key_id ?? null) ?? session.client_label ?? DASH;
  return { user, key, strong: session.virtual_key_id != null || session.user_id != null };
}

const DASH = '—';

/** Sum the reported token classes of a session's LIVE ring stubs. */
export function ringTokenTotals(requests: SessionRequestStub[]): {
  input: number | null;
  output: number | null;
  cached: number | null;
} {
  let input: number | null = null;
  let output: number | null = null;
  let cached: number | null = null;
  for (const stub of requests) {
    if (stub.input_tokens != null) input = (input ?? 0) + stub.input_tokens;
    if (stub.output_tokens != null) output = (output ?? 0) + stub.output_tokens;
    if (stub.cached_tokens != null) cached = (cached ?? 0) + stub.cached_tokens;
  }
  return { input, output, cached };
}

/** Prefer the durable lifetime aggregate; fall back to the live ring sums. */
export function tokenTotals(
  aggregate: SessionAggregate | null,
  requests: SessionRequestStub[],
): { input: number | null; output: number | null; cached: number | null; quality: 'measured' | 'derived' | 'unavailable' } {
  if (aggregate && (aggregate.input_tokens != null || aggregate.output_tokens != null || aggregate.cached_tokens != null)) {
    return {
      input: aggregate.input_tokens ?? null,
      output: aggregate.output_tokens ?? null,
      cached: aggregate.cached_tokens ?? null,
      quality: 'measured',
    };
  }
  const ring = ringTokenTotals(requests);
  const hasAny = ring.input != null || ring.output != null || ring.cached != null;
  return { ...ring, quality: hasAny ? 'derived' : 'unavailable' };
}
