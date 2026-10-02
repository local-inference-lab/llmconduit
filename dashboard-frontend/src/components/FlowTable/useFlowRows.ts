/**
 * Merges the two sources of flow rows into ONE filtered, newest-on-top list for the table:
 *   - the LIVE WS store (`flows` Map keyed by api_call_id + `flowOrder`, newest-prepended) —
 *     the authoritative live state the socket feeds (snapshot + flow_status + usage frames);
 *   - the `/flows` TanStack query — the REST list, invalidated on accepted `flow` frames
 *     (connection.ts finding 10). It seeds rows the store has not seen and stays the source of
 *     server-only fields (terminal_reason, cost roll-up) until a live frame supersedes them.
 *
 * The store wins on volatile data except that a terminal status always beats an `open` status,
 * regardless of which transport delivered it first. `flowOrder` defines the
 * live-row identity, but the MERGED union is sorted GLOBALLY by `started_ms` descending so a
 * newer REST-only row can never sort below an older live row (finding 4) — newest-on-top holds
 * regardless of source. Filtering + the distinct model/upstream option lists are derived here so
 * the table and filter bar share one computation.
 */
import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { FlowSummary } from '../../api/types';
import { useDashboard } from '../../store/hooks';
import { getConnection, queryKeys } from '../../api/connection';
import { pickAttempts, sameAttempts } from '../../api/attempts';
import type { FlowFilters } from './filterTypes';

export interface FlowRowsResult {
  /** Filtered rows, newest-on-top (the array the virtualizer renders). */
  rows: FlowSummary[];
  /** Total rows BEFORE filtering (for the "shown / total" readout). */
  total: number;
  /** Distinct model values present (requested or served) for the filter chips. */
  models: string[];
  /** Distinct upstream targets present for the filter chips. */
  upstreams: string[];
  /** Gap 15 — distinct `client_label`s present, for the per-client filter chips. */
  clients: string[];
  /** Sessions — distinct detected harnesses present, for the harness filter chips. */
  harnesses: string[];
}

/** Union the live store rows (authoritative) with REST-only rows, newest-on-top. */
function mergeRows(
  order: string[],
  flows: Map<string, FlowSummary>,
  queryFlows: FlowSummary[],
): FlowSummary[] {
  // Index the REST list by api_call_id so a live row can re-adopt the server-only roll-up
  // fields the WS frame does not carry (finding 5).
  const restById = new Map(queryFlows.map((f) => [f.api_call_id, f]));
  const seen = new Set<string>();
  const merged: FlowSummary[] = [];
  // Live rows first, in store order. The live row's status/usage WIN, but the REST-authoritative
  // request-line + roll-up fields (endpoint/method/uri, finished/elapsed, cost, terminal_reason)
  // are backfilled from the REST row — so a WS-CREATED row stops showing placeholder method/uri
  // once the authoritative REST row arrives (finding 3), and a live update never blanks the
  // server's cost/terminal_reason (finding 5).
  for (const id of order) {
    const f = flows.get(id);
    if (f) {
      merged.push(mergeLiveWithRest(f, restById.get(id)));
      seen.add(id);
    }
  }
  // REST-only rows the store has not seen yet.
  for (const f of queryFlows) {
    if (!seen.has(f.api_call_id)) merged.push(f);
  }
  // Sort the COMBINED union GLOBALLY by `started_ms` descending so newest-on-top holds across
  // BOTH sources — a newer REST-only row must not sort below an older live row just because the
  // live rows were emitted first (finding 4). Stable tiebreak keeps deterministic ordering for
  // equal timestamps. (`flowOrder` already tracks live newest-prepended, so for an all-live list
  // this preserves the existing order.)
  merged.sort((a, b) => b.started_ms - a.started_ms);
  return merged;
}

/**
 * Reconciles a live store row with its REST counterpart. The live frame is the FRESHEST source of
 * the volatile flow STATE (`usage` and normally `status`). Status is reconciled monotonically:
 * terminal beats `open`, so a delayed/stale WS frame cannot resurrect a completed REST row and a
 * stale REST refetch cannot reopen a terminal WS row. But a WS-created row (one the
 * `flow_status` patch minted before the REST list arrived) carries PLACEHOLDERS for the fields the
 * frame cannot author — `method:'POST'`, `uri:''`, and null `finished/elapsed/cost/terminal_reason`
 * — so it kept showing those placeholders even after the authoritative REST row landed (finding 3).
 *
 * Field policy:
 *  - status: terminal beats `open`; if both agree on terminality, LIVE wins.
 *  - usage / started_ms: LIVE wins (the socket owns the live state + stream start).
 *  - method / uri: REST-authoritative request line — REST wins when present (it never changes over a
 *    flow's life, so this only replaces a WS placeholder; falls back to live if REST omitted it).
 *  - model_requested / model_served / upstream_target / response_id: LIVE wins when present, else
 *    REST backfills (a WS-created row may not have learned the served model/target yet).
 *  - finished_ms / elapsed_ms / cost / terminal_reason: roll-up/terminal fields — LIVE wins when it
 *    HAS them (a completing flow streams elapsed/finished), else the REST roll-up backfills, so a
 *    live update never blanks the server's values (finding 5).
 *  - cost_confidence: PAIRED with `cost` (gap 07 review round 1, finding 4). It is non-nullable
 *    (always a present tag, defaulting to `unavailable`), so it cannot use `??`. A WS-created row
 *    carries the store DEFAULT `unavailable` until the backend sends the real tag — so it follows
 *    `cost`'s source: when the LIVE row authored a real cost (`live.cost != null`) its own
 *    confidence wins; otherwise the REST roll-up's confidence backfills ALONGSIDE the REST `cost`,
 *    so the tag never stays stuck at `unavailable` after the server reports a confident/estimated
 *    cost. This keeps the rendered `$` and its confidence label CONSISTENT (same source).
 *
 * Returns the live row UNCHANGED when nothing actually differs, to avoid needless object churn (and
 * keep referential stability for the virtualizer).
 */
function mergeLiveWithRest(live: FlowSummary, rest: FlowSummary | undefined): FlowSummary {
  if (!rest) return live;
  // `cost` is live-first, REST-backfilled. `cost_confidence` follows the SAME source so the dollar
  // figure and its confidence label always agree: if the live frame supplied the cost, use its tag;
  // else adopt the REST roll-up's tag together with the REST cost.
  const liveAuthoredCost = live.cost != null;
  const status = live.status === 'open' && rest.status !== 'open' ? rest.status : live.status;
  const merged: FlowSummary = {
    ...live,
    status,
    // REST-authoritative request line: replace a WS placeholder with the real value.
    method: rest.method || live.method,
    uri: rest.uri || live.uri,
    // Live-first, REST-backfilled correlation/model fields.
    response_id: live.response_id ?? rest.response_id ?? null,
    model_requested: live.model_requested ?? rest.model_requested ?? null,
    model_served: live.model_served ?? rest.model_served ?? null,
    upstream_target: live.upstream_target ?? rest.upstream_target ?? null,
    // Live-first, REST-backfilled roll-up / terminal fields.
    finished_ms: live.finished_ms ?? rest.finished_ms ?? null,
    elapsed_ms: live.elapsed_ms ?? rest.elapsed_ms ?? null,
    cost: live.cost ?? rest.cost ?? null,
    cost_confidence: liveAuthoredCost ? live.cost_confidence : rest.cost_confidence,
    terminal_reason: live.terminal_reason ?? rest.terminal_reason ?? null,
    // Gap 02/03 (gap 10b) — the projected spine fields, merged live-first / REST-backed so the
    // measured latency waterfall (gap 10) + attempt trace (gap 11) light up for a MERGED row.
    // The REST `/flows` row now projects them too; a live frame's freshest value wins, else the
    // REST projection backfills. `??` keeps an unmeasured phase ABSENT (never `0`) — the breakdown
    // renders `—`, never a fabricated segment. (`PhaseTimings` fields are flattened via `extends`.)
    ingress_ms: live.ingress_ms ?? rest.ingress_ms,
    normalization_done_ms: live.normalization_done_ms ?? rest.normalization_done_ms,
    routing_decision_ms: live.routing_decision_ms ?? rest.routing_decision_ms,
    first_content_delta_ms: live.first_content_delta_ms ?? rest.first_content_delta_ms,
    stream_end_ms: live.stream_end_ms ?? rest.stream_end_ms,
    finalize_ms: live.finalize_ms ?? rest.finalize_ms,
    // `attempts` is an ARRAY, so it CANNOT use the scalar `live ?? rest` rule the phase epochs use:
    // a snapshot summary serializes `attempts: []` ("no attempt recorded yet") rather than omitting
    // it, and `??` would treat that `[]` as authoritative — BLOCKING the REST backfill of a populated
    // gap-11 trace (gap 10b review round 2). `pickAttempts` instead lets a NON-EMPTY list (either
    // side) win and treats an empty array as ABSENT for backfill, so a snapshot `[]` never wrongly
    // empties the merged trace.
    attempts: pickAttempts(live.attempts, rest.attempts),
    first_upstream_byte_ms: live.first_upstream_byte_ms ?? rest.first_upstream_byte_ms,
    // Gap 04/15: the client attribution is an IMMUTABLE per-flow identity derived ONCE pre-redaction
    // (it never changes over a flow's life). Live-first / REST-backfilled like the other scalars — so
    // a WS-created row (which may not carry it) re-adopts the REST `/flows` projection. `client_source`
    // follows `client_label`'s source so the label + its strength tag always agree.
    client_label: live.client_label ?? rest.client_label ?? null,
    client_source: live.client_label != null ? live.client_source : (rest.client_source ?? live.client_source ?? null),
    // Sessions: the harness/session facts are immutable per flow (computed once at ingress);
    // live-first / REST-backfilled like the attribution.
    harness: live.harness ?? rest.harness ?? null,
    harness_version: live.harness_version ?? rest.harness_version ?? null,
    session_id: live.session_id ?? rest.session_id ?? null,
    chain_parent_request_id: live.chain_parent_request_id ?? rest.chain_parent_request_id ?? null,
    divergence_kind: live.divergence_kind ?? rest.divergence_kind ?? null,
    cache_bust: live.cache_bust ?? rest.cache_bust ?? null,
  };
  return shallowEqualSummary(live, merged) ? live : merged;
}

/** True when every `FlowSummary` field is identical (so the merge can return `live` unchanged). */
function shallowEqualSummary(a: FlowSummary, b: FlowSummary): boolean {
  return (
    a.status === b.status &&
    a.method === b.method &&
    a.uri === b.uri &&
    (a.response_id ?? null) === (b.response_id ?? null) &&
    (a.model_requested ?? null) === (b.model_requested ?? null) &&
    (a.model_served ?? null) === (b.model_served ?? null) &&
    (a.upstream_target ?? null) === (b.upstream_target ?? null) &&
    (a.finished_ms ?? null) === (b.finished_ms ?? null) &&
    (a.elapsed_ms ?? null) === (b.elapsed_ms ?? null) &&
    (a.cost ?? null) === (b.cost ?? null) &&
    // Gap 07 finding 4: the confidence tag is part of row identity — a backfill that only
    // changes `cost_confidence` (e.g. unavailable → estimated once the server tag lands) MUST
    // produce a new object so the row re-renders with the corrected label.
    a.cost_confidence === b.cost_confidence &&
    (a.terminal_reason ?? null) === (b.terminal_reason ?? null) &&
    // Gap 02/03 (gap 10b): the projected spine fields are part of row identity too — a REST
    // backfill that only adds timing/attempt data (live row had none) MUST produce a new object
    // so the row re-renders and the gap-10/11 consumers see it (else the projected data is
    // discarded). Phase epochs compare `?? null` so an absent↔null pair is treated as equal (no
    // spurious re-render); `attempts` uses `sameAttempts` (see below — `[]` and absent are equal).
    (a.ingress_ms ?? null) === (b.ingress_ms ?? null) &&
    (a.normalization_done_ms ?? null) === (b.normalization_done_ms ?? null) &&
    (a.routing_decision_ms ?? null) === (b.routing_decision_ms ?? null) &&
    (a.first_content_delta_ms ?? null) === (b.first_content_delta_ms ?? null) &&
    (a.stream_end_ms ?? null) === (b.stream_end_ms ?? null) &&
    (a.finalize_ms ?? null) === (b.finalize_ms ?? null) &&
    (a.first_upstream_byte_ms ?? null) === (b.first_upstream_byte_ms ?? null) &&
    // Gap 04/15: the client attribution is part of row identity — a REST backfill that adds the
    // `client_label`/`client_source` (a WS-created row had none) MUST produce a new object so the
    // CLIENT cell + the "by client" roll-up re-render with the attribution (else it's discarded).
    (a.client_label ?? null) === (b.client_label ?? null) &&
    (a.client_source ?? null) === (b.client_source ?? null) &&
    // Sessions: part of row identity so a REST backfill that adds them re-renders the row.
    (a.harness ?? null) === (b.harness ?? null) &&
    (a.harness_version ?? null) === (b.harness_version ?? null) &&
    (a.session_id ?? null) === (b.session_id ?? null) &&
    (a.chain_parent_request_id ?? null) === (b.chain_parent_request_id ?? null) &&
    (a.divergence_kind ?? null) === (b.divergence_kind ?? null) &&
    (a.cache_bust ?? null) === (b.cache_bust ?? null) &&
    // `attempts` is compared via `sameAttempts`: EMPTY and ABSENT are the same "no trace" state
    // (a snapshot's `[]` vs the merge's normalized `undefined` must NOT churn a new object), but a
    // backfilled NON-EMPTY list is a new reference ⇒ unequal, so the row re-renders and the
    // gap-10/11 consumers see the trace (gap 10b review round 2).
    sameAttempts(a.attempts, b.attempts)
  );
}

function applyFilters(rows: FlowSummary[], f: FlowFilters): FlowSummary[] {
  return rows.filter((row) => {
    if (f.status && row.status !== f.status) return false;
    if (f.model && row.model_requested !== f.model && row.model_served !== f.model) return false;
    if (f.upstream && row.upstream_target !== f.upstream) return false;
    // Gap 15: the per-client facet matches the row's `client_label` exactly. An unattributed row
    // (no label) never matches a client filter (it can't be claimed by a client).
    if (f.client && row.client_label !== f.client) return false;
    // Sessions: exact matches on the detected harness / the linked session node; the cache-bust
    // facet keeps only rows whose divergence fell inside the predecessor (a `true` flag — a row
    // without lineage is never claimed as a bust).
    if (f.harness && row.harness !== f.harness) return false;
    if (f.session && row.session_id !== f.session) return false;
    if (f.cacheBust === true && row.cache_bust !== true) return false;
    return true;
  });
}

function distinct(rows: FlowSummary[], pick: (r: FlowSummary) => (string | null | undefined)[]): string[] {
  const set = new Set<string>();
  for (const r of rows) for (const v of pick(r)) if (v) set.add(v);
  return [...set].sort();
}

/**
 * Distinct `client_label`s ordered by DESCENDING flow volume (gap 15 review MEDIUM). Client attribution
 * is HIGH-CARDINALITY (unlike the bounded model/upstream sets) — thousands of distinct keys would render
 * thousands of chips and wrap the filter bar unusable. The FilterBar caps to the top-N by volume off
 * THIS order, so the busiest clients are the offered chips; first-seen order breaks ties for stability.
 */
function clientsByVolume(rows: FlowSummary[]): string[] {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const c = r.client_label;
    if (c) counts.set(c, (counts.get(c) ?? 0) + 1);
  }
  // Descending count; insertion order (first-seen) is the stable tiebreak (Map preserves it).
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([label]) => label);
}

export function useFlowRows(filters: FlowFilters): FlowRowsResult {
  const order = useDashboard((s) => s.flowOrder);
  const flows = useDashboard((s) => s.flows);
  // Time-travel: while seeking (D11 paused on a historical cut), the store holds the FROZEN
  // snapshot summaries. Merging the live `/flows` REST list (or live WS rows) here would leak
  // flows/state from AFTER the seeked timestamp into the frozen view, so we render the snapshot
  // rows ALONE while seeking and resume the live merge on LIVE (HIGH finding 1).
  const seeking = useDashboard((s) => s.connection === 'seeking');
  const { client } = getConnection();

  // The REST list — the PRODUCTION data source for the table. It seeds rows the live store has
  // not seen and carries the server-only roll-up fields (cost/terminal_reason). WS `flow` frames
  // invalidate `queryKeys.flows` (connection.ts), so it refetches when the server list changes.
  // Enabled for BOTH the real backend (where it is authoritative) and the mock (its `mockFetch`
  // answers `/flows`). Component tests that drive the store directly seed `resetWorld()` with a
  // real bootstrap and no live server, so the fetch simply fails/stays empty without churn.
  // DISABLED while seeking: the REST list is live (post-seek) data that must not bleed into the
  // frozen snapshot (finding 1).
  const query = useQuery({
    queryKey: queryKeys.flows,
    queryFn: () => client.flows(),
    enabled: !seeking,
  });
  // Ignore any cached REST result while seeking so the frozen snapshot stands alone.
  const queryData = seeking ? undefined : query.data;

  const merged = useMemo(
    () => mergeRows(order, flows, queryData?.flows ?? []),
    [order, flows, queryData],
  );
  const rows = useMemo(() => applyFilters(merged, filters), [merged, filters]);
  const models = useMemo(() => distinct(merged, (r) => [r.model_requested, r.model_served]), [merged]);
  const upstreams = useMemo(() => distinct(merged, (r) => [r.upstream_target]), [merged]);
  // Gap 15: the distinct `client_label`s for the per-client filter, ordered by DESCENDING volume so the
  // FilterBar can cap to the top-N busiest (high-cardinality defense — review MEDIUM). Unattributed rows
  // have no label ⇒ contribute nothing (an absent attribution is never a filterable client).
  const clients = useMemo(() => clientsByVolume(merged), [merged]);
  const harnesses = useMemo(() => distinct(merged, (r) => [r.harness]), [merged]);

  return { rows, total: merged.length, models, upstreams, clients, harnesses };
}
