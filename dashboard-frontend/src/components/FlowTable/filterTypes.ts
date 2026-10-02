/** Filter model for the FlowTable, kept out of the component file so react-refresh stays happy. */
import type { FlowStatus } from '../../api/types';

export interface FlowFilters {
  status: FlowStatus | null;
  model: string | null;
  upstream: string | null;
  /** Gap 15 — the per-client facet: the `client_label` to scope the table (+ roll-up) to one client. */
  client: string | null;
  /** Sessions — the detected harness profile name. */
  harness: string | null;
  /** Sessions — a gateway session node id (set by the Sessions view cross-link). */
  session: string | null;
  /** Sessions — `true` scopes the table to cache-busting flows only. */
  cacheBust: boolean | null;
}

export const EMPTY_FILTERS: FlowFilters = {
  status: null, model: null, upstream: null, client: null, harness: null, session: null, cacheBust: null,
};
