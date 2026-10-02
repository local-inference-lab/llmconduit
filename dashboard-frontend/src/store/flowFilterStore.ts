/**
 * The SHARED FlowTable filter (D12). The FlowTable (D10) used to own its filter in component
 * `useState`, which made it unreachable from the other views. D12's Topology (click a provider
 * node) and Sankey (click a flow band) must be able to drive that same filter — "click here →
 * see those flows" — so the filter is hoisted into this tiny vanilla-zustand store (the same
 * `createStore` + `useSyncExternalStore` bridge the dashboard/auth stores use). The FlowTable
 * reads it (it remains the ONLY writer of the chip toggles); Topology/Sankey set it, then
 * `navigate('flows')` so the cross-link lands on the table already filtered.
 *
 * It is deliberately separate from `dashboardStore` (the live WS/seek state): the filter is pure
 * UI selection that must survive view switches and is NOT cleared by a seek/teardown of the live
 * data. Keeping it out of `dashboardStore` also means a filter change never invalidates the live
 * slices' selectors.
 */
import { createStore } from 'zustand/vanilla';
import { EMPTY_FILTERS, type FlowFilters } from '../components/FlowTable/filterTypes';

export interface FlowFilterState {
  filters: FlowFilters;
  /** Replace the whole filter set (the FlowTable's FilterBar onChange; it owns chip toggles). */
  setFilters: (next: FlowFilters) => void;
  /**
   * Cross-link from a topology node: SET the upstream filter to that target (finding 10). A
   * cross-link is "click here → SEE those flows", so it deterministically SETS the facet — it does
   * NOT toggle off on a repeat (chip-toggle semantics live in the FilterBar, the only chip writer).
   */
  setUpstream: (upstream: string) => void;
  /** Cross-link from a sankey band: SET the model filter to that model (deterministic — finding 10). */
  setModel: (model: string) => void;
  /** Gap 15 — cross-link from the "by client" roll-up: SET the client filter to that `client_label`. */
  setClient: (client: string) => void;
  /** Sessions — cross-link from the Sessions view: SET the session facet to that node id. */
  setSession: (session: string) => void;
  /** Sessions — cross-link: SET the harness facet. */
  setHarness: (harness: string) => void;
  /** Clear every facet. */
  clear: () => void;
}

export const flowFilterStore = createStore<FlowFilterState>((set) => ({
  filters: EMPTY_FILTERS,
  setFilters: (filters) => set({ filters }),
  // Cross-link setters are DETERMINISTIC (finding 10): a click SETS the facet so the table always
  // lands filtered to what was clicked. (The FilterBar owns the toggle-off-on-repeat chip behavior.)
  setUpstream: (upstream) => set((s) => ({ filters: { ...s.filters, upstream } })),
  setModel: (model) => set((s) => ({ filters: { ...s.filters, model } })),
  // Gap 15: a "by client" roll-up click SETS the client facet so the table lands scoped to that
  // client (the FilterBar owns the toggle-off-on-repeat chip behavior, like the other facets).
  setClient: (client) => set((s) => ({ filters: { ...s.filters, client } })),
  setSession: (session) => set((s) => ({ filters: { ...s.filters, session } })),
  setHarness: (harness) => set((s) => ({ filters: { ...s.filters, harness } })),
  clear: () => set({ filters: EMPTY_FILTERS }),
}));

export type FlowFilterStore = typeof flowFilterStore;
