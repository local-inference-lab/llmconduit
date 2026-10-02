import { describe, it, expect, beforeEach } from 'vitest';
import { flowFilterStore } from './flowFilterStore';
import { EMPTY_FILTERS } from '../components/FlowTable/filterTypes';

describe('flowFilterStore — the shared FlowTable filter (D12 cross-link)', () => {
  beforeEach(() => {
    flowFilterStore.getState().clear();
  });

  it('starts empty', () => {
    expect(flowFilterStore.getState().filters).toEqual(EMPTY_FILTERS);
  });

  it('setUpstream SETS the target deterministically (a click filters; no toggle-off) — finding 10', () => {
    flowFilterStore.getState().setUpstream('vllm-a');
    expect(flowFilterStore.getState().filters.upstream).toBe('vllm-a');
    // A cross-link is "click → SEE those flows": a repeat re-SETS the same value, never clears it.
    flowFilterStore.getState().setUpstream('vllm-a');
    expect(flowFilterStore.getState().filters.upstream).toBe('vllm-a');
    // A different target replaces it.
    flowFilterStore.getState().setUpstream('vllm-b');
    expect(flowFilterStore.getState().filters.upstream).toBe('vllm-b');
  });

  it('setModel / setUpstream set their facets independently', () => {
    flowFilterStore.getState().setModel('gpt-4o');
    flowFilterStore.getState().setUpstream('vllm-a');
    expect(flowFilterStore.getState().filters).toEqual({ ...EMPTY_FILTERS, status: null, model: 'gpt-4o', upstream: 'vllm-a', client: null });
  });

  it('setClient SETS the client facet deterministically (gap 15 cross-link)', () => {
    flowFilterStore.getState().setClient('key-9f3a1c0b2d4e');
    expect(flowFilterStore.getState().filters.client).toBe('key-9f3a1c0b2d4e');
    // Other facets untouched; a different client replaces it (deterministic SET, no toggle-off).
    expect(flowFilterStore.getState().filters.upstream).toBeNull();
    flowFilterStore.getState().setClient('svc-checkout');
    expect(flowFilterStore.getState().filters.client).toBe('svc-checkout');
  });

  it('setFilters replaces the whole set; clear resets', () => {
    flowFilterStore.getState().setFilters({ ...EMPTY_FILTERS, status: 'open', model: 'm', upstream: 'u', client: 'c' });
    expect(flowFilterStore.getState().filters).toEqual({ ...EMPTY_FILTERS, status: 'open', model: 'm', upstream: 'u', client: 'c' });
    flowFilterStore.getState().clear();
    expect(flowFilterStore.getState().filters).toEqual(EMPTY_FILTERS);
  });
});
