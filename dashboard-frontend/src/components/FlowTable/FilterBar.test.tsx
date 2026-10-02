/**
 * FilterBar (D12 R5 MED) — the active model/upstream filter must ALWAYS render as a visible,
 * toggle-off-able chip even when its value matches NO row in view (a Topology/Sankey cross-link can
 * set a facet to an idle provider or an aged-out model), and a `clear` control is available whenever
 * any facet is active. Without this, the chip options (derived from rows in view) omit the orphaned
 * value, leaving an invisible filter the user cannot clear.
 */
import { describe, it, expect, afterEach, vi } from 'vitest';
import { render, cleanup, fireEvent, within } from '@testing-library/react';
import { FilterBar } from './FilterBar';
import { EMPTY_FILTERS, type FlowFilters } from './filterTypes';

afterEach(cleanup);

function renderBar(over: Partial<Parameters<typeof FilterBar>[0]> = {}) {
  const onChange = vi.fn();
  const props = {
    filters: EMPTY_FILTERS as FlowFilters,
    models: ['gpt-4o'],
    upstreams: ['vllm-a'],
    clients: [] as string[],
    total: 3,
    shown: 3,
    onChange,
    ...over,
  };
  const r = render(<FilterBar {...props} />);
  return { ...r, onChange };
}

/** The chips inside a labelled group (status/model/upstream). */
function groupChips(container: HTMLElement, label: string): HTMLElement[] {
  const group = within(container).getByText(label).parentElement as HTMLElement;
  return Array.from(group.querySelectorAll('button'));
}

describe('FilterBar — active filter is always visible + clearable (D12 R5 MED)', () => {
  it('renders an active chip for a SELECTED model that matches NO row in view (cross-link to an off-screen model)', () => {
    // The cross-link set model='claude-x' but the in-view rows only carry 'gpt-4o' → 'claude-x' is
    // NOT among the derived options. It must still appear as an active chip.
    const { container, onChange } = renderBar({
      models: ['gpt-4o'],
      filters: { ...EMPTY_FILTERS, status: null, model: 'claude-x', upstream: null, client: null },
    });
    const chip = within(container).getByText('claude-x');
    expect(chip).toBeTruthy();
    expect(chip.getAttribute('aria-pressed')).toBe('true');
    // Clicking it toggles the filter OFF (clears that facet).
    fireEvent.click(chip);
    expect(onChange).toHaveBeenCalledWith({ ...EMPTY_FILTERS, status: null, model: null, upstream: null, client: null });
  });

  it('renders an active chip for a SELECTED upstream that matches NO row in view', () => {
    const { container } = renderBar({
      upstreams: ['vllm-a'],
      filters: { ...EMPTY_FILTERS, status: null, model: null, upstream: 'idle-provider', client: null },
    });
    const chip = within(container).getByText('idle-provider');
    expect(chip.getAttribute('aria-pressed')).toBe('true');
  });

  it('shows the orphaned active value even when the derived option list is EMPTY (no rows in view)', () => {
    const { container } = renderBar({
      models: [],
      upstreams: [],
      filters: { ...EMPTY_FILTERS, status: null, model: 'claude-x', upstream: null, client: null },
      total: 0,
      shown: 0,
    });
    // The model group renders solely because of the active selection.
    expect(groupChips(container, 'model').map((b) => b.textContent)).toEqual(['claude-x']);
  });

  it('does not duplicate a selected value that is ALSO in the derived options', () => {
    const { container } = renderBar({
      models: ['gpt-4o', 'claude-x'],
      filters: { ...EMPTY_FILTERS, status: null, model: 'claude-x', upstream: null, client: null },
    });
    const labels = groupChips(container, 'model').map((b) => b.textContent);
    expect(labels.filter((l) => l === 'claude-x')).toHaveLength(1);
  });

  it('exposes a `clear` control when any facet is active that resets ALL facets', () => {
    const { container, onChange } = renderBar({
      filters: { ...EMPTY_FILTERS, status: 'open', model: 'gpt-4o', upstream: 'vllm-a', client: null },
    });
    const clear = within(container).getByTestId('flow-filter-clear');
    fireEvent.click(clear);
    expect(onChange).toHaveBeenCalledWith(EMPTY_FILTERS);
  });

  it('hides the `clear` control when no facet is active', () => {
    const { container } = renderBar({ filters: EMPTY_FILTERS });
    expect(within(container).queryByTestId('flow-filter-clear')).toBeNull();
  });
});

describe('FilterBar — client facet is bounded (gap 15 review MEDIUM: high cardinality)', () => {
  // `clients` arrives volume-ordered from useFlowRows; the bar caps to the top-N busiest.
  const many = Array.from({ length: 50 }, (_, i) => `client-${String(i).padStart(2, '0')}`);

  it('caps the client chips to the top-N (NOT one chip per distinct client_label)', () => {
    const { container } = renderBar({ clients: many });
    const chips = groupChips(container, 'client');
    // Bounded — far fewer than the 50 distinct clients (the CLIENT_CHIP_CAP, currently 8).
    expect(chips.length).toBeLessThanOrEqual(8);
    expect(chips.length).toBeGreaterThan(0);
    // Only the busiest (FIRST in the volume order) are offered.
    expect(chips.map((b) => b.textContent)).toContain('client-00');
    expect(chips.map((b) => b.textContent)).not.toContain('client-49');
    // An explicit "+N more" overflow hint flags the un-listed lower-volume clients.
    expect(within(container).getByTestId('flow-filter-client-overflow').textContent).toContain('more');
  });

  it('keeps an ACTIVE out-of-top-N client selected + visible (never silently drops the current filter)', () => {
    // `client-49` is the LEAST busy (last in volume order) → outside the top-N — but it is the active
    // filter, so it MUST still render as an active, toggle-off-able chip.
    const { container, onChange } = renderBar({
      clients: many,
      filters: { ...EMPTY_FILTERS, status: null, model: null, upstream: null, client: 'client-49' },
    });
    // The client chip wraps its label in a truncate span, so resolve the enclosing button for aria.
    const chip = within(container).getByText('client-49').closest('button')!;
    expect(chip.getAttribute('aria-pressed')).toBe('true');
    // The total chip count stays bounded even with the active value folded in.
    expect(groupChips(container, 'client').length).toBeLessThanOrEqual(8);
    // Clicking it toggles the client facet OFF.
    fireEvent.click(chip);
    expect(onChange).toHaveBeenCalledWith({ ...EMPTY_FILTERS, status: null, model: null, upstream: null, client: null });
  });

  it('an active client value that IS NOT in the option list at all still shows (cross-link to an aged-out client)', () => {
    const { container } = renderBar({
      clients: ['client-00', 'client-01'],
      filters: { ...EMPTY_FILTERS, status: null, model: null, upstream: null, client: 'ghost-client' },
    });
    expect(within(container).getByText('ghost-client').closest('button')!.getAttribute('aria-pressed')).toBe('true');
  });

  it('renders a very long client_label as a TRUNCATED, bounded chip with the full value in a title (review round 2 MEDIUM)', () => {
    // gap-04 labels (UA / configured-header) can be ~4 KiB — a single long one must NOT render at
    // unbounded width and blow up the bar; it is truncated (bounded `max-w` + ellipsis) with the full
    // value on hover. Active so it also proves the always-folded-in active selection is bounded too.
    const longLabel = `python-httpx/${'x'.repeat(4096)}`;
    const { container } = renderBar({
      clients: [longLabel, 'client-00'],
      filters: { ...EMPTY_FILTERS, status: null, model: null, upstream: null, client: longLabel },
    });
    const chip = within(container).getByText(longLabel).closest('button')!;
    // The label is wrapped in the bounded truncate span (not rendered bare in the button).
    const labelSpan = chip.querySelector('[data-testid="flow-filter-chip-label"]') as HTMLElement;
    expect(labelSpan).toBeTruthy();
    // jsdom returns 0 for layout sizes, so assert the actual width-CONSTRAINING contract (review round 3:
    // `max-w` + `truncate` is a NO-OP on an inline box). The span must establish a block formatting
    // context (`inline-block`/`block`) AND be flex-shrinkable (`min-w-0`) AND cap width AND ellipsis-clip —
    // not merely carry `truncate` (the class that does nothing on an inline span). Real layout (scrollWidth
    // > clientWidth + bounded button width) is asserted in the Playwright e2e at --workers=4.
    expect(labelSpan.className).toMatch(/\b(inline-block|block)\b/);
    expect(labelSpan.className).toContain('min-w-0');
    expect(labelSpan.className).toMatch(/max-w-/);
    expect(labelSpan.className).toContain('truncate');
    // The FULL value is available on hover (the chip's title), and the chip stays active/toggle-off-able.
    expect(chip.getAttribute('title')).toBe(longLabel);
    expect(chip.getAttribute('aria-pressed')).toBe('true');
    // The cap still holds (active + top-N), so a long label can't multiply into an unbounded bar.
    expect(groupChips(container, 'client').length).toBeLessThanOrEqual(8);
  });
});
