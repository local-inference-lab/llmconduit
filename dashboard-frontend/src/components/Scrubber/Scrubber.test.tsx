import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, cleanup, fireEvent, waitFor } from '@testing-library/react';
import { Scrubber } from './Scrubber';
import { dashboardStore, type LiveBaseline } from '../../store/dashboardStore';
import { getConnection } from '../../api/connection';
import type { MetricsResponse, MetricWindow } from '../../api/types';
import { renderWithQuery, resetWorld } from '../testHarness';

function win(over: Partial<MetricWindow> = {}): MetricWindow {
  const samples = over.samples ?? 252;
  return {
    reqs_per_sec: 4.2, active_streams: 3, error_pct: 1.1,
    p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142,
    prefill_tokens_per_sec: 10_000, decode_tokens_per_sec: 78.34, cost_per_min: 0.21,
    samples, usage_samples: samples, prefill_samples: samples, decode_samples: samples,
    priced_samples: samples, cost_confidence: 'estimated',
    ...over,
  };
}
function metrics(seq: number, reqs: number): MetricsResponse {
  return {
    metrics_seq: seq, reqs_per_sec: reqs, active_streams: 3, error_pct: 1.1,
    p50: 180, p95: 920, p99: 1840, tokens_per_sec: 142,
    prefill_tokens_per_sec: 10_000, decode_tokens_per_sec: 78.34, cost_per_min: 0.21,
    samples: 252, usage_samples: 252, prefill_samples: 252, decode_samples: 252,
    priced_samples: 252, cost_confidence: 'estimated',
    windows: { m1: win({ reqs_per_sec: reqs }), m5: win(), h1: win() },
  };
}

/** Push N reqs/s samples into the store so the hill has a span (each a fresh seq). */
function seedHill(samples: number[]): void {
  act(() => {
    samples.forEach((r, i) => dashboardStore.getState().setMetrics(metrics(i + 1, r)));
  });
}

/** Give the track a real width so fraction math works (jsdom reports 0). */
function stubTrackWidth(width = 200): () => void {
  const original = HTMLElement.prototype.getBoundingClientRect;
  HTMLElement.prototype.getBoundingClientRect = function () {
    return { x: 0, y: 0, top: 0, left: 0, right: width, bottom: 40, width, height: 40, toJSON: () => ({}) } as DOMRect;
  };
  return () => { HTMLElement.prototype.getBoundingClientRect = original; };
}

/**
 * jsdom has NO `PointerEvent` constructor, so testing-library's `fireEvent.pointerX` falls back to
 * a base Event that DROPS `clientX`/`pointerId`. Polyfill it as a MouseEvent carrying those fields
 * (representative of a real browser) so the drag/hover coordinate math runs.
 */
function installPointerEvent(): () => void {
  const had = 'PointerEvent' in window;
  const prev = (window as { PointerEvent?: unknown }).PointerEvent;
  class PointerEventPolyfill extends window.MouseEvent {
    pointerId: number;
    constructor(type: string, init: PointerEventInit = {}) {
      super(type, init as MouseEventInit);
      this.pointerId = init.pointerId ?? 1;
    }
  }
  (window as { PointerEvent?: unknown }).PointerEvent = PointerEventPolyfill as unknown as typeof PointerEvent;
  (globalThis as { PointerEvent?: unknown }).PointerEvent = PointerEventPolyfill as unknown as typeof PointerEvent;
  return () => {
    if (had) (window as { PointerEvent?: unknown }).PointerEvent = prev;
    else delete (window as { PointerEvent?: unknown }).PointerEvent;
    (globalThis as { PointerEvent?: unknown }).PointerEvent = (window as { PointerEvent?: unknown }).PointerEvent;
  };
}

let restoreRect: (() => void) | null = null;
let restorePE: (() => void) | null = null;

beforeEach(() => {
  resetWorld({ mock: true });
  restoreRect = stubTrackWidth();
  restorePE = installPointerEvent();
});
afterEach(() => {
  cleanup();
  restoreRect?.();
  restoreRect = null;
  restorePE?.();
  restorePE = null;
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('Scrubber — hill + hover', () => {
  it('renders the reqs/s hill path from history', () => {
    const { socket } = getConnection();
    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    seedHill([1, 4, 2, 6, 3]);
    const hill = getByTestId('scrubber-hill').querySelector('path')!;
    // A non-trivial area path (multiple line segments + close) is drawn.
    expect(hill.getAttribute('d')).toBeTruthy();
    expect(hill.getAttribute('d')!.split('L').length).toBeGreaterThan(3);
  });

  it('hover shows a tooltip with the time + reqs/s at the point', () => {
    const { socket } = getConnection();
    const { getByTestId, queryByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    seedHill([1, 4, 2, 6, 3]);
    expect(queryByTestId('scrubber-tooltip')).toBeNull();
    fireEvent.pointerMove(getByTestId('scrubber-track'), { clientX: 100 });
    const tip = getByTestId('scrubber-tooltip');
    expect(tip.textContent).toMatch(/req\/s/);
    expect(tip.textContent).toMatch(/\d{2}:\d{2}:\d{2}/); // HH:MM:SS clock
  });

  it('renders the tooltip OUTSIDE any overflow-hidden ancestor so it is not clipped (finding 4)', () => {
    const { socket } = getConnection();
    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    seedHill([1, 4, 2, 6, 3]);
    fireEvent.pointerMove(getByTestId('scrubber-track'), { clientX: 100 });
    const tip = getByTestId('scrubber-tooltip');
    const scrubberRoot = getByTestId('scrubber');
    // The tooltip sits ABOVE the track (`-top-9`); jsdom can't compute real clipping, so assert the
    // STRUCTURE: no ancestor between the tooltip and the scrubber root carries `overflow-hidden`
    // (which clipped it in the real UI). The hill's clip layer must NOT be an ancestor of the tip.
    let el: HTMLElement | null = tip.parentElement;
    while (el && el !== scrubberRoot) {
      expect(el.className).not.toContain('overflow-hidden');
      el = el.parentElement;
    }
    // And the clipped hill layer really does exist (the clip is scoped to it, not the track).
    const hillClip = getByTestId('scrubber-hill').parentElement!;
    expect(hillClip.className).toContain('overflow-hidden');
    expect(hillClip.contains(tip)).toBe(false);
  });
});

describe('Scrubber — seek + LIVE', () => {
  it('clicking/dragging pauses live WS, fetches /snapshot?at=, and atomically installs the frozen cut', async () => {
    const { socket, client } = getConnection();
    const seekSpy = vi.spyOn(socket, 'seek');
    const snapSpy = vi.spyOn(client, 'snapshot');
    const cutSpy = vi.spyOn(dashboardStore.getState(), 'applySeekCut');

    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    seedHill([1, 4, 2, 6, 3]);

    // Click partway along the track → pause live WS (shadow-buffer) WITHOUT yet exposing 'seeking'.
    act(() => {
      fireEvent.pointerDown(getByTestId('scrubber-track'), { clientX: 80, pointerId: 1 });
    });
    // Live WS applying is paused immediately (shadow-buffer)…
    expect(seekSpy).toHaveBeenCalled();
    expect(socket.isPaused()).toBe(true);
    // …but the store is NOT yet 'seeking' — the rows/cursors are still LIVE until the cut lands
    // (finding 1: never `seeking` with unfrozen data). The fetch is still in flight here.
    expect(dashboardStore.getState().connection).not.toBe('seeking');

    // The snapshot fetch fires (after the rAF frame) and the frozen cut is installed ATOMICALLY.
    await waitFor(() => expect(snapSpy).toHaveBeenCalled());
    await waitFor(() => expect(cutSpy).toHaveBeenCalled());
    // Only NOW does the store read as seeking, with the frozen instant AND the cut's monitor seq.
    await waitFor(() => {
      const st = dashboardStore.getState();
      expect(st.connection).toBe('seeking');
      expect(st.seekAtMs).not.toBeNull();
      // `seekMonitorSeq` is the SNAPSHOT's monitor_seq cut (mock = 5), not a live cursor.
      expect(st.seekMonitorSeq).toBe(st.cursors.monitor_seq);
    });
  });

  it('rapid drag moves coalesce to a bounded number of fetches (no request storm)', async () => {
    vi.useFakeTimers();
    const { socket, client } = getConnection();
    const snapSpy = vi.spyOn(client, 'snapshot').mockResolvedValue({
      cursors: { flow_seq: 0, metrics_seq: 0, topology_seq: 0, monitor_seq: 0 }, at_ms: Date.now(), summaries: [], metrics: null, topology: null,
    });
    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    act(() => {
      [1, 4, 2, 6, 3, 5, 7, 2, 8].forEach((r, i) => dashboardStore.getState().setMetrics(metrics(i + 1, r)));
    });

    const track = getByTestId('scrubber-track');
    act(() => {
      fireEvent.pointerDown(track, { clientX: 10, pointerId: 1 });
      // 40 rapid moves WITHIN the same animation frame.
      for (let i = 0; i < 40; i++) fireEvent.pointerMove(track, { clientX: 10 + i * 4, pointerId: 1 });
    });
    // Flush the single coalesced rAF frame (jsdom shims rAF on a timer).
    await act(async () => {
      vi.advanceTimersByTime(32);
    });
    // 40 moves + the down → ONE coalesced fetch this frame (NOT ~41).
    expect(snapSpy.mock.calls.length).toBeLessThanOrEqual(1);
  });

  it('a seek does NOT add a point to the live hill, and live resume continues from live ticks (D11 R5)', () => {
    const { socket } = getConnection();
    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    seedHill([1, 4, 2, 6, 3]); // 5 live ticks (seq 1..5)
    const hill = () => getByTestId('scrubber-hill').querySelector('path')!.getAttribute('d')!;
    const beforeSeek = hill();

    // SEEK: capture baseline, then atomically install the FROZEN historical cut + connection=seeking.
    // The frozen metrics (a distinct seq + reqs/s) must NOT fold into the live hill ring.
    let baseline!: LiveBaseline;
    act(() => {
      baseline = dashboardStore.getState().captureLiveBaseline();
      dashboardStore.getState().applySeekCut({
        rows: [],
        cursors: { flow_seq: 0, metrics_seq: 99, topology_seq: 0, monitor_seq: 7 },
        atMs: Date.now(),
        monitorSeq: 7,
        metrics: metrics(99, 50), // frozen reqs/s = 50 (would spike the hill if folded)
        topology: null,
      });
    });
    expect(dashboardStore.getState().connection).toBe('seeking');
    // The hill path is UNCHANGED — the frozen seek cut added no point to the live ring.
    expect(hill()).toBe(beforeSeek);

    // RESUME: restore the live baseline (seq 5) atomically with connection=live → deduped, no point.
    act(() => dashboardStore.getState().restoreLiveBaseline(baseline));
    expect(dashboardStore.getState().connection).toBe('live');
    expect(hill()).toBe(beforeSeek);

    // A NEW live tick (seq 6) continues the hill cleanly — now the path grows.
    act(() => dashboardStore.getState().setMetrics(metrics(6, 9)));
    expect(hill()).not.toBe(beforeSeek);
  });

  it('the LIVE toggle resumes the socket (replays buffered frames)', () => {
    const { socket } = getConnection();
    const liveSpy = vi.spyOn(socket, 'live');
    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    seedHill([1, 4, 2]);

    // Enter seek first so the LIVE toggle is shown.
    act(() => {
      socket.seek();
      dashboardStore.getState().enterSeek(Date.now());
    });
    fireEvent.click(getByTestId('live-toggle'));
    expect(liveSpy).toHaveBeenCalled();
  });
});

describe('Scrubber — prefers-reduced-motion', () => {
  it('does not pulse the live indicator when reduced motion is set', () => {
    vi.stubGlobal('matchMedia', (q: string) => ({
      matches: q.includes('prefers-reduced-motion'),
      media: q, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {}, onchange: null, dispatchEvent: () => false,
    }));
    const { socket } = getConnection();
    const { getByTestId } = renderWithQuery(<Scrubber socket={socket} />);
    // The live indicator's dot must NOT carry the pulse animation class under reduced motion.
    const dot = getByTestId('live-indicator').querySelector('span[aria-hidden]')!;
    expect(dot.className).not.toContain('animate-pulse');
    vi.unstubAllGlobals();
  });
});
