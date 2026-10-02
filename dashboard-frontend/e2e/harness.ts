import { test as base, expect, type Page } from '@playwright/test';

/**
 * Fixed wall-clock. The mock stamps flow timestamps + time-window math off `Date.now()`,
 * so freezing it makes those render byte-identically run-to-run (stable pixel baselines).
 */
export const FIXED_NOW = Date.UTC(2026, 5, 21, 14, 20, 0); // 2026-06-21T14:20:00Z

export type ViewName =
  | 'flows'
  | 'sessions'
  | 'topology'
  | 'sankey'
  | 'theater'
  | 'overview'
  | 'chat'
  | 'providers'
  | 'access';

/** Each view: the nav-tab label to click + a route-specific "ready" marker (text/regex). */
export const VIEWS: { name: ViewName; tab: string; ready: string | RegExp }[] = [
  { name: 'flows', tab: 'Flows', ready: '/v1/responses' },
  { name: 'topology', tab: 'Topology', ready: /click a node to filter flows/i },
  { name: 'sankey', tab: 'Sankey', ready: /Token Sankey/i },
  { name: 'theater', tab: 'Theater', ready: /No active streams/i },
  // Gap 16 — the control-room overview (the 5th route). Its masthead text is the ready marker.
  { name: 'overview', tab: 'Overview', ready: /control room/i },
  // The live active-sessions board (the dashboard's primary view). Masthead subtitle is the ready marker.
  { name: 'sessions', tab: 'Sessions', ready: /active in the last 15 minutes/i },
  { name: 'chat', tab: 'Chat', ready: /^chat$/i },
  { name: 'providers', tab: 'Providers', ready: /provider inventory/i },
  { name: 'access', tab: 'Access', ready: /Access control/i },
];

/**
 * Determinism shim, injected before any app code runs:
 *  - seed Math.random (mulberry32) so d3-force's jiggle lays nodes out identically;
 *  - freeze Date / Date.now so mock-stamped timestamps + the LIVE window are fixed.
 * performance.now() + timers are left alone, so d3-force / uPlot still animate to a
 * settled state — we just remove the two non-deterministic inputs (RNG + wall-clock).
 */
export async function installDeterminism(page: Page): Promise<void> {
  await page.addInitScript((fixedNow) => {
    let s = 0x02f6e2b1 >>> 0;
    Math.random = () => {
      s = (s + 0x6d2b79f5) >>> 0;
      let t = Math.imul(s ^ (s >>> 15), 1 | s);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    const RealDate = Date;
    class FrozenDate extends RealDate {
      constructor(...args: ConstructorParameters<typeof Date>) {
        super(...(args.length ? args : [fixedNow]));
      }
      static now() {
        return fixedNow;
      }
    }
    // @ts-ignore - replace the global clock for the page
    globalThis.Date = FrozenDate;
  }, FIXED_NOW);
}

/** Log into the mock dashboard. The mock accepts any token (`/dashboard/login` -> `{ ok: true }`). */
export async function login(page: Page): Promise<void> {
  await installDeterminism(page);
  await page.goto('/dashboard/?mock=1', { waitUntil: 'networkidle' });
  await page.locator('input').first().fill('dev-token');
  await page.getByRole('button', { name: /sign in/i }).click();
  // Auth flips -> the nav tabs render.
  await expect(page.getByRole('button', { name: 'Flows', exact: true })).toBeVisible();
}

/** Click a nav tab and wait for that view's route-specific ready marker. */
export async function openView(page: Page, view: { tab: string; ready: string | RegExp }): Promise<void> {
  await page.getByRole('navigation').getByRole('button', { name: view.tab, exact: true }).click();
  await expect(page.getByText(view.ready).first()).toBeVisible();
  await page.waitForLoadState('networkidle');
  // Self-hosted webfonts must paint before the pixel baseline, else metrics differ run-to-run.
  await page.evaluate(() => document.fonts.ready.then(() => undefined));
}

/**
 * `test` with an auto console-error gate. Any `console.error` / uncaught `pageerror`
 * is collected; assert `consoleErrors` is empty in the test. Fully deterministic
 * (0 errors is 0 errors) and independent of layout/pixel jitter.
 */
export const test = base.extend<{ consoleErrors: string[] }>({
  consoleErrors: async ({ page }, use) => {
    const errors: string[] = [];
    page.on('console', (m) => {
      if (m.type() === 'error') errors.push(m.text());
    });
    page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
    await use(errors);
  },
});

export { expect };
