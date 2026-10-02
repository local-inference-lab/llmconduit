// Ad-hoc screenshotter for the running Argus dev server (mock mode).
//   npm run shot               # all four views -> /tmp/argus-<view>.png
//   npm run shot -- topology   # just one (or several) views
//   ARGUS_URL=... npm run shot # point at a different base URL
// Deterministic clock + seeded RNG (matches the e2e baselines) so shots are repeatable.
import { chromium } from '@playwright/test';

const TABS = { flows: 'Flows', topology: 'Topology', sankey: 'Sankey', theater: 'Theater', overview: 'Overview', chat: 'Chat', providers: 'Providers', access: 'Access', 'access-policies': 'Access', 'access-limits': 'Access' };
const FIXED_NOW = Date.UTC(2026, 5, 21, 14, 20, 0);
const BASE = process.env.ARGUS_URL || 'http://localhost:5273/dashboard/?mock=1';

const requested = process.argv.slice(2);
const targets = requested.length ? requested : Object.keys(TABS);

function seedAndFreeze(fixedNow) {
  let s = 0x02f6e2b1 >>> 0;
  Math.random = () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const RealDate = Date;
  class FrozenDate extends RealDate {
    constructor(...args) {
      super(...(args.length ? args : [fixedNow]));
    }
    static now() {
      return fixedNow;
    }
  }
  globalThis.Date = FrozenDate;
}

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
const errors = [];
page.on('console', (m) => {
  if (m.type() === 'error') errors.push(m.text());
});
page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));

await page.addInitScript(seedAndFreeze, FIXED_NOW);
await page.goto(BASE, { waitUntil: 'networkidle' });
await page.locator('input').first().fill('dev-token');
await page.getByRole('button', { name: /sign in/i }).click();
await page.getByRole('button', { name: 'Flows', exact: true }).waitFor();

for (const name of targets) {
  const tab = TABS[name];
  if (!tab) {
    console.log('skip unknown view:', name);
    continue;
  }
  await page.getByRole('navigation').getByRole('button', { name: tab, exact: true }).click();
  if (name === 'access-policies') {
    await page.getByRole('button', { name: /close create access/i }).click();
    await page.getByRole('tab', { name: 'Model policies', exact: true }).click();
    await page.getByText('Providers', { exact: true }).click();
    await page.getByText('Models', { exact: true }).click();
    await page.getByTestId('access-view').evaluate((element) => { element.scrollTop = 0; });
  }
  if (name === 'access-limits') {
    await page.getByRole('button', { name: /close create access/i }).click();
    await page.getByRole('tab', { name: 'Model policies', exact: true }).click();
    await page.getByLabel('Max concurrent sessions').scrollIntoViewIfNeeded();
  }
  await page.waitForTimeout(1000);
  const out = `/tmp/argus-${name}.png`;
  await page.screenshot({ path: out });
  console.log('SHOT', name, '->', out);
}

await browser.close();
console.log('CONSOLE_ERRORS', errors.length);
errors.forEach((e) => console.log('  -', e));
if (errors.length) process.exit(1);
