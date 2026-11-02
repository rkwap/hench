// "Live" list refresh must not take the keyboard focus away (web-functional-9): while focus
// is on anything inside the page's content (a link, a button, a field), the refresh waits;
// once focus is elsewhere, it swaps the content again.
// Usage: node live_focus.js <dashboard URL with a 2 s poll interval>
const { chromium } = require('playwright');
const { check, done, eventually } = require('./support');

(async () => {
  const base = process.argv[2];
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.clock.install();
    let fetches = 0;
    page.on('request', (request) => {
      if (request.resourceType() === 'fetch') fetches += 1;
    });
    await page.goto(base + '/retries');
    await page.click('#live-toggle');
    check(await page.getAttribute('#live-toggle', 'aria-pressed') === 'true', 'Live is on');

    const mark = () => page.evaluate(() => { document.querySelector('#content .totals').dataset.mark = 'old'; });
    const swapped = () => page.evaluate(() => document.querySelector('#content .totals').dataset.mark !== 'old');
    const settle = async () => {
      await page.clock.runFor(2_100);
      await page.waitForTimeout(400);
    };

    for (const selector of ['#content .totals a', '#content a[href*="/retries/"]', '#content button[formaction$="/all/delete"]', '#filter-input']) {
      await mark();
      await page.focus(selector);
      const before = fetches;
      await settle();
      await settle();
      const state = await page.evaluate((selector) => ({
        focused: document.activeElement === document.querySelector(selector),
        active: document.activeElement.tagName,
      }), selector);
      check(state.focused, `focus stays on ${selector} (now on ${state.active})`);
      check(!(await swapped()), `the content is not swapped while ${selector} has the focus`);
      check(fetches === before, `no refresh while ${selector} has the focus (${fetches - before} fetches)`);
    }

    // Focus outside the content (the Live switch): the refresh goes on.
    await mark();
    await page.focus('#live-toggle');
    await page.clock.runFor(2_100);
    const refreshed = await eventually(page, () => document.querySelector('#content .totals').dataset.mark !== 'old');
    check(refreshed, 'the content is refreshed once the focus left it');
    check(await page.evaluate(() => document.activeElement.id) === 'live-toggle', 'focus stays on the Live switch');
  } finally {
    await browser.close();
  }
  done();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
