// The dashboard's refresh slider shows the interval the live chart polls at, over the whole
// range the server accepts (web-functional-7), and routine polls are not announced to screen
// readers: only a failing refresh is, once (web-functional-10).
// Usage: node dashboard.js <dashboard URL with a 1 s poll interval> <with 60 s>
const { chromium } = require('playwright');
const { check, done } = require('./support');

(async () => {
  const [fast, slow] = process.argv.slice(2);
  const browser = await chromium.launch();
  try {
    for (const [base, seconds] of [[fast, 1], [slow, 60]]) {
      const page = await browser.newPage();
      await page.goto(base);
      await page.waitForSelector('#interval-control:not([hidden])');
      const slider = () =>
        page.evaluate(() => {
          const input = document.getElementById('poll-interval');
          return {
            min: input.min,
            max: input.max,
            value: input.value,
            label: document.getElementById('poll-interval-value').textContent.trim(),
            polling: window.hench.pollInterval(),
          };
        });
      let state = await slider();
      check(state.min === '1' && state.max === '60', `the slider covers 1-60 s (${state.min}-${state.max})`);
      check(state.value === String(seconds), `the slider shows ${seconds} s (${state.value})`);
      check(state.label === `${seconds} s`, `the label says ${seconds} s (${state.label})`);
      check(state.polling === seconds * 1000, `the chart polls every ${seconds} s (${state.polling} ms)`);
      await page.$eval('#poll-interval', (input) => {
        input.value = '7';
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      });
      state = await slider();
      check(state.label === '7 s' && state.polling === 7000, `moving the slider polls at what it shows (${state.label}, ${state.polling} ms)`);
      await page.close();
    }

    // Polls every second for a few seconds: nothing is announced.
    const page = await browser.newPage();
    await page.goto(fast);
    await page.evaluate(() => {
      window.announced = [];
      for (const region of document.querySelectorAll('[aria-live]:not([aria-live="off"]), [role="status"], [role="alert"], [role="log"]')) {
        new MutationObserver(() => window.announced.push(region.textContent)).observe(region, {
          childList: true,
          characterData: true,
          subtree: true,
        });
      }
    });
    await page.waitForTimeout(3_500);
    let announced = await page.evaluate(() => window.announced);
    check(announced.length === 0, `routine polls are not announced (${JSON.stringify(announced)})`);
    const status = await page.evaluate(() => document.getElementById('live-status').textContent);
    check(/^Updated: \d+ processed, \d+ failed\.$/.test(status), `the status line still has the latest numbers (${status})`);

    // A failing refresh is announced once, however many polls fail.
    await page.route('**/stats', (route) => route.abort());
    await page.waitForTimeout(3_500);
    announced = await page.evaluate(() => window.announced.filter((text) => text.trim() !== ''));
    check(announced.length === 1 && /retrying/.test(announced[0]), `a failure is announced once (${JSON.stringify(announced)})`);
    await page.unroute('**/stats');
    await page.waitForTimeout(2_500);
    announced = await page.evaluate(() => window.announced.filter((text) => text.trim() !== ''));
    check(announced.length === 1, `recovering is quiet (${JSON.stringify(announced)})`);
    await page.close();
  } finally {
    await browser.close();
  }
  done();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
