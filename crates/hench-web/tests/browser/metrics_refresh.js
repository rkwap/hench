// The metrics page's minute refresh waits while the viewer reads a table view or explores a
// chart with the keyboard (it used to close the table and drop the focus), and the overview's
// table view follows the classes ticked in the table (web-functional-8).
// Usage: node metrics_refresh.js <dashboard URL, with metrics of 8 classes in the last hour>
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
    await page.goto(base + '/metrics');
    check((await page.getAttribute('#metrics-root', 'data-auto-refresh')) === '60', 'the last hour refreshes itself');

    const state = () =>
      page.evaluate(() => {
        const table = document.querySelector('#metrics-root details.table-view table');
        return {
          head: Array.from(table.querySelectorAll('thead th')).slice(1).map((th) => th.textContent.trim()),
          widths: Array.from(table.querySelectorAll('tbody tr')).map((row) => row.cells.length),
          rows: table.querySelectorAll('tbody tr').length,
          legend: Array.from(document.querySelectorAll('#metrics-chart [data-legend] .legend-item')).map((item) => item.textContent.trim()),
        };
      });
    const followsTicks = (current, what) => {
      check(JSON.stringify(current.head) === JSON.stringify(current.legend), `${what}: the table view shows the charted classes (${current.head} / ${current.legend})`);
      check(current.rows > 0 && current.widths.every((width) => width === current.head.length + 1), `${what}: every row has a value per class`);
    };
    let current = await state();
    check(current.legend.length === 5, `five classes are charted at first (${current.legend})`);
    followsTicks(current, 'at first');
    const boxes = await page.$$('[data-series-toggle]');
    const unticked = await boxes[0].getAttribute('aria-label');
    await boxes[0].uncheck();
    await boxes[6].check();
    current = await state();
    check(current.legend.length === 5 && !current.legend.some((name) => unticked.includes(`“${name}”`)), `the ticks changed the chart (${current.legend})`);
    followsTicks(current, 'after ticking');
    const ticked = current.legend;

    // Reading a table view and exploring the chart with the keyboard: the refresh waits.
    await page.click('details.table-view > summary');
    await page.focus('#metrics-chart svg');
    await page.keyboard.press('ArrowLeft');
    const before = fetches;
    await page.clock.runFor(61_000);
    await page.waitForTimeout(600);
    const reading = await page.evaluate(() => ({
      open: document.querySelector('details.table-view').open,
      active: document.activeElement.tagName.toLowerCase(),
      tooltip: !!document.querySelector('#metrics-chart .chart-tooltip:not([hidden])'),
    }));
    check(fetches === before, `no refresh while the viewer reads (${fetches - before} fetches)`);
    check(reading.open, 'the table view stays open');
    check(reading.active === 'svg', `the chart keeps the focus (${reading.active})`);
    check(reading.tooltip, 'the tooltip stays');

    // Done reading: the next minute refreshes, keeping the ticked classes and their table.
    await page.click('details.table-view > summary');
    await page.evaluate(() => {
      document.activeElement.blur();
      document.querySelector('#metrics-root .panel').dataset.mark = 'old';
    });
    await page.clock.runFor(61_000);
    const swapped = await eventually(page, () => document.querySelector('#metrics-root .panel').dataset.mark !== 'old');
    check(fetches > before, 'the page refreshes once the viewer is done');
    check(swapped, 'the content was swapped');
    current = await state();
    check(JSON.stringify(current.legend) === JSON.stringify(ticked), `the refresh keeps the ticked classes (${current.legend})`);
    followsTicks(current, 'after the refresh');
  } finally {
    await browser.close();
  }
  done();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
