// At phone width names stay readable: job classes and queue names are not broken every few
// letters (`overflow-wrap: anywhere` squeezed their columns to a few pixels; web-functional-11).
// Muted labels on the secondary surface reach WCAG AA contrast (4.5:1) in both themes
// (web-functional-15).
// Usage: node layout.js <dashboard URL with retries, a queue and an embedded process>
const { chromium } = require('playwright');
const { check, done, contrast } = require('./support');

// The contrast of each element matching `selector` against the background it is drawn on.
async function contrasts(page, selector) {
  return page.evaluate((selector) => {
    const background = (node) => {
      for (let current = node; current; current = current.parentElement) {
        const color = getComputedStyle(current).backgroundColor;
        if (color && color !== 'transparent' && !/rgba\(.*,\s*0\)$/.test(color)) {
          return color;
        }
      }
      return 'rgb(255, 255, 255)';
    };
    return Array.from(document.querySelectorAll(selector)).map((node) => ({
      text: node.textContent.trim(),
      color: getComputedStyle(node).color,
      background: background(node),
    }));
  }, selector);
}

(async () => {
  const base = process.argv[2];
  const browser = await chromium.launch();
  try {
    const phone = await browser.newPage({ viewport: { width: 390, height: 900 }, colorScheme: 'light' });
    for (const [path, selector] of [['/retries', 'tbody .job-class'], ['/queues', 'tbody .queue-name'], ['/queues/maintenance', 'tbody .job-class']]) {
      await phone.goto(base + path);
      const names = await phone.evaluate((selector) =>
        Array.from(document.querySelectorAll(selector)).map((node) => ({
          text: node.textContent.trim(),
          lines: node.getClientRects().length,
        })), selector);
      check(names.length > 0, `${path} lists names`);
      for (const name of names) {
        check(name.lines === 1, `${path}: “${name.text}” is on one line (${name.lines})`);
      }
      const overflow = await phone.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
      check(overflow <= 0, `${path}: the page itself does not scroll sideways (${overflow} px)`);
    }

    for (const scheme of ['light', 'dark']) {
      const page = await browser.newPage({ colorScheme: scheme });
      for (const [path, selector] of [['/', '.fact dt'], ['/busy', '.fact dt'], ['/busy', '.chip-muted']]) {
        await page.goto(base + path);
        const found = await contrasts(page, selector);
        check(found.length > 0, `${scheme} ${path}: ${selector} found`);
        for (const item of found) {
          const ratio = contrast(item.color, item.background);
          check(ratio >= 4.5, `${scheme} ${path}: “${item.text}” ${item.color} on ${item.background} is ${ratio.toFixed(2)}:1`);
        }
      }
      await page.close();
    }
  } finally {
    await browser.close();
  }
  done();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
