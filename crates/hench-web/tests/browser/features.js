// The four feature tabs (Periodic, Batches, Limits, Locks) and a job page carrying every
// feature, in the light and the dark theme (docs/design/pro-features.md §6.4 item 5): each page
// loads without a script error or a content-security-policy refusal, marks its tab as the
// current one, is drawn in the theme asked for (by the system setting, or by the theme button),
// keeps its text readable (WCAG AA contrast against the background it is drawn on), and does not
// scroll sideways, at desktop and at phone width.
// Usage: node features.js <dashboard URL> <JSON list of [path, tab] pairs>
const { chromium } = require('playwright');
const { check, done, contrast } = require('./support');

// Text elements of the page's content with their color and the background they are drawn on
// (the first opaque background of their ancestors). Hidden and empty elements are left out.
async function texts(page) {
  return page.evaluate(() => {
    const background = (node) => {
      for (let current = node; current; current = current.parentElement) {
        const color = getComputedStyle(current).backgroundColor;
        if (color && color !== 'transparent' && !/rgba\(.*,\s*0\)$/.test(color)) {
          return color;
        }
      }
      return 'rgb(255, 255, 255)';
    };
    const selector = 'main td, main th, main dt, main dd, main p, main a, main .chip, main .chip-muted, main label, main h1, main h2, main summary, main button';
    return Array.from(document.querySelectorAll(selector))
      .filter((node) => {
        const rect = node.getBoundingClientRect();
        const style = getComputedStyle(node);
        // Elements that draw text of their own (not only through children).
        const own = Array.from(node.childNodes).some((child) => child.nodeType === Node.TEXT_NODE && child.textContent.trim() !== '');
        return own && rect.width > 1 && rect.height > 1 && style.visibility !== 'hidden' && Number(style.opacity) > 0.5;
      })
      .map((node) => {
        const style = getComputedStyle(node);
        const size = parseFloat(style.fontSize);
        const bold = Number(style.fontWeight) >= 700;
        return {
          text: node.textContent.trim().slice(0, 40),
          tag: node.tagName.toLowerCase() + (node.className ? '.' + String(node.className).split(' ').join('.') : ''),
          color: style.color,
          background: background(node),
          // WCAG large text: 24 px, or 18.66 px bold.
          large: size >= 24 || (bold && size >= 18.66),
        };
      });
  });
}

// The relative luminance of the page's background (0 black, 1 white).
async function backgroundLuminance(page) {
  const color = await page.evaluate(() => getComputedStyle(document.body).backgroundColor);
  const [r, g, b] = color.match(/\d+(\.\d+)?/g).slice(0, 3).map(Number).map((value) => {
    const channel = value / 255;
    return channel <= 0.03928 ? channel / 12.92 : Math.pow((channel + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

(async () => {
  const base = process.argv[2];
  const pages = JSON.parse(process.argv[3]);
  const browser = await chromium.launch();
  try {
    for (const scheme of ['light', 'dark']) {
      const page = await browser.newPage({ colorScheme: scheme, viewport: { width: 1280, height: 900 } });
      const errors = [];
      page.on('console', (message) => {
        if (message.type() === 'error') {
          errors.push(message.text());
        }
      });
      page.on('pageerror', (error) => errors.push(String(error)));
      for (const [path, tab] of pages) {
        errors.length = 0;
        const response = await page.goto(base + path);
        await page.waitForLoadState('networkidle');
        check(response.status() === 200, `${scheme} ${path}: 200 (${response.status()})`);
        check(errors.length === 0, `${scheme} ${path}: no script error or refused inline style (${JSON.stringify(errors)})`);
        const current = await page.evaluate(() =>
          Array.from(document.querySelectorAll('.nav [aria-current=page]')).map((node) => node.textContent.trim()));
        check(JSON.stringify(current) === JSON.stringify([tab]), `${scheme} ${path}: the ${tab} tab is the current one (${JSON.stringify(current)})`);
        const luminance = await backgroundLuminance(page);
        check(scheme === 'dark' ? luminance < 0.1 : luminance > 0.8, `${scheme} ${path}: drawn in the ${scheme} theme (background luminance ${luminance.toFixed(2)})`);
        const found = await texts(page);
        check(found.length > 0, `${scheme} ${path}: has text`);
        for (const item of found) {
          const ratio = contrast(item.color, item.background);
          const needed = item.large ? 3 : 4.5;
          if (ratio < needed) {
            check(false, `${scheme} ${path}: ${item.tag} “${item.text}” ${item.color} on ${item.background} is ${ratio.toFixed(2)}:1`);
          }
        }
        const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
        check(overflow <= 0, `${scheme} ${path}: no sideways scroll at 1280 px (${overflow} px)`);
      }
      await page.close();

      // Phone width: the page itself never scrolls sideways (wide tables scroll in their box).
      const phone = await browser.newPage({ colorScheme: scheme, viewport: { width: 390, height: 900 } });
      for (const [path] of pages) {
        await phone.goto(base + path);
        const overflow = await phone.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
        check(overflow <= 0, `${scheme} ${path}: no sideways scroll at 390 px (${overflow} px)`);
      }
      await phone.close();
    }

    // The theme button overrides the system setting on the feature pages too: from a light
    // system, auto -> light -> dark.
    const page = await browser.newPage({ colorScheme: 'light' });
    await page.goto(base + pages[0][0]);
    await page.click('#theme-toggle');
    await page.click('#theme-toggle');
    for (const [path] of pages) {
      await page.goto(base + path);
      const theme = await page.evaluate(() => document.documentElement.getAttribute('data-theme'));
      const luminance = await backgroundLuminance(page);
      check(theme === 'dark' && luminance < 0.1, `${path}: the chosen dark theme applies (${theme}, ${luminance.toFixed(2)})`);
      const found = await texts(page);
      for (const item of found) {
        const ratio = contrast(item.color, item.background);
        if (ratio < (item.large ? 3 : 4.5)) {
          check(false, `chosen dark ${path}: ${item.tag} “${item.text}” ${item.color} on ${item.background} is ${ratio.toFixed(2)}:1`);
        }
      }
    }
    await page.close();
  } finally {
    await browser.close();
  }
  done();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
