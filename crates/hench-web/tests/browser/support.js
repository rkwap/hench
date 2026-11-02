// Shared by the browser scripts: assertions that report every failure, then exit non-zero.
const failures = [];

function check(condition, message) {
  if (condition) {
    console.log('ok   ' + message);
  } else {
    console.log('FAIL ' + message);
    failures.push(message);
  }
}

function done() {
  if (failures.length > 0) {
    console.log(`${failures.length} check(s) failed`);
    process.exit(1);
  }
}

// WCAG contrast ratio of two `rgb(r, g, b)` colors.
function contrast(foreground, background) {
  const luminance = (color) => {
    const [r, g, b] = color.match(/\d+(\.\d+)?/g).slice(0, 3).map(Number).map((value) => {
      const channel = value / 255;
      return channel <= 0.03928 ? channel / 12.92 : Math.pow((channel + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const [light, dark] = [luminance(foreground), luminance(background)].sort((a, b) => b - a);
  return (light + 0.05) / (dark + 0.05);
}

// Whether `condition` (a function run in the page) holds within `timeout` ms, polled from Node
// (the page's own timers may be faked with `page.clock`).
async function eventually(page, condition, timeout = 5_000) {
  const deadline = Date.now() + timeout;
  for (;;) {
    if (await page.evaluate(condition)) {
      return true;
    }
    if (Date.now() > deadline) {
      return false;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

module.exports = { check, done, contrast, eventually };
