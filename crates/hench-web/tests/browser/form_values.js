// Forms whose fields carry a value read from Redis (a lock's holder, a batch member's jid, a
// limit's identity, a retry's key, a process identity) work through a real browser when the
// value holds a lone LF or CR: Chromium rewrites such a value as CRLF when it submits a form,
// so the pages write those values encoded (round 3, web-api-security-5). Each form is used the
// way an operator would (selecting rows, pressing the button, accepting the confirmation), and
// the notice the dashboard redirects to must report the action done; the test then checks
// Redis.
// Usage: node form_values.js <dashboard URL> <JSON {lock, batch}: the lock's digest, the bid>
const { chromium } = require('playwright');
const { check, done } = require('./support');

(async () => {
  const base = process.argv[2];
  const { lock, batch } = JSON.parse(process.argv[3]);
  const browser = await chromium.launch();
  const page = await browser.newPage();
  page.on('dialog', (dialog) => dialog.accept());
  const errors = [];
  page.on('pageerror', (error) => errors.push(String(error)));

  // Presses `button` (a selector) after checking every box matching `boxes`, and returns the
  // notice of the page the form redirects to.
  const act = async (path, button, boxes) => {
    await page.goto(base + path);
    if (boxes) {
      for (const box of await page.$$(boxes)) {
        await box.check();
      }
    }
    await Promise.all([page.waitForNavigation(), page.click(button)]);
    const notice = await page.$('.notice');
    return notice ? (await notice.textContent()).trim() : '(no notice)';
  };

  let notice = await act(`/locks/${lock}`, 'form.entry-actions button[name=release]');
  check(notice.startsWith('Released lock'), `the lock page releases a lock held by "lone\\nlf": ${notice}`);

  notice = await act('/locks', 'button[name=release][data-needs-selection]', 'input[name=key]');
  check(notice === 'Released 1 lock.', `the Locks list releases a lock held by "lone\\rcr": ${notice}`);

  notice = await act('/limits', 'button[name=reset][data-needs-selection]', 'input[name=identity]');
  check(notice === 'Reset 1 limit.', `the Limits list resets a limit keyed "lone\\nlf": ${notice}`);

  notice = await act(`/batches/${batch}`, 'section[aria-labelledby=pending-heading] button[name=skip]');
  check(notice === 'Skipped 1 pending member.', `a batch's page skips the member "member\\rcr": ${notice}`);

  notice = await act('/retries', 'button[name=delete][data-needs-selection]', 'input[name=key]');
  check(notice === 'Deleted 1 job.', `the Retries list deletes the job "retry\\nlf": ${notice}`);

  // The row's button (the page's own Quiet All posts no identity).
  notice = await act('/busy', 'form:has(input[name=identity]) button[name=quiet]');
  check(notice === 'Asked 1 process to quiet down.', `the Busy page quiets the process "odd\\nhost:1:abc": ${notice}`);

  check(errors.length === 0, `no script error: ${errors.join('; ')}`);
  await browser.close();
  done();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
