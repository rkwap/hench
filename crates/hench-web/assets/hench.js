// hench dashboard: behaviour shared by every page. Plain JavaScript, no dependencies, no
// inline handlers (the content security policy allows only scripts served by the dashboard).
//
// * color theme toggle (automatic / light / dark, remembered in localStorage);
// * relative times ("3 minutes ago") kept current, corrected for the server's clock;
// * confirmation of destructive buttons (data-confirm);
// * selection of jobs in lists: select all, shift-click ranges, buttons that need a selection;
// * "Live": list pages refresh themselves every few seconds while switched on.
(function () {
  "use strict";

  var body = document.body;
  var THEME_KEY = "hench-theme";
  var LIVE_KEY = "hench-live";
  var POLL_KEY = "hench-poll-ms";

  function storageGet(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  }

  function storageSet(key, value) {
    try {
      if (value === null) {
        window.localStorage.removeItem(key);
      } else {
        window.localStorage.setItem(key, value);
      }
    } catch (error) {
      // Storage unavailable: the choice lasts for this page only.
    }
  }

  // ------------------------------------------------------------------ theme

  function setupTheme() {
    var button = document.getElementById("theme-toggle");
    if (!button) {
      return;
    }
    var order = ["auto", "light", "dark"];
    function current() {
      var stored = storageGet(THEME_KEY);
      return stored === "light" || stored === "dark" ? stored : "auto";
    }
    function apply(theme) {
      if (theme === "auto") {
        document.documentElement.removeAttribute("data-theme");
      } else {
        document.documentElement.setAttribute("data-theme", theme);
      }
      var label = button.getAttribute("data-label-" + theme) || theme;
      button.setAttribute("title", label);
      button.setAttribute("aria-label", label);
    }
    apply(current());
    button.hidden = false;
    button.addEventListener("click", function () {
      var next = order[(order.indexOf(current()) + 1) % order.length];
      storageSet(THEME_KEY, next === "auto" ? null : next);
      apply(next);
    });
  }

  // ---------------------------------------------------------- relative time

  // The server's clock minus ours, so that "just now" means just now on the server.
  var clockOffset = 0;
  var serverNow = Number(body.getAttribute("data-now-ms"));
  if (isFinite(serverNow) && serverNow > 0) {
    clockOffset = serverNow - Date.now();
  }

  function now() {
    return Date.now() + clockOffset;
  }

  // The same units and thresholds as the server's wording (format.rs).
  var UNITS = [
    [365 * 24 * 3600, "year"],
    [30 * 24 * 3600, "month"],
    [24 * 3600, "day"],
    [3600, "hour"],
    [60, "minute"],
    [1, "second"]
  ];
  var JUST_NOW_SECS = 10;
  var formatter = null;
  if (window.Intl && Intl.RelativeTimeFormat) {
    formatter = new Intl.RelativeTimeFormat("en", { numeric: "always" });
  }

  function relative(timestamp) {
    var seconds = Math.round((now() - timestamp) / 1000);
    var past = seconds >= 0;
    var elapsed = Math.abs(seconds);
    if (elapsed < JUST_NOW_SECS || !formatter) {
      return elapsed < JUST_NOW_SECS ? body.getAttribute("data-just-now") || "" : null;
    }
    for (var i = 0; i < UNITS.length; i++) {
      if (elapsed >= UNITS[i][0]) {
        var count = Math.floor(elapsed / UNITS[i][0]);
        return formatter.format(past ? -count : count, UNITS[i][1]);
      }
    }
    return null;
  }

  function refreshTimes(root) {
    var times = (root || document).querySelectorAll("time[data-relative]");
    for (var i = 0; i < times.length; i++) {
      var timestamp = Date.parse(times[i].getAttribute("datetime"));
      if (isNaN(timestamp)) {
        continue;
      }
      var text = relative(timestamp);
      if (text && times[i].textContent !== text) {
        times[i].textContent = text;
      }
    }
  }

  function tickServerClock() {
    var clock = document.getElementById("server-time");
    if (!clock) {
      return;
    }
    var date = new Date(now());
    var pad = function (n) {
      return (n < 10 ? "0" : "") + n;
    };
    clock.textContent =
      pad(date.getUTCHours()) + ":" + pad(date.getUTCMinutes()) + ":" + pad(date.getUTCSeconds()) + " UTC";
    clock.setAttribute("datetime", date.toISOString().replace(/\.\d+Z$/, "Z"));
  }

  // ---------------------------------------------------------- confirmations

  document.addEventListener("click", function (event) {
    var target = event.target instanceof Element ? event.target.closest("[data-confirm]") : null;
    if (!target) {
      return;
    }
    if (!window.confirm(target.getAttribute("data-confirm"))) {
      event.preventDefault();
      event.stopPropagation();
    }
  });

  // -------------------------------------------------------------- selection

  function setupBulkForms(root) {
    var forms = (root || document).querySelectorAll("[data-bulk-form]");
    for (var i = 0; i < forms.length; i++) {
      setupBulkForm(forms[i]);
    }
  }

  function setupBulkForm(form) {
    var all = form.querySelector("[data-check-all]");
    var items = Array.prototype.slice.call(form.querySelectorAll("[data-check-item]"));
    var buttons = form.querySelectorAll("[data-needs-selection]");
    var last = null;

    function update() {
      var checked = items.filter(function (item) {
        return item.checked;
      }).length;
      for (var b = 0; b < buttons.length; b++) {
        buttons[b].disabled = checked === 0;
      }
      if (all) {
        all.checked = checked > 0 && checked === items.length;
        all.indeterminate = checked > 0 && checked < items.length;
      }
    }

    if (all) {
      all.hidden = false;
      all.addEventListener("change", function () {
        items.forEach(function (item) {
          item.checked = all.checked;
        });
        update();
      });
    }
    items.forEach(function (item, index) {
      item.addEventListener("click", function (event) {
        if (event.shiftKey && last !== null && last !== index) {
          var from = Math.min(last, index);
          var to = Math.max(last, index);
          for (var k = from; k <= to; k++) {
            items[k].checked = item.checked;
          }
        }
        last = index;
        update();
      });
    });
    update();
  }

  // ------------------------------------------------------------------- live

  function pollInterval() {
    var stored = Number(storageGet(POLL_KEY));
    if (isFinite(stored) && stored >= 1000 && stored <= 60000) {
      return stored;
    }
    var fallback = Number(body.getAttribute("data-poll-ms"));
    return isFinite(fallback) && fallback >= 1000 ? fallback : 5000;
  }

  // Refreshing would throw away what the viewer is doing: a selection, an open detail, or
  // the keyboard focus on anything in the content (a field, a link, a button), which the swap
  // would destroy, sending the focus back to the top of the page. (The content itself takes
  // the focus on a click outside its controls: that does not count.)
  function viewerIsBusy(main) {
    if (main.querySelector("[data-check-item]:checked, details[open]")) {
      return true;
    }
    var active = document.activeElement;
    return !!(active && active !== main && main.contains(active));
  }

  function setupLive() {
    var button = document.getElementById("live-toggle");
    if (!button || !body.hasAttribute("data-live-page") || !window.fetch || !window.DOMParser) {
      return;
    }
    var timer = null;
    var running = false;

    function schedule() {
      window.clearTimeout(timer);
      if (running) {
        timer = window.setTimeout(refresh, pollInterval());
      }
    }

    function refresh() {
      var main = document.getElementById("content");
      if (!running || !main || document.hidden || viewerIsBusy(main)) {
        schedule();
        return;
      }
      window
        .fetch(window.location.href, { credentials: "same-origin", headers: { Accept: "text/html" } })
        .then(function (response) {
          return response.ok ? response.text() : null;
        })
        .then(function (html) {
          if (html === null || !running || viewerIsBusy(main)) {
            return;
          }
          var fresh = new DOMParser().parseFromString(html, "text/html");
          var content = fresh.getElementById("content");
          if (!content) {
            return;
          }
          var nodes = Array.prototype.map.call(content.childNodes, function (node) {
            return document.importNode(node, true);
          });
          main.replaceChildren.apply(main, nodes);
          if (fresh.title) {
            document.title = fresh.title;
          }
          var stamp = fresh.body && Number(fresh.body.getAttribute("data-now-ms"));
          if (stamp) {
            clockOffset = stamp - Date.now();
          }
          setupBulkForms(main);
          refreshTimes(main);
        })
        .catch(function () {
          // Network hiccup: try again at the next tick.
        })
        .then(schedule, schedule);
    }

    function set(on) {
      running = on;
      button.setAttribute("aria-pressed", on ? "true" : "false");
      storageSet(LIVE_KEY, on ? "on" : null);
      schedule();
    }

    button.hidden = false;
    button.addEventListener("click", function () {
      set(!running);
    });
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden && running) {
        refresh();
      }
    });
    set(storageGet(LIVE_KEY) === "on");
  }

  function start() {
    setupTheme();
    setupBulkForms(document);
    setupLive();
    refreshTimes(document);
    tickServerClock();
    window.setInterval(function () {
      refreshTimes(document);
    }, 15000);
    window.setInterval(tickServerClock, 1000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }

  // Shared with dashboard.js.
  window.hench = {
    storageGet: storageGet,
    storageSet: storageSet,
    pollKey: POLL_KEY,
    pollInterval: pollInterval,
    setClock: function (serverMillis) {
      if (isFinite(serverMillis) && serverMillis > 0) {
        clockOffset = serverMillis - Date.now();
      }
    }
  };
})();
