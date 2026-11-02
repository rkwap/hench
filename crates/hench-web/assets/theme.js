// Applies the color theme the viewer picked (stored by hench.js) before the first paint,
// so a dark-mode choice never flashes light. "auto" (no stored choice) follows the system.
(function () {
  "use strict";
  try {
    var theme = window.localStorage.getItem("hench-theme");
    if (theme === "light" || theme === "dark") {
      document.documentElement.setAttribute("data-theme", theme);
    }
  } catch (error) {
    // Storage can be unavailable (private browsing, blocked cookies): follow the system.
  }
})();
