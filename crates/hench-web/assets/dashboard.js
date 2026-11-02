// hench dashboard: the live chart (jobs processed and failed between two refreshes of
// /stats) and the history chart (per day), drawn as SVG by hand. Both have a crosshair
// tooltip (pointer and keyboard); the history also has a table view in the page.
(function () {
  "use strict";

  var SVG_NS = "http://www.w3.org/2000/svg";
  var LIVE_POINTS = 60;
  var MARGIN = { top: 10, right: 14, bottom: 26, left: 48 };
  var SERIES = ["processed", "failed"];
  var shared = window.hench || {
    storageGet: function () {
      return null;
    },
    storageSet: function () {},
    pollKey: "hench-poll-ms",
    pollInterval: function () {
      return 5000;
    },
    setClock: function () {}
  };

  function svg(name, attributes, parent) {
    var node = document.createElementNS(SVG_NS, name);
    Object.keys(attributes).forEach(function (key) {
      node.setAttribute(key, String(attributes[key]));
    });
    if (parent) {
      parent.appendChild(node);
    }
    return node;
  }

  function full(n) {
    return Number(n).toLocaleString("en-US");
  }

  function compact(n) {
    if (n >= 1e9) {
      return trim(n / 1e9) + "B";
    }
    if (n >= 1e6) {
      return trim(n / 1e6) + "M";
    }
    if (n >= 1e3) {
      return trim(n / 1e3) + "K";
    }
    return String(n);
  }

  function trim(value) {
    return value.toFixed(1).replace(/\.0$/, "");
  }

  // A clean axis: about four steps of 1, 2, 2.5 or 5 times a power of ten (whole jobs).
  function axis(max) {
    if (!(max > 0)) {
      return { max: 4, step: 1 };
    }
    var raw = max / 4;
    var power = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10));
    var fraction = raw / power;
    var nice = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 2.5 ? 2.5 : fraction <= 5 ? 5 : 10;
    var step = Math.max(1, nice * power);
    return { max: Math.ceil(max / step) * step, step: step };
  }

  // ------------------------------------------------------------- the chart

  function LineChart(figure) {
    this.figure = figure;
    this.plot = figure.querySelector("[data-chart-plot]");
    this.note = figure.querySelector("[data-chart-note]");
    this.names = {
      processed: figure.getAttribute("data-label-processed") || "processed",
      failed: figure.getAttribute("data-label-failed") || "failed"
    };
    this.labels = [];
    this.axisLabels = [];
    this.values = { processed: [], failed: [] };
    this.index = null;
    this.tooltip = document.createElement("div");
    this.tooltip.className = "chart-tooltip";
    this.tooltip.hidden = true;
    this.plot.appendChild(this.tooltip);
  }

  LineChart.prototype.setData = function (labels, axisLabels, values) {
    this.labels = labels;
    this.axisLabels = axisLabels;
    this.values = values;
    if (this.index !== null && this.index >= labels.length) {
      this.index = labels.length - 1;
    }
    this.render();
  };

  LineChart.prototype.x = function (index) {
    var count = this.labels.length;
    var width = this.width - MARGIN.left - MARGIN.right;
    if (count <= 1) {
      return MARGIN.left + width / 2;
    }
    return MARGIN.left + (width * index) / (count - 1);
  };

  LineChart.prototype.y = function (value) {
    var height = this.height - MARGIN.top - MARGIN.bottom;
    return MARGIN.top + height - (height * value) / this.scale.max;
  };

  LineChart.prototype.render = function () {
    var self = this;
    if (this.svg) {
      this.svg.remove();
    }
    this.width = Math.max(this.plot.clientWidth, 200);
    this.height = Math.max(this.plot.clientHeight, 120);
    var count = this.labels.length;
    var max = 0;
    SERIES.forEach(function (key) {
      self.values[key].forEach(function (value) {
        max = Math.max(max, value);
      });
    });
    this.scale = axis(max);
    var useCompact = this.scale.max >= 10000;

    var root = svg("svg", {
      viewBox: "0 0 " + this.width + " " + this.height,
      role: "img",
      "aria-label": this.summary(),
      tabindex: count ? 0 : -1,
      focusable: count ? "true" : "false"
    });
    this.svg = root;

    // Gridlines and value ticks.
    for (var tick = 0; tick <= this.scale.max + 1e-9; tick += this.scale.step) {
      var y = Math.round(this.y(tick)) + 0.5;
      svg(
        "line",
        { class: tick === 0 ? "baseline" : "gridline", x1: MARGIN.left, x2: this.width - MARGIN.right, y1: y, y2: y },
        root
      );
      var label = svg("text", { class: "tick", x: MARGIN.left - 8, y: y + 4, "text-anchor": "end" }, root);
      label.textContent = useCompact ? compact(tick) : full(tick);
    }

    // Category labels along the bottom: about six, never overlapping.
    if (count > 0) {
      var room = Math.max(1, Math.floor((this.width - MARGIN.left - MARGIN.right) / 72));
      var every = Math.max(1, Math.ceil(count / Math.min(room, 6)));
      for (var i = count - 1; i >= 0; i -= every) {
        var text = svg(
          "text",
          { class: "tick", x: this.x(i), y: this.height - 8, "text-anchor": i === count - 1 ? "end" : "middle" },
          root
        );
        text.textContent = this.axisLabels[i];
      }
    }

    // The lines, with a marker at the latest point.
    SERIES.forEach(function (key) {
      var values = self.values[key];
      if (!values.length) {
        return;
      }
      var path = values
        .map(function (value, index) {
          return (index ? "L" : "M") + self.x(index).toFixed(1) + " " + self.y(value).toFixed(1);
        })
        .join(" ");
      svg("path", { class: "line series-" + key, d: path }, root);
      var last = values.length - 1;
      svg("circle", { class: "marker series-" + key, cx: self.x(last), cy: self.y(values[last]), r: 4 }, root);
    });

    // Crosshair, hover markers and the pointer target.
    this.crosshair = svg("line", { class: "crosshair", y1: MARGIN.top, y2: this.height - MARGIN.bottom, visibility: "hidden" }, root);
    this.hoverMarkers = SERIES.map(function (key) {
      return svg("circle", { class: "marker series-" + key, r: 5, visibility: "hidden" }, root);
    });
    var hit = svg(
      "rect",
      {
        class: "hit-area",
        x: MARGIN.left - 12,
        y: 0,
        width: this.width - MARGIN.left - MARGIN.right + 24,
        height: this.height
      },
      root
    );
    hit.addEventListener("pointermove", function (event) {
      var box = root.getBoundingClientRect();
      var scaleX = self.width / box.width;
      self.show(self.nearest((event.clientX - box.left) * scaleX));
    });
    hit.addEventListener("pointerleave", function () {
      self.show(null);
    });
    root.addEventListener("focus", function () {
      self.show(self.index === null ? count - 1 : self.index);
    });
    root.addEventListener("blur", function () {
      self.show(null);
    });
    root.addEventListener("keydown", function (event) {
      if (!count) {
        return;
      }
      var index = self.index === null ? count - 1 : self.index;
      if (event.key === "ArrowLeft") {
        index = Math.max(0, index - 1);
      } else if (event.key === "ArrowRight") {
        index = Math.min(count - 1, index + 1);
      } else if (event.key === "Home") {
        index = 0;
      } else if (event.key === "End") {
        index = count - 1;
      } else {
        return;
      }
      event.preventDefault();
      self.show(index);
    });

    this.plot.insertBefore(root, this.tooltip);
    if (this.note) {
      this.note.hidden = count > 0;
    }
    if (this.index !== null) {
      this.show(this.index);
    }
  };

  LineChart.prototype.nearest = function (x) {
    var count = this.labels.length;
    if (!count) {
      return null;
    }
    if (count === 1) {
      return 0;
    }
    var width = this.width - MARGIN.left - MARGIN.right;
    var index = Math.round(((x - MARGIN.left) / width) * (count - 1));
    return Math.min(count - 1, Math.max(0, index));
  };

  LineChart.prototype.summary = function () {
    var count = this.labels.length;
    if (!count) {
      return this.figure.querySelector(".chart-title").textContent;
    }
    var last = count - 1;
    return (
      this.figure.querySelector(".chart-title").textContent +
      ": " +
      this.labels[last] +
      ", " +
      full(this.values.processed[last] || 0) +
      " " +
      this.names.processed +
      ", " +
      full(this.values.failed[last] || 0) +
      " " +
      this.names.failed
    );
  };

  LineChart.prototype.show = function (index) {
    var self = this;
    this.index = index;
    if (index === null || !this.labels.length) {
      this.crosshair.setAttribute("visibility", "hidden");
      this.hoverMarkers.forEach(function (marker) {
        marker.setAttribute("visibility", "hidden");
      });
      this.tooltip.hidden = true;
      return;
    }
    var x = this.x(index);
    this.crosshair.setAttribute("x1", x);
    this.crosshair.setAttribute("x2", x);
    this.crosshair.setAttribute("visibility", "visible");
    SERIES.forEach(function (key, position) {
      var marker = self.hoverMarkers[position];
      marker.setAttribute("cx", x);
      marker.setAttribute("cy", self.y(self.values[key][index] || 0));
      marker.setAttribute("visibility", "visible");
    });

    this.tooltip.replaceChildren();
    var title = document.createElement("div");
    title.className = "chart-tooltip-title";
    title.textContent = this.labels[index];
    this.tooltip.appendChild(title);
    SERIES.forEach(function (key) {
      var row = document.createElement("div");
      row.className = "chart-tooltip-row";
      var swatch = document.createElement("span");
      swatch.className = "legend-key series-" + key;
      var value = document.createElement("strong");
      value.textContent = full(self.values[key][index] || 0);
      var name = document.createElement("span");
      name.textContent = self.names[key];
      row.appendChild(swatch);
      row.appendChild(value);
      row.appendChild(name);
      self.tooltip.appendChild(row);
    });
    this.tooltip.hidden = false;
    // Beside the crosshair, flipped to the left near the right edge.
    var box = this.svg.getBoundingClientRect();
    var pixelX = (x / this.width) * box.width;
    var tipWidth = this.tooltip.offsetWidth;
    var left = pixelX + 14;
    if (left + tipWidth > box.width) {
      left = Math.max(0, pixelX - tipWidth - 14);
    }
    this.tooltip.style.left = left + "px";
    this.tooltip.style.top = MARGIN.top + "px";
  };

  // ------------------------------------------------------------- live chart

  function setupLive(figure) {
    var chart = new LineChart(figure);
    var url = figure.getAttribute("data-stats-url");
    // The latest numbers for screen readers (read on demand, not announced), and the region
    // that announces a refresh starting to fail (once per failure, not every poll).
    var status = document.getElementById("live-status");
    var alertRegion = document.getElementById("live-alert");
    var failing = false;
    var template = figure.getAttribute("data-status-template") || "";
    var errorText = figure.getAttribute("data-error-text") || "";
    var labels = [];
    var axisLabels = [];
    var values = { processed: [], failed: [] };
    var previous = null;
    var timer = null;

    var control = document.getElementById("interval-control");
    var slider = document.getElementById("poll-interval");
    var output = document.getElementById("poll-interval-value");
    if (control && slider && output) {
      // The slider covers the intervals the server accepts, so it always shows the one in use.
      var seconds = Math.min(Number(slider.max), Math.max(Number(slider.min), Math.round(shared.pollInterval() / 1000)));
      slider.value = String(seconds);
      var unit = output.textContent.replace(/^[\d\s]+/, "");
      var showValue = function () {
        output.textContent = slider.value + " " + unit;
      };
      showValue();
      control.hidden = false;
      slider.addEventListener("input", showValue);
      slider.addEventListener("change", function () {
        shared.storageSet(shared.pollKey, String(Number(slider.value) * 1000));
        schedule();
      });
    }

    function schedule() {
      window.clearTimeout(timer);
      timer = window.setTimeout(poll, shared.pollInterval());
    }

    function setText(id, value) {
      var node = document.getElementById(id);
      if (node && value !== undefined && node.textContent !== String(value)) {
        node.textContent = String(value);
      }
    }

    function poll() {
      if (document.hidden) {
        schedule();
        return;
      }
      window
        .fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
        .then(function (response) {
          if (!response.ok) {
            throw new Error("HTTP " + response.status);
          }
          return response.json();
        })
        .then(function (data) {
          figure.classList.remove("is-stale");
          var stats = data.hench || {};
          var clock = String(data.server_utc_time || "").replace(/\s*UTC$/, "");
          if (previous) {
            var done = Math.max(0, (stats.processed || 0) - (previous.processed || 0));
            var failed = Math.max(0, (stats.failed || 0) - (previous.failed || 0));
            labels.push(clock + " UTC");
            axisLabels.push(clock);
            values.processed.push(done);
            values.failed.push(failed);
            if (labels.length > LIVE_POINTS) {
              labels.shift();
              axisLabels.shift();
              values.processed.shift();
              values.failed.shift();
            }
            chart.setData(labels, axisLabels, values);
            var text = template.replace("{processed}", full(done)).replace("{failed}", full(failed));
            if (status && status.textContent !== text) {
              status.textContent = text;
            }
          }
          if (failing) {
            failing = false;
            if (alertRegion) {
              alertRegion.textContent = "";
            }
          }
          previous = stats;
          Object.keys(stats).forEach(function (key) {
            var cells = document.querySelectorAll('[data-total="' + key + '"]');
            for (var i = 0; i < cells.length; i++) {
              var text = full(stats[key]);
              if (cells[i].textContent !== text) {
                cells[i].textContent = text;
              }
            }
          });
          var redis = data.redis || {};
          setText("redis-version", redis.store_version);
          setText("redis-uptime", redis.uptime_in_days);
          setText("redis-connections", redis.connected_clients);
          setText("redis-memory", redis.used_memory_human);
          setText("redis-peak-memory", redis.used_memory_peak_human);
          shared.setClock(Date.parse(data.server_time));
        })
        .catch(function () {
          figure.classList.add("is-stale");
          if (chart.note && !labels.length) {
            chart.note.textContent = errorText;
            chart.note.hidden = false;
          }
          if (!failing) {
            failing = true;
            if (alertRegion) {
              alertRegion.textContent = errorText;
            }
          }
        })
        .then(schedule);
    }

    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) {
        poll();
      }
    });
    chart.render();
    poll();
    return chart;
  }

  // ---------------------------------------------------------- history chart

  function setupHistory(figure) {
    var chart = new LineChart(figure);
    var data;
    try {
      data = JSON.parse(figure.getAttribute("data-series") || "{}");
    } catch (error) {
      data = {};
    }
    var dates = data.dates || [];
    var short = dates.map(function (date) {
      var parsed = new Date(date + "T00:00:00Z");
      if (isNaN(parsed.getTime()) || !window.Intl) {
        return date;
      }
      return new Intl.DateTimeFormat("en", { month: "short", day: "numeric", timeZone: "UTC" }).format(parsed);
    });
    chart.setData(dates, short, { processed: data.processed || [], failed: data.failed || [] });
    return chart;
  }

  function start() {
    var charts = [];
    var live = document.getElementById("live-chart");
    if (live && window.fetch) {
      charts.push(setupLive(live));
    }
    var history = document.getElementById("history-chart");
    if (history) {
      charts.push(setupHistory(history));
    }
    var pending = null;
    window.addEventListener("resize", function () {
      window.clearTimeout(pending);
      pending = window.setTimeout(function () {
        charts.forEach(function (chart) {
          chart.render();
        });
      }, 150);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
