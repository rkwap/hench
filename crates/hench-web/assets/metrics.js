// hench dashboard: the charts of the metrics pages, drawn as SVG by hand.
//
// * /metrics: the execution time of each job class over the period, one line per class
//   ticked in the table (eight at most; a class keeps its color while others come and go);
// * /metrics/{class}: executions and failures over the period, the distribution of the
//   execution times (columns) and the execution times of every minute (a heat map);
// * deploy marks on the time charts; every chart has a tooltip that follows the pointer and
//   the arrow keys; pages showing the last hour refresh every minute.
//
// The tables on the pages hold the same numbers, for reading without the charts. Data comes
// from `data-chart` attributes (JSON): the content security policy forbids inline scripts.
(function () {
  "use strict";

  var SVG_NS = "http://www.w3.org/2000/svg";
  // Categorical colors (hench.css: --cat-1 … --cat-8) and sequential steps (--heat-1 … 5).
  var SLOTS = 8;
  var HEAT_STEPS = 5;
  var MARGIN = { top: 16, right: 18, bottom: 28, left: 60 };
  var HEAT_ROW = 18;
  var MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  var charts = [];

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

  function element(name, className, text) {
    var node = document.createElement(name);
    if (className) {
      node.className = className;
    }
    if (text !== undefined) {
      node.textContent = text;
    }
    return node;
  }

  function readData(figure) {
    try {
      return JSON.parse(figure.getAttribute("data-chart") || "null");
    } catch (error) {
      return null;
    }
  }

  // ---------------------------------------------------------------- numbers

  function formatNumber(value, decimals) {
    return Number(value).toLocaleString("en-US", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals
    });
  }

  function compact(value) {
    var units = [
      [1e9, "B"],
      [1e6, "M"],
      [1e3, "K"]
    ];
    for (var i = 0; i < units.length; i++) {
      if (value >= units[i][0]) {
        return (value / units[i][0]).toFixed(1).replace(/\.0$/, "") + units[i][1];
      }
    }
    return String(value);
  }

  // Decimals needed to print every multiple of `step` exactly (2.5 → 1, 0.05 → 2).
  function decimalsOf(step) {
    var decimals = 0;
    while (decimals < 6 && Math.abs(Math.round(step * Math.pow(10, decimals)) - step * Math.pow(10, decimals)) > 1e-6) {
      decimals += 1;
    }
    return decimals;
  }

  // A clean value axis: about four steps of 1, 2, 2.5 or 5 times a power of ten.
  function niceAxis(max, whole) {
    if (!(max > 0)) {
      return whole ? { max: 4, step: 1, decimals: 0 } : { max: 1, step: 0.25, decimals: 2 };
    }
    var raw = max / 4;
    var power = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10));
    var fraction = raw / power;
    var nice = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 2.5 ? 2.5 : fraction <= 5 ? 5 : 10;
    var step = nice * power;
    if (whole) {
      step = Math.max(1, Math.ceil(step));
    }
    return { max: Math.ceil(max / step - 1e-9) * step, step: step, decimals: whole ? 0 : decimalsOf(step) };
  }

  function tickText(value, scale) {
    return scale.max >= 10000 ? compact(value) : formatNumber(value, scale.decimals);
  }

  // ------------------------------------------------------------------ times

  function pad(n) {
    return (n < 10 ? "0" : "") + n;
  }

  function parseTime(iso) {
    var date = new Date(iso);
    return isNaN(date.getTime()) ? null : date;
  }

  // Axis labels: the time of day for minute buckets, the day as well for longer periods.
  function axisLabel(iso, granularity) {
    var date = parseTime(iso);
    if (!date) {
      return iso;
    }
    var time = pad(date.getUTCHours()) + ":" + pad(date.getUTCMinutes());
    return granularity === "ten_minutes" ? MONTHS[date.getUTCMonth()] + " " + date.getUTCDate() + " " + time : time;
  }

  function longLabel(iso) {
    var date = parseTime(iso);
    if (!date) {
      return iso;
    }
    return (
      MONTHS[date.getUTCMonth()] + " " + date.getUTCDate() + ", " + pad(date.getUTCHours()) + ":" + pad(date.getUTCMinutes()) + " UTC"
    );
  }

  // Which bucket labels to print along an axis: about six that fit, always the last one.
  function labelIndexes(count, width, labelWidth) {
    var indexes = [];
    if (count <= 0) {
      return indexes;
    }
    var room = Math.max(1, Math.floor(width / labelWidth));
    var every = Math.max(1, Math.ceil(count / Math.min(room, 6)));
    for (var i = count - 1; i >= 0; i -= every) {
      indexes.push(i);
    }
    return indexes;
  }

  // ---------------------------------------------------------------- tooltip

  function Tooltip(plot) {
    this.plot = plot;
    this.node = element("div", "chart-tooltip");
    this.node.hidden = true;
    plot.appendChild(this.node);
  }

  // `rows`: [{key: "slot-3" | "processed" | null, value: "1.25 s", name: "ImportJob"}].
  Tooltip.prototype.show = function (title, rows, notes, x, y, chartWidth) {
    var node = this.node;
    node.replaceChildren();
    node.appendChild(element("div", "chart-tooltip-title", title));
    rows.forEach(function (row) {
      var line = element("div", "chart-tooltip-row");
      if (row.key) {
        line.appendChild(element("span", "legend-key " + row.key));
      }
      line.appendChild(element("strong", null, row.value));
      line.appendChild(element("span", null, row.name));
      node.appendChild(line);
    });
    (notes || []).forEach(function (note) {
      node.appendChild(element("div", "chart-tooltip-note", note));
    });
    node.hidden = false;
    // Beside the point, flipped to the left near the right edge.
    var width = node.offsetWidth;
    var left = x + 14;
    if (left + width > chartWidth) {
      left = Math.max(0, x - width - 14);
    }
    node.style.left = left + "px";
    node.style.top = Math.max(0, y) + "px";
  };

  Tooltip.prototype.hide = function () {
    this.node.hidden = true;
  };

  // Keyboard navigation shared by the charts: arrows, Home and End move `index` within
  // `count`; returns the new index or null when the key is not handled.
  function moveIndex(event, index, count) {
    if (event.key === "ArrowLeft") {
      return Math.max(0, index - 1);
    }
    if (event.key === "ArrowRight") {
      return Math.min(count - 1, index + 1);
    }
    if (event.key === "Home") {
      return 0;
    }
    if (event.key === "End") {
      return count - 1;
    }
    return null;
  }

  // Pointer position in the SVG's own coordinates.
  function pointer(root, width, height, event) {
    var box = root.getBoundingClientRect();
    return {
      x: ((event.clientX - box.left) * width) / Math.max(1, box.width),
      y: ((event.clientY - box.top) * height) / Math.max(1, box.height),
      scale: box.width / Math.max(1, width)
    };
  }

  // The unit of a count, from the figure's labels: "1 job", "3 jobs".
  function units(figure) {
    var many = figure.getAttribute("data-label-unit") || "";
    var one = figure.getAttribute("data-label-unit-one") || many;
    return function (count) {
      return count === 1 ? one : many;
    };
  }

  // Deploy marks: [{index, label}] of the marks falling on a bucket of the chart.
  function placeMarks(marks, buckets) {
    var positions = {};
    buckets.forEach(function (bucket, index) {
      positions[bucket] = index;
    });
    return (marks || [])
      .map(function (mark) {
        return { index: positions[mark.bucket], label: String(mark.label) };
      })
      .filter(function (mark) {
        return mark.index !== undefined;
      });
  }

  function marksAt(marks, index, deployLabel) {
    return marks
      .filter(function (mark) {
        return mark.index === index;
      })
      .map(function (mark) {
        return deployLabel + ": " + mark.label;
      });
  }

  function drawMarks(root, marks, x, top, bottom) {
    marks.forEach(function (mark) {
      var position = Math.round(x(mark.index)) + 0.5;
      svg("line", { class: "mark-line", x1: position, x2: position, y1: top, y2: bottom }, root);
      svg("path", { class: "mark-flag", d: "M" + (position - 4) + " " + (top - 7) + " h8 l-4 6 z" }, root);
    });
  }

  // ------------------------------------------------------------ time charts

  // Lines over the period's buckets: one per visible series, each with its own color slot.
  function TimeChart(figure, data) {
    this.figure = figure;
    this.plot = figure.querySelector("[data-chart-plot]");
    this.legend = figure.querySelector("[data-legend]");
    this.buckets = data.buckets || [];
    this.granularity = data.granularity || "minute";
    this.series = (data.series || []).map(function (series) {
      return { name: String(series.name), values: series.values || [] };
    });
    this.decimals = Number(data.decimals) || 0;
    this.unit = data.unit ? " " + data.unit : "";
    this.deployLabel = figure.getAttribute("data-label-deploy") || "Deploy";
    this.marks = placeMarks(data.marks, this.buckets);
    this.slots = {};
    this.index = null;
    this.tooltip = new Tooltip(this.plot);
  }

  TimeChart.prototype.visible = function () {
    var slots = this.slots;
    return this.series
      .map(function (_, index) {
        return index;
      })
      .filter(function (index) {
        return Object.prototype.hasOwnProperty.call(slots, index);
      });
  };

  TimeChart.prototype.isFull = function () {
    return this.visible().length >= SLOTS;
  };

  // Shows series `index` in the first free color slot; false when every slot is taken.
  TimeChart.prototype.showSeries = function (index) {
    if (Object.prototype.hasOwnProperty.call(this.slots, index)) {
      return true;
    }
    var used = {};
    var slots = this.slots;
    Object.keys(slots).forEach(function (key) {
      used[slots[key]] = true;
    });
    for (var slot = 1; slot <= SLOTS; slot++) {
      if (!used[slot]) {
        this.slots[index] = slot;
        return true;
      }
    }
    return false;
  };

  TimeChart.prototype.hideSeries = function (index) {
    delete this.slots[index];
  };

  TimeChart.prototype.slotClass = function (index) {
    return "has-slot slot-" + this.slots[index];
  };

  TimeChart.prototype.x = function (index) {
    var count = this.buckets.length;
    var width = this.width - MARGIN.left - MARGIN.right;
    return count <= 1 ? MARGIN.left + width / 2 : MARGIN.left + (width * index) / (count - 1);
  };

  TimeChart.prototype.y = function (value) {
    var height = this.height - MARGIN.top - MARGIN.bottom;
    return MARGIN.top + height - (height * value) / this.scale.max;
  };

  TimeChart.prototype.format = function (value) {
    return formatNumber(value || 0, this.decimals) + this.unit;
  };

  TimeChart.prototype.render = function () {
    var self = this;
    if (this.svg) {
      this.svg.remove();
    }
    this.width = Math.max(this.plot.clientWidth, 240);
    this.height = Math.max(this.plot.clientHeight, 140);
    var visible = this.visible();
    var count = this.buckets.length;
    var max = 0;
    visible.forEach(function (index) {
      self.series[index].values.forEach(function (value) {
        max = Math.max(max, Number(value) || 0);
      });
    });
    this.scale = niceAxis(max, this.decimals === 0);

    var root = svg("svg", {
      viewBox: "0 0 " + this.width + " " + this.height,
      role: "img",
      "aria-label": this.summary(visible),
      tabindex: count ? 0 : -1,
      focusable: count ? "true" : "false"
    });
    this.svg = root;

    for (var tick = 0; tick <= this.scale.max + 1e-9; tick += this.scale.step) {
      var y = Math.round(this.y(tick)) + 0.5;
      svg("line", { class: tick === 0 ? "baseline" : "gridline", x1: MARGIN.left, x2: this.width - MARGIN.right, y1: y, y2: y }, root);
      svg("text", { class: "tick", x: MARGIN.left - 8, y: y + 4, "text-anchor": "end" }, root).textContent = tickText(tick, this.scale);
    }
    var labelWidth = this.granularity === "ten_minutes" ? 96 : 56;
    labelIndexes(count, this.width - MARGIN.left - MARGIN.right, labelWidth).forEach(function (index) {
      svg(
        "text",
        { class: "tick", x: self.x(index), y: self.height - 8, "text-anchor": index === count - 1 ? "end" : index === 0 ? "start" : "middle" },
        root
      ).textContent = axisLabel(self.buckets[index], self.granularity);
    });
    drawMarks(root, this.marks, this.x.bind(this), MARGIN.top, this.height - MARGIN.bottom);

    visible.forEach(function (index) {
      var values = self.series[index].values;
      if (!values.length) {
        return;
      }
      var path = values
        .map(function (value, position) {
          return (position ? "L" : "M") + self.x(position).toFixed(1) + " " + self.y(Number(value) || 0).toFixed(1);
        })
        .join(" ");
      svg("path", { class: "line " + self.slotClass(index), d: path }, root);
      var last = values.length - 1;
      svg("circle", { class: "marker " + self.slotClass(index), cx: self.x(last), cy: self.y(Number(values[last]) || 0), r: 4 }, root);
    });

    this.crosshair = svg("line", { class: "crosshair", y1: MARGIN.top, y2: this.height - MARGIN.bottom, visibility: "hidden" }, root);
    this.hover = visible.map(function (index) {
      return { index: index, node: svg("circle", { class: "marker " + self.slotClass(index), r: 5, visibility: "hidden" }, root) };
    });
    var hit = svg(
      "rect",
      { class: "hit-area", x: MARGIN.left - 12, y: 0, width: this.width - MARGIN.left - MARGIN.right + 24, height: this.height },
      root
    );
    hit.addEventListener("pointermove", function (event) {
      self.point(self.nearest(pointer(root, self.width, self.height, event).x));
    });
    hit.addEventListener("pointerleave", function () {
      self.point(null);
    });
    root.addEventListener("focus", function () {
      self.point(self.index === null ? count - 1 : self.index);
    });
    root.addEventListener("blur", function () {
      self.point(null);
    });
    root.addEventListener("keydown", function (event) {
      if (!count) {
        return;
      }
      var next = moveIndex(event, self.index === null ? count - 1 : self.index, count);
      if (next !== null) {
        event.preventDefault();
        self.point(next);
      }
    });
    this.plot.insertBefore(root, this.tooltip.node);
    this.renderLegend(visible);
    if (this.index !== null) {
      this.point(this.index);
    }
  };

  TimeChart.prototype.renderLegend = function (visible) {
    var self = this;
    if (!this.legend) {
      return;
    }
    this.legend.replaceChildren();
    visible.forEach(function (index) {
      var item = element("span", "legend-item");
      item.appendChild(element("span", "legend-key " + self.slotClass(index)));
      item.appendChild(document.createTextNode(self.series[index].name));
      self.legend.appendChild(item);
    });
  };

  TimeChart.prototype.summary = function (visible) {
    var self = this;
    var title = this.figure.querySelector(".chart-title");
    var names = visible.map(function (index) {
      return self.series[index].name;
    });
    return (title ? title.textContent : "") + (names.length ? ": " + names.join(", ") : "");
  };

  TimeChart.prototype.nearest = function (x) {
    var count = this.buckets.length;
    if (!count) {
      return null;
    }
    if (count === 1) {
      return 0;
    }
    var width = this.width - MARGIN.left - MARGIN.right;
    return Math.min(count - 1, Math.max(0, Math.round(((x - MARGIN.left) / width) * (count - 1))));
  };

  TimeChart.prototype.point = function (index) {
    var self = this;
    this.index = index;
    if (index === null || !this.buckets.length || !this.crosshair) {
      if (this.crosshair) {
        this.crosshair.setAttribute("visibility", "hidden");
      }
      (this.hover || []).forEach(function (hover) {
        hover.node.setAttribute("visibility", "hidden");
      });
      this.tooltip.hide();
      return;
    }
    var x = this.x(index);
    this.crosshair.setAttribute("x1", x);
    this.crosshair.setAttribute("x2", x);
    this.crosshair.setAttribute("visibility", "visible");
    this.hover.forEach(function (hover) {
      hover.node.setAttribute("cx", x);
      hover.node.setAttribute("cy", self.y(Number(self.series[hover.index].values[index]) || 0));
      hover.node.setAttribute("visibility", "visible");
    });
    var rows = this.hover.map(function (hover) {
      return {
        key: self.slotClass(hover.index),
        value: self.format(self.series[hover.index].values[index]),
        name: self.series[hover.index].name
      };
    });
    var scale = this.svg.getBoundingClientRect().width / this.width;
    this.tooltip.show(
      longLabel(this.buckets[index]),
      rows,
      marksAt(this.marks, index, this.deployLabel),
      x * scale,
      MARGIN.top,
      this.plot.clientWidth
    );
  };

  // ------------------------------------------------------------- histogram

  // Columns: executions per duration bucket, fastest on the left.
  function ColumnChart(figure, data) {
    this.figure = figure;
    this.plot = figure.querySelector("[data-chart-plot]");
    this.labels = data.labels || [];
    this.ranges = data.ranges || [];
    this.counts = (data.counts || []).map(function (count) {
      return Number(count) || 0;
    });
    this.units = units(figure);
    this.index = null;
    this.tooltip = new Tooltip(this.plot);
  }

  ColumnChart.prototype.render = function () {
    var self = this;
    if (this.svg) {
      this.svg.remove();
    }
    this.width = Math.max(this.plot.clientWidth, 240);
    this.height = Math.max(this.plot.clientHeight, 140);
    var count = this.counts.length;
    var max = Math.max.apply(null, this.counts.concat([0]));
    this.scale = niceAxis(max, true);
    var plotWidth = this.width - MARGIN.left - MARGIN.right;
    var plotHeight = this.height - MARGIN.top - MARGIN.bottom;
    this.band = count ? plotWidth / count : plotWidth;
    var barWidth = Math.max(2, Math.min(24, this.band - 2));
    var y = function (value) {
      return MARGIN.top + plotHeight - (plotHeight * value) / self.scale.max;
    };

    var root = svg("svg", {
      viewBox: "0 0 " + this.width + " " + this.height,
      role: "img",
      "aria-label": this.figure.querySelector(".chart-title") ? this.figure.querySelector(".chart-title").textContent : "",
      tabindex: count ? 0 : -1,
      focusable: count ? "true" : "false"
    });
    this.svg = root;
    for (var tick = 0; tick <= this.scale.max + 1e-9; tick += this.scale.step) {
      var tickY = Math.round(y(tick)) + 0.5;
      svg("line", { class: tick === 0 ? "baseline" : "gridline", x1: MARGIN.left, x2: this.width - MARGIN.right, y1: tickY, y2: tickY }, root);
      svg("text", { class: "tick", x: MARGIN.left - 8, y: tickY + 4, "text-anchor": "end" }, root).textContent = tickText(tick, this.scale);
    }
    var every = Math.max(1, Math.ceil(46 / Math.max(1, this.band)));
    var tallest = this.counts.indexOf(max);
    this.columns = this.counts.map(function (value, index) {
      var center = MARGIN.left + self.band * index + self.band / 2;
      if ((count - 1 - index) % every === 0) {
        svg("text", { class: "tick", x: center, y: self.height - 8, "text-anchor": "middle" }, root).textContent = self.labels[index];
      }
      var node = null;
      if (value > 0) {
        var top = y(value);
        var bottom = y(0);
        var radius = Math.min(4, barWidth / 2, bottom - top);
        var left = center - barWidth / 2;
        node = svg(
          "path",
          {
            class: "column",
            d:
              "M" + left + " " + bottom +
              " V" + (top + radius) +
              " Q" + left + " " + top + " " + (left + radius) + " " + top +
              " H" + (left + barWidth - radius) +
              " Q" + (left + barWidth) + " " + top + " " + (left + barWidth) + " " + (top + radius) +
              " V" + bottom + " Z"
          },
          root
        );
        if (index === tallest) {
          // The extreme gets its value on the cap; the others are in the tooltip and table.
          svg("text", { class: "value-label", x: center, y: top - 6, "text-anchor": "middle" }, root).textContent = formatNumber(value, 0);
        }
      }
      return { node: node, center: center, top: value > 0 ? y(value) : y(0) };
    });
    var hit = svg("rect", { class: "hit-area", x: MARGIN.left, y: 0, width: plotWidth, height: this.height }, root);
    hit.addEventListener("pointermove", function (event) {
      var position = pointer(root, self.width, self.height, event);
      self.point(Math.min(count - 1, Math.max(0, Math.floor((position.x - MARGIN.left) / self.band))));
    });
    hit.addEventListener("pointerleave", function () {
      self.point(null);
    });
    root.addEventListener("focus", function () {
      self.point(self.index === null ? Math.max(0, tallest) : self.index);
    });
    root.addEventListener("blur", function () {
      self.point(null);
    });
    root.addEventListener("keydown", function (event) {
      if (!count) {
        return;
      }
      var next = moveIndex(event, self.index === null ? Math.max(0, tallest) : self.index, count);
      if (next !== null) {
        event.preventDefault();
        self.point(next);
      }
    });
    this.plot.insertBefore(root, this.tooltip.node);
    if (this.index !== null) {
      this.point(this.index);
    }
  };

  ColumnChart.prototype.point = function (index) {
    var self = this;
    this.index = index;
    (this.columns || []).forEach(function (column, position) {
      if (column.node) {
        column.node.classList.toggle("is-active", position === index);
      }
    });
    if (index === null || !this.columns || !this.columns[index]) {
      this.tooltip.hide();
      return;
    }
    var column = this.columns[index];
    var scale = this.svg.getBoundingClientRect().width / this.width;
    this.tooltip.show(
      this.ranges[index] || this.labels[index],
      [{ key: null, value: formatNumber(self.counts[index], 0), name: this.units(self.counts[index]) }],
      [],
      column.center * scale,
      Math.max(0, column.top * scale - 40),
      this.plot.clientWidth
    );
  };

  // -------------------------------------------------------------- heat map

  // One step of the sequential scale for a count: linear up to five, logarithmic beyond.
  function heatStep(count, max) {
    if (!(count > 0)) {
      return 0;
    }
    if (max <= HEAT_STEPS) {
      return Math.max(1, Math.min(HEAT_STEPS, Math.round((count * HEAT_STEPS) / max)));
    }
    return Math.max(1, Math.min(HEAT_STEPS, Math.ceil((HEAT_STEPS * Math.log(count)) / Math.log(max))));
  }

  // Cells: executions per minute (columns) and duration bucket (rows, fastest at the bottom).
  function HeatMap(figure, data) {
    this.figure = figure;
    this.plot = figure.querySelector("[data-chart-plot]");
    this.buckets = data.buckets || [];
    this.granularity = data.granularity || "minute";
    this.rows = (data.rows || []).map(function (row) {
      return { label: String(row.label), range: String(row.range), counts: row.counts || [] };
    });
    this.units = units(figure);
    this.deployLabel = figure.getAttribute("data-label-deploy") || "Deploy";
    this.marks = placeMarks(data.marks, this.buckets);
    this.cell = null;
    this.tooltip = new Tooltip(this.plot);
    this.max = 0;
    var self = this;
    this.rows.forEach(function (row) {
      row.counts.forEach(function (count) {
        self.max = Math.max(self.max, Number(count) || 0);
      });
    });
    // The plot grows with the rows, so that the axis labels always fit.
    this.plot.style.height = this.rows.length * HEAT_ROW + MARGIN.top + MARGIN.bottom + "px";
  }

  HeatMap.prototype.render = function () {
    var self = this;
    if (this.svg) {
      this.svg.remove();
    }
    this.width = Math.max(this.plot.clientWidth, 240);
    this.height = this.rows.length * HEAT_ROW + MARGIN.top + MARGIN.bottom;
    var columns = this.buckets.length;
    var plotWidth = this.width - MARGIN.left - MARGIN.right;
    this.column = columns ? plotWidth / columns : plotWidth;
    var gap = this.column >= 8 ? 2 : 0;

    var root = svg("svg", {
      viewBox: "0 0 " + this.width + " " + this.height,
      role: "img",
      "aria-label": this.figure.querySelector(".chart-title") ? this.figure.querySelector(".chart-title").textContent : "",
      tabindex: columns && this.rows.length ? 0 : -1,
      focusable: columns && this.rows.length ? "true" : "false"
    });
    this.svg = root;
    this.rows.forEach(function (row, rowIndex) {
      var top = self.rowTop(rowIndex);
      svg("text", { class: "tick", x: MARGIN.left - 8, y: top + HEAT_ROW / 2 + 4, "text-anchor": "end" }, root).textContent = row.label;
      row.counts.forEach(function (count, columnIndex) {
        var step = heatStep(Number(count) || 0, self.max);
        if (step) {
          svg(
            "rect",
            {
              class: "cell heat-" + step,
              x: MARGIN.left + columnIndex * self.column + gap / 2,
              y: top + 1,
              width: Math.max(1, self.column - gap),
              height: HEAT_ROW - 2,
              rx: gap ? 2 : 0
            },
            root
          );
        }
      });
    });
    var bottom = this.height - MARGIN.bottom;
    svg("line", { class: "baseline", x1: MARGIN.left, x2: this.width - MARGIN.right, y1: bottom + 0.5, y2: bottom + 0.5 }, root);
    var labelWidth = this.granularity === "ten_minutes" ? 96 : 56;
    labelIndexes(columns, plotWidth, labelWidth).forEach(function (index) {
      svg(
        "text",
        { class: "tick", x: self.center(index), y: self.height - 8, "text-anchor": index === columns - 1 ? "end" : index === 0 ? "start" : "middle" },
        root
      ).textContent = axisLabel(self.buckets[index], self.granularity);
    });
    drawMarks(root, this.marks, this.center.bind(this), MARGIN.top, bottom);
    this.focus = svg("rect", { class: "cell-focus", width: Math.max(2, this.column), height: HEAT_ROW, visibility: "hidden" }, root);
    var hit = svg("rect", { class: "hit-area", x: MARGIN.left, y: MARGIN.top, width: plotWidth, height: this.rows.length * HEAT_ROW }, root);
    hit.addEventListener("pointermove", function (event) {
      var position = pointer(root, self.width, self.height, event);
      var column = Math.min(columns - 1, Math.max(0, Math.floor((position.x - MARGIN.left) / self.column)));
      var row = self.rows.length - 1 - Math.min(self.rows.length - 1, Math.max(0, Math.floor((position.y - MARGIN.top) / HEAT_ROW)));
      self.point({ column: column, row: row });
    });
    hit.addEventListener("pointerleave", function () {
      self.point(null);
    });
    root.addEventListener("focus", function () {
      self.point(self.cell || self.latest());
    });
    root.addEventListener("blur", function () {
      self.point(null);
    });
    root.addEventListener("keydown", function (event) {
      var cell = self.cell || self.latest();
      if (!cell) {
        return;
      }
      var next = { column: cell.column, row: cell.row };
      if (event.key === "ArrowUp") {
        next.row = Math.min(self.rows.length - 1, cell.row + 1);
      } else if (event.key === "ArrowDown") {
        next.row = Math.max(0, cell.row - 1);
      } else {
        var column = moveIndex(event, cell.column, columns);
        if (column === null) {
          return;
        }
        next.column = column;
      }
      event.preventDefault();
      self.point(next);
    });
    this.plot.insertBefore(root, this.tooltip.node);
    if (this.cell) {
      this.point(this.cell);
    }
  };

  HeatMap.prototype.rowTop = function (row) {
    return MARGIN.top + (this.rows.length - 1 - row) * HEAT_ROW;
  };

  HeatMap.prototype.center = function (column) {
    return MARGIN.left + column * this.column + this.column / 2;
  };

  // The busiest cell of the latest minute with executions: where the keyboard starts.
  HeatMap.prototype.latest = function () {
    for (var column = this.buckets.length - 1; column >= 0; column--) {
      var best = null;
      for (var row = 0; row < this.rows.length; row++) {
        var count = Number(this.rows[row].counts[column]) || 0;
        if (count > 0 && (best === null || count > best.count)) {
          best = { row: row, count: count };
        }
      }
      if (best) {
        return { column: column, row: best.row };
      }
    }
    return this.rows.length && this.buckets.length ? { column: this.buckets.length - 1, row: 0 } : null;
  };

  HeatMap.prototype.point = function (cell) {
    this.cell = cell;
    if (!cell || !this.focus || !this.rows[cell.row]) {
      if (this.focus) {
        this.focus.setAttribute("visibility", "hidden");
      }
      this.tooltip.hide();
      return;
    }
    var top = this.rowTop(cell.row);
    this.focus.setAttribute("x", MARGIN.left + cell.column * this.column);
    this.focus.setAttribute("y", top);
    this.focus.setAttribute("visibility", "visible");
    var row = this.rows[cell.row];
    var count = Number(row.counts[cell.column]) || 0;
    var scale = this.svg.getBoundingClientRect().width / this.width;
    this.tooltip.show(
      longLabel(this.buckets[cell.column]),
      [{ key: null, value: formatNumber(count, 0), name: this.units(count) + " · " + row.range }],
      marksAt(this.marks, cell.column, this.deployLabel),
      this.center(cell.column) * scale,
      Math.max(0, (top - 44) * scale),
      this.plot.clientWidth
    );
  };

  // ------------------------------------------------------------------ pages

  // `2026-11-01 14:05` from `2026-11-01T14:05:00Z`, as the server labels table rows.
  function bucketLabel(iso) {
    return String(iso).slice(0, 10) + " " + String(iso).slice(11, 16);
  }

  // The table view of the overview chart: the seconds of the charted classes per bucket,
  // newest first, buckets where none of them took any time left out (as the server renders it
  // for the classes ticked when the page opens).
  function renderTable(table, chart) {
    var head = table && table.querySelector("thead tr");
    var body = table && table.querySelector("tbody");
    if (!head || !body || !head.firstElementChild) {
      return;
    }
    var visible = chart.visible();
    var cells = [head.firstElementChild];
    visible.forEach(function (index) {
      var cell = element("th", "num", chart.series[index].name);
      cell.setAttribute("scope", "col");
      cells.push(cell);
    });
    head.replaceChildren.apply(head, cells);
    var rows = [];
    for (var bucket = chart.buckets.length - 1; bucket >= 0; bucket--) {
      var values = visible.map(function (index) {
        return Number(chart.series[index].values[bucket]) || 0;
      });
      if (!values.some(function (value) { return value > 0; })) {
        continue;
      }
      var row = element("tr");
      var label = element("th", null, bucketLabel(chart.buckets[bucket]));
      label.setAttribute("scope", "row");
      row.appendChild(label);
      values.forEach(function (value) {
        row.appendChild(element("td", "num", formatNumber(value, chart.decimals)));
      });
      rows.push(row);
    }
    body.replaceChildren.apply(body, rows);
  }

  // The metrics page: the chart shows the classes ticked in the table. `remembered` keeps the
  // viewer's choice across refreshes (class names), else the boxes ticked by the server win.
  function setupOverview(root, remembered) {
    var figure = root.querySelector("#metrics-chart");
    var data = figure && readData(figure);
    if (!data) {
      return null;
    }
    var chart = new TimeChart(figure, data);
    var toggles = Array.prototype.slice.call(root.querySelectorAll("[data-series-toggle]"));
    toggles.forEach(function (toggle) {
      var index = Number(toggle.value);
      var series = chart.series[index];
      var wanted = remembered ? series && remembered.indexOf(series.name) >= 0 : toggle.checked;
      toggle.checked = !!(wanted && series && chart.showSeries(index));
    });

    function sync() {
      var full = chart.isFull();
      toggles.forEach(function (toggle) {
        var index = Number(toggle.value);
        var swatch = toggle.parentNode.querySelector("[data-series-swatch]");
        if (swatch) {
          swatch.className = "swatch" + (toggle.checked ? " " + chart.slotClass(index) : "");
        }
        toggle.disabled = !toggle.checked && full;
      });
    }

    var table = root.querySelector("[data-chart-table]");
    toggles.forEach(function (toggle) {
      toggle.hidden = false;
      toggle.addEventListener("change", function () {
        var index = Number(toggle.value);
        if (toggle.checked) {
          toggle.checked = chart.showSeries(index);
        } else {
          chart.hideSeries(index);
        }
        sync();
        chart.render();
        renderTable(table, chart);
      });
    });
    var help = root.querySelector("[data-series-help]");
    if (help && toggles.length) {
      help.hidden = false;
    }
    sync();
    chart.render();
    renderTable(table, chart);
    chart.visibleNames = function () {
      return chart.visible().map(function (index) {
        return chart.series[index].name;
      });
    };
    return chart;
  }

  function setupJob(root) {
    var made = [];
    var series = root.querySelector("#job-series-chart");
    var seriesData = series && readData(series);
    if (seriesData) {
      var chart = new TimeChart(series, seriesData);
      chart.series.forEach(function (_, index) {
        chart.showSeries(index);
      });
      chart.render();
      made.push(chart);
    }
    var histogram = root.querySelector("#histogram-chart");
    var histogramData = histogram && readData(histogram);
    if (histogramData) {
      var columns = new ColumnChart(histogram, histogramData);
      columns.render();
      made.push(columns);
    }
    var heat = root.querySelector("#heatmap-chart");
    var heatData = heat && readData(heat);
    if (heatData) {
      var map = new HeatMap(heat, heatData);
      map.render();
      made.push(map);
      var key = root.querySelector("[data-heat-key]");
      if (key) {
        key.hidden = false;
      }
    }
    return made;
  }

  function init(root, remembered) {
    charts = [];
    var overview = setupOverview(root, remembered);
    if (overview) {
      charts.push(overview);
    }
    charts = charts.concat(setupJob(root));
  }

  // Pages showing the last hour refresh their content in place every minute (keeping the
  // classes the viewer charted), unless the tab is hidden or the viewer is busy with the
  // content: a table view open, or the keyboard focus on something in it (a field, a link, a
  // chart being explored with the arrow keys), which swapping the content would destroy.
  function setupRefresh(root) {
    var seconds = Number(root.getAttribute("data-auto-refresh"));
    if (!(seconds >= 10) || !window.fetch || !window.DOMParser) {
      return;
    }
    function busy() {
      if (root.querySelector("details[open]")) {
        return true;
      }
      var active = document.activeElement;
      return !!(active && active !== root && root.contains(active));
    }
    function schedule() {
      window.setTimeout(refresh, seconds * 1000);
    }
    function refresh() {
      if (document.hidden || busy()) {
        schedule();
        return;
      }
      window
        .fetch(window.location.href, { credentials: "same-origin", headers: { Accept: "text/html" } })
        .then(function (response) {
          return response.ok ? response.text() : null;
        })
        .then(function (html) {
          if (html === null || busy()) {
            return;
          }
          var fresh = new DOMParser().parseFromString(html, "text/html").getElementById("metrics-root");
          if (!fresh) {
            return;
          }
          var remembered = null;
          charts.forEach(function (chart) {
            if (chart.visibleNames) {
              remembered = chart.visibleNames();
            }
          });
          var nodes = Array.prototype.map.call(fresh.childNodes, function (node) {
            return document.importNode(node, true);
          });
          root.replaceChildren.apply(root, nodes);
          init(root, remembered);
        })
        .catch(function () {
          // Network hiccup: try again at the next tick.
        })
        .then(schedule, schedule);
    }
    schedule();
  }

  function start() {
    var root = document.getElementById("metrics-root");
    if (!root) {
      return;
    }
    init(root, null);
    setupRefresh(root);
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
