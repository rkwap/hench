#!/usr/bin/env bash
# Prints the medians of the runs benchmarks/run.sh recorded, as Markdown tables: for each
# benchmark, configuration and system, the number of runs, the median time and throughput, the
# range of the throughputs and, for Hench's load runs, the median time until the last job
# ended (the scripts stop their clocks when the queue is empty, polled every 200 ms).
#
# Usage: benchmarks/summarize.sh <runs.csv>
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: summarize.sh <runs.csv>" >&2; exit 2; }

awk -F, '
  function median(list, count,    values, n, i, j, value) {
    n = split(list, values, " ")
    for (i = 2; i <= n; i++) {
      value = values[i]
      for (j = i - 1; j >= 1 && values[j] + 0 > value + 0; j--) values[j + 1] = values[j]
      values[j + 1] = value
    }
    lowest = values[1]
    highest = values[n]
    return (n % 2) ? values[(n + 1) / 2] : (values[n / 2] + values[n / 2 + 1]) / 2
  }
  function grouped(number,    text, result) {
    text = sprintf("%d", number + 0.5)
    result = ""
    while (length(text) > 3) {
      result = "," substr(text, length(text) - 2) result
      text = substr(text, 1, length(text) - 3)
    }
    return text result
  }
  NR == 1 { next }
  {
    key = $1 SUBSEP $2 SUBSEP $3
    if (!(key in runs)) { order[++keys] = key; jobs[key] = $5 }
    runs[key]++
    seconds[key] = seconds[key] " " $6
    rates[key] = rates[key] " " $7
    if ($8 != "") lasts[key] = lasts[key] " " $8
  }
  END {
    titles["load"] = "bin/sidekiqload: one process drains a queue of no-op jobs"
    titles["web"] = "bin/webload: requests to one dashboard page, in process"
    titles["multi"] = "bin/multi_queue_bench: 8 processes drain 8 queues"
    units["load"] = "jobs/s"; units["multi"] = "jobs/s"; units["web"] = "requests/s"
    split("load web multi", benchmarks, " ")
    for (b = 1; b <= 3; b++) {
      benchmark = benchmarks[b]
      printed = 0
      for (k = 1; k <= keys; k++) {
        split(order[k], part, SUBSEP)
        if (part[1] != benchmark) continue
        if (!printed) {
          printf "### %s\n\n", titles[benchmark]
          printf "| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |\n"
          printf "|---|---|---:|---:|---:|---:|---:|---:|\n"
          printed = 1
        }
        key = order[k]
        time = median(seconds[key])
        rate = median(rates[key])
        range = grouped(lowest) "–" grouped(highest)
        last = (key in lasts) ? sprintf("%.2f s", median(lasts[key])) : ""
        printf "| %s | %s | %d | %s | %.2f s | %s %s | %s | %s |\n", part[2], part[3], runs[key], grouped(jobs[key]), time, grouped(rate), units[benchmark], range, last
      }
      if (printed) printf "\n"
    }
  }' "$1"
