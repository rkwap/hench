### bin/sidekiqload: one process drains a queue of no-op jobs

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| LATENCY=0 THREADS=10 | hench-reliable | 5 | 500,000 | 7.64 s | 65,450 jobs/s | 63,775–65,453 | 7.56 s |
| LATENCY=0 THREADS=10 | hench-basic | 5 | 500,000 | 4.42 s | 113,051 jobs/s | 108,125–113,067 | 4.39 s |
| LATENCY=1 THREADS=30 | hench-reliable | 5 | 500,000 | 45.73 s | 10,933 jobs/s | 10,932–10,938 | 45.64 s |
| LATENCY=1 THREADS=30 | hench-basic | 5 | 500,000 | 24.08 s | 20,765 jobs/s | 20,763–20,775 | 24.02 s |
| LATENCY=1 THREADS=100 | hench-reliable | 5 | 500,000 | 18.03 s | 27,739 jobs/s | 27,733–28,052 | 17.89 s |
| LATENCY=1 THREADS=100 | hench-basic | 5 | 500,000 | 10.55 s | 47,381 jobs/s | 47,358–47,390 | 10.50 s |

### bin/multi_queue_bench: 8 processes drain 8 queues

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| PROCESSES=8 QUEUES=8 THREADS=10 | hench-reliable | 3 | 8,000,000 | 82.01 s | 97,552 jobs/s | 97,550–97,796 |  |
| PROCESSES=8 QUEUES=8 THREADS=10 | hench-basic | 3 | 8,000,000 | 48.93 s | 163,510 jobs/s | 162,147–163,660 |  |

