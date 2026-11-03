### bin/sidekiqload: one process drains a queue of no-op jobs

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| LATENCY=0 THREADS=10 | hench-reliable | 5 | 500,000 | 13.67 s | 36,563 jobs/s | 35,522–37,117 | 13.67 s |
| LATENCY=0 THREADS=10 | hench-basic | 5 | 500,000 | 8.85 s | 56,497 jobs/s | 55,230–56,504 | 8.76 s |
| LATENCY=0 THREADS=10 | sidekiq | 5 | 500,000 | 47.50 s | 10,526 jobs/s | 9,116–11,523 |  |
| LATENCY=1 THREADS=30 | hench-reliable | 5 | 500,000 | 102.81 s | 4,863 jobs/s | 4,833–4,891 | 102.75 s |
| LATENCY=1 THREADS=30 | hench-basic | 5 | 500,000 | 52.03 s | 9,609 jobs/s | 9,534–9,761 | 52.03 s |
| LATENCY=1 THREADS=30 | sidekiq | 5 | 500,000 | 110.92 s | 4,507 jobs/s | 4,486–4,629 |  |
| LATENCY=1 THREADS=100 | hench-reliable | 5 | 500,000 | 33.48 s | 14,934 jobs/s | 14,838–15,025 | 33.40 s |
| LATENCY=1 THREADS=100 | hench-basic | 5 | 500,000 | 17.15 s | 29,154 jobs/s | 29,134–29,492 | 17.02 s |
| LATENCY=1 THREADS=100 | sidekiq | 5 | 500,000 | 112.91 s | 4,428 jobs/s | 4,412–4,480 |  |

### bin/webload: requests to one dashboard page, in process

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| /busy | hench | 5 | 10,000 | 38.35 s | 260 requests/s | 192–385 |  |
| /busy | sidekiq | 5 | 10,000 | 17.03 s | 587 requests/s | 519–658 |  |
| /queues | hench | 5 | 10,000 | 21.81 s | 458 requests/s | 319–1,614 |  |
| /queues | sidekiq | 5 | 10,000 | 10.34 s | 966 requests/s | 854–1,376 |  |

### bin/multi_queue_bench: 8 processes drain 8 queues

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| PROCESSES=8 QUEUES=8 THREADS=10 | hench-reliable | 3 | 8,000,000 | 87.26 s | 91,682 jobs/s | 91,450–91,897 |  |
| PROCESSES=8 QUEUES=8 THREADS=10 | hench-basic | 3 | 8,000,000 | 52.05 s | 153,688 jobs/s | 153,604–154,871 |  |
| PROCESSES=8 QUEUES=8 THREADS=10 | sidekiq | 3 | 8,000,000 | 59.89 s | 133,588 jobs/s | 132,990–134,498 |  |

