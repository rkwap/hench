### bin/sidekiqload: one process drains a queue of no-op jobs

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| LATENCY=0 THREADS=10 | hench-reliable | 5 | 500,000 | 7.64 s | 65,456 jobs/s | 65,449–67,227 | 7.46 s |
| LATENCY=0 THREADS=10 | hench-basic | 5 | 500,000 | 4.42 s | 113,046 jobs/s | 113,030–113,058 | 4.35 s |
| LATENCY=0 THREADS=10 | sidekiq | 5 | 500,000 | 21.32 s | 23,449 jobs/s | 23,236–23,675 |  |
| LATENCY=1 THREADS=30 | hench-reliable | 5 | 500,000 | 78.96 s | 6,332 jobs/s | 6,328–6,334 | 78.82 s |
| LATENCY=1 THREADS=30 | hench-basic | 5 | 500,000 | 40.18 s | 12,444 jobs/s | 12,442–12,459 | 40.05 s |
| LATENCY=1 THREADS=30 | sidekiq | 5 | 500,000 | 23.63 s | 21,155 jobs/s | 21,151–21,343 |  |
| LATENCY=1 THREADS=100 | hench-reliable | 5 | 500,000 | 26.77 s | 18,677 jobs/s | 18,509–18,681 | 26.63 s |
| LATENCY=1 THREADS=100 | hench-basic | 5 | 500,000 | 14.03 s | 35,635 jobs/s | 35,156–35,668 | 13.95 s |
| LATENCY=1 THREADS=100 | sidekiq | 5 | 500,000 | 24.87 s | 20,106 jobs/s | 19,940–20,271 |  |

### bin/webload: requests to one dashboard page, in process

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| /busy | hench | 5 | 10,000 | 6.63 s | 1,508 requests/s | 1,470–1,534 |  |
| /busy | sidekiq | 5 | 10,000 | 11.18 s | 894 requests/s | 887–932 |  |
| /queues | hench | 5 | 10,000 | 3.60 s | 2,777 requests/s | 2,705–3,389 |  |
| /queues | sidekiq | 5 | 10,000 | 7.10 s | 1,409 requests/s | 1,388–1,501 |  |

### bin/multi_queue_bench: 8 processes drain 8 queues

| Configuration | System | Runs | Work | Median time | Median throughput | Range | Last job ended |
|---|---|---:|---:|---:|---:|---:|---:|
| PROCESSES=8 QUEUES=8 THREADS=10 | hench-reliable | 3 | 8,000,000 | 88.13 s | 90,777 jobs/s | 90,150–91,627 |  |
| PROCESSES=8 QUEUES=8 THREADS=10 | hench-basic | 3 | 8,000,000 | 52.68 s | 151,851 jobs/s | 151,261–152,447 |  |
| PROCESSES=8 QUEUES=8 THREADS=10 | sidekiq | 3 | 8,000,000 | 63.25 s | 126,482 jobs/s | 126,048–128,199 |  |

