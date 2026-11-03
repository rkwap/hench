# Recorded runs

Each directory is one session of [`benchmarks/run.sh`](../run.sh), named after its date (UTC)
and the machine's CPU, plus what set a session apart from another on the same machine
(`-c6-enabled`: the CPU idle state C6 was enabled; `-nodelay`: Hench's runs at the commit that
turned `TCP_NODELAY` on):

- `environment.txt`: the machine (model, cores, memory, power), the operating system, the CPU
  idle states (Linux), the instance type (EC2), the commit of Hench measured, and the versions
  of rustc, the Redis server, toxiproxy, Ruby and Sidekiq;
- `raw/`: the output of every run, in the order they ran, each headed by the command line, the
  time and the load average before it (colour codes removed, nothing else);
- `runs.csv`: one line per run: benchmark, configuration, system, round, jobs (or requests),
  seconds, throughput, when Hench's last job ended, load average and the raw output's file;
- `summary.md`: the medians, from [`benchmarks/summarize.sh`](../summarize.sh).

[`benchmarks/README.md`](../README.md) presents the results and how they were measured.
