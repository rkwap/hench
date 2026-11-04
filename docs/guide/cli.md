# The `hench` command

`hench-server` builds the `hench` binary (`cargo install hench-server --locked`, which
builds it with the dependency versions it was tested with): a standalone
dashboard, a JSON API and maintenance commands. Redis comes from `--redis-url`,
`HENCH_REDIS_URL`, or the same variables as the workers. Give secrets (the dashboard's
password, a Redis URL with a password) through the environment: command-line arguments are
visible to every user of the machine (`ps`).

```sh
# The dashboard on port 7420, with Basic authentication and the JSON API; the password is
# read from HENCH_WEB_PASSWORD.
export HENCH_WEB_PASSWORD='a long random password'
hench web --bind 0.0.0.0:7420 --username admin --api
# Read-only, under /jobs, behind a proxy that forwards its own host name.
hench web --read-only --prefix /jobs --allowed-host jobs.example.com

# sidekiqmon-style reports (add --json for machine-readable output).
hench stats                 # version, overview, processes, queues, busiest job classes
hench stats overview
hench processes
hench queues                # with each queue's limits
hench queues limit exports --set "concurrency=3; window=100/1m"   # or --clear

# Signals: quiet (finish running jobs, take no more) and stop.
hench quiet host-1:4242:0a1b2c3d4e5f
hench stop all              # every process that is not embedded in an application

# Push a job from a shell or a cron job. The class is the job's registered name: the
# function's for `#[hench::job]` (`monthly_report` in "Getting started"), else `NAME`.
# `hench` does not know your job types: give the queue (else `default`), the retries (else the
# default) and the throttle (`--throttle NAME[:KEY]`, else none) the job needs.
hench enqueue monthly_report --args '[7, "2026-10"]' --queue reports --in 5m

# The features (see their pages of the guide; `hench <command> --help` for every option).
hench periodic              # the schedules: show, enqueue, pause, unpause, remove, add, ...
hench batches --state dead  # the batches: show, cancel, delete, skip, dispatch
hench limits                # the limiters and throttles: show, reset, clear-line
hench locks                 # the locks of unique jobs: show, release, release-all
```

With `--api`, `GET <prefix>/api/v1/stats`, `/queues` and `/processes` return JSON and
`POST <prefix>/api/v1/jobs` enqueues `{"class": "...", "args": [...], "queue": "...", "in":
60}`. Each process of `/processes` (and of `hench processes --json`) lists the names of the
queues it works on in `queues`, and its capsules by name in `capsules`, with their
concurrency, mode and queue weights (`{"default": {"concurrency": 5, "mode": "weighted",
"weights": {"critical": 3, "default": 1}}}`). `PUT <prefix>/api/v1/queues/{name}/limits` with
`{"limits": "concurrency=3"}` sets a queue's limits and `DELETE` clears them
([queue limits](rate-limiting.md#queue-limits)); `/queues` shows each queue's `limits`. The
features add their routes under the same
prefix (`/periodic`, `/batches`, `/limits`, `/locks`, described in their pages); a
read-only server refuses every change. Requests are read strictly (an unknown field, in the
body or in `unique`, or an unknown
`unique.until` is a `422`), and every error is JSON, `{"error": "..."}`, unknown routes (`404`)
and methods (`405`) included. `GET /healthz` answers `ok` while Redis does (`503` within a
second otherwise). The
server refuses to expose the API on a public address without credentials. Without
credentials it also answers only requests addressed to `localhost` or an IP address: a web
page on another site could otherwise point its own host name at your machine (DNS rebinding)
and drive a dashboard listening on `127.0.0.1`. Name the hosts a proxy forwards with
`--allowed-host` (or `HENCH_WEB_ALLOWED_HOSTS`, comma-separated; `.example.com` covers the
subdomains, `*` every name). Like `hench enqueue`, the JSON API pushes jobs by class name
without knowing their job types: a job without `"queue"` goes to `default`, and one without
`"throttle"` (and `"throttle_key"`) runs unthrottled, whatever its job type declares.

Slow or idle clients cannot hold the server: a request head must arrive within 10 seconds (an
idle connection is closed after as long), a body within 30 seconds, and a response the client
stops reading is abandoned after 30 seconds; at most 512 connections are served at once
(`--max-connections`, below the open-file limit), further clients wait to be accepted. SIGINT
or SIGTERM stops it gracefully: requests in flight get 10 seconds to finish, and a second
signal stops it at once. Expose it to the internet only behind a reverse proxy that limits
request rates (and terminates TLS).

---

[← The dashboard](dashboard.md) · [Guide](README.md) · [Deployment →](deployment.md)
