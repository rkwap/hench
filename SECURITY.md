# Security policy

## Supported versions

Security fixes are released for the latest minor version of Hench (currently 0.1.x). Hench
follows [Semantic Versioning](https://semver.org); before 1.0, upgrading to the next minor
version may be needed to receive a fix.

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Report it privately, in either
of these ways:

- on GitHub: the repository's **Security** tab, then **Report a vulnerability**;
- by email to **rkwap2011@gmail.com**.

Include what you found, the versions affected, and the steps or the program that show it. You
will get an acknowledgement within a week, and updates until the issue is fixed. Once a fix is
released, the advisory credits you, unless you would rather stay anonymous.

## Scope

Reports are especially welcome about:

- the dashboard and its JSON API (`hench-web`, and `hench web` from `hench-server`):
  authentication, CSRF, cross-site scripting, host checks, and anything that lets a visitor
  read or change jobs they should not;
- the `hench` command line;
- how jobs and their arguments are stored in and read back from Redis.

A dashboard exposed to the internet without authentication is a configuration problem, not a
vulnerability; [the dashboard guide](docs/guide/dashboard.md) explains how to protect it.
