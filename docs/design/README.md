# Design documents: periodic jobs, unique jobs, batches, rate limiting

These documents specify the four features that Sidekiq sells in Pro/Enterprise, designed for hench
after studying production implementations: Sidekiq Pro/Enterprise's public documentation (behaviour
only), BullMQ (MIT), sidekiq-cron (MIT), sidekiq-unique-jobs (MIT), sidekiq-throttled (MIT),
sidekiq-batch (MIT), Oban (Apache-2.0) and rusty-sidekiq (MIT).

| Document | Contents |
|---|---|
| [pro-features.md](pro-features.md) | Cross-feature conventions, the push and execution pipelines, interplay, the implementation plan (phase 0 shared infrastructure, phase 1 lanes A and B, phase 2 integration, phase 3 follow-ups) and the consolidated decisions. Start here. |
| [unique.md](unique.md) | Unique jobs (deduplication locks) |
| [periodic.md](periodic.md) | Periodic (cron) jobs |
| [batches.md](batches.md) | Batches, callbacks and workflows |
| [ratelimit.md](ratelimit.md) | Limiters, throttles and queue limits |

**Decisions.** Every decision these documents raise is settled by its recommendation (the owner's
instruction of 2026-11-01: take the recommended production-ready option). pro-features.md §9
lists them. Once a feature lands, its condensed contract lives in docs/DESIGN.md §9g–§9k, which is
binding; these documents keep the full rationale, the survey of the references and the test plans.
