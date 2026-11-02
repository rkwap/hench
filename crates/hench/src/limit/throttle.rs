//! Throttles: the registry of a process, their identities in Redis and their checks
//! (`docs/design/ratelimit.md` §2.4.3, §2.6).
//!
//! A throttle's limits become limit identities named after it: a global limit is `<name>`, a
//! per-key limit `<name>:<key>`; a second limit of the same kind and scope gets `~2`
//! (`<name>~2`, `<name>~2:<key>`), a third `~3`. Their per-minute counters are kept under the
//! throttle's name.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::job::Registry;
use crate::limit::rule::{self, Rule};
use crate::limit::{LimitKind, Scope, Throttle, ThrottleOption};

impl Throttle {
    /// The rules of a job with throttle key `key`: the global limits, and the per-key ones when
    /// the job has a key (a job without one gets the global limits only).
    pub(crate) fn rules(&self, key: Option<&str>) -> Result<Vec<Rule>> {
        self.validate()?;
        let key = key.map(rule::stored_key);
        let mut seen: HashMap<(char, Scope), u32> = HashMap::new();
        let mut rules = Vec::with_capacity(self.limits().len());
        for (limit, scope) in self.limits() {
            let count = seen.entry((limit.kind().letter(), *scope)).or_insert(0);
            *count += 1;
            let name = if *count == 1 {
                self.name().to_owned()
            } else {
                format!("{}~{count}", self.name())
            };
            match (scope, &key) {
                (Scope::Global, _) => {
                    rules.push(Rule::new(*limit, name, self.name().to_owned()));
                }
                (Scope::PerKey, Some(key)) => {
                    rules.push(Rule::new(
                        *limit,
                        rule::id(&name, Some(key)),
                        self.name().to_owned(),
                    ));
                }
                (Scope::PerKey, None) => {}
            }
        }
        Ok(rules)
    }

    /// Checks the definition: its name, at least one valid limit, its lease, the expiry of its
    /// counters and its longest delay.
    pub fn validate(&self) -> Result<()> {
        let name = self.name().escape_debug();
        let invalid =
            |reason: String| Error::Config(format!("invalid throttle `{name}`: {reason}"));
        let because = |error: Error| match error {
            Error::Config(reason) => invalid(reason),
            other => invalid(other.to_string()),
        };
        rule::check_name_of("throttle", self.name())?;
        if self.limits().is_empty() {
            return Err(invalid(
                "it has no limit (add one, such as `.concurrency(5)`)".to_owned(),
            ));
        }
        for (limit, _) in self.limits() {
            rule::check_limit(limit).map_err(because)?;
        }
        rule::check_lease(self.lease_duration()).map_err(because)?;
        rule::check_stats_ttl(self.stats_ttl_duration()).map_err(because)?;
        if self.max_delay_duration() < Duration::from_secs(1) {
            return Err(invalid("its max_delay must be at least 1 s".to_owned()));
        }
        Ok(())
    }

    /// Checks that workers renewing the throttle's permits every `heartbeat` keep them alive:
    /// its lease must last [`RENEWALS_PER_LEASE`] heartbeats (concurrency limits only; rate
    /// limits hold no lease).
    pub(crate) fn check_renewals(&self, heartbeat: Duration) -> Result<()> {
        let leased = self
            .limits()
            .iter()
            .any(|(limit, _)| limit.kind() == LimitKind::Concurrency);
        let needed = heartbeat.saturating_mul(RENEWALS_PER_LEASE);
        if !leased || self.lease_duration() >= needed {
            return Ok(());
        }
        Err(Error::Config(format!(
            "invalid throttle `{}`: its lease ({:?}) is shorter than {RENEWALS_PER_LEASE} \
             heartbeat intervals ({needed:?}): workers renew the permits of running jobs once \
             per heartbeat, so a lease that short ends while jobs still run and lets more of them \
             in; raise the lease (it is how long a crashed worker's slots stay taken, not the \
             expected job duration) or lower `heartbeat_interval`",
            self.name(),
            self.lease_duration(),
        )))
    }
}

/// Heartbeats a throttle's lease must last (the heartbeat renews engine-held permits; one beat
/// may be late, another fail).
pub(crate) const RENEWALS_PER_LEASE: u32 = 3;

/// The throttles of a process, by name (the jobs' own, then the builder's, then the
/// configuration file's), resolved by the throttle gate.
#[derive(Debug, Clone, Default)]
pub(crate) struct Throttles {
    by_name: HashMap<String, Throttle>,
}

impl Throttles {
    /// The registry of `throttles`: a later definition of a name replaces an earlier one.
    pub(crate) fn new(throttles: impl IntoIterator<Item = Throttle>) -> Self {
        let mut by_name: HashMap<String, Throttle> = HashMap::new();
        for throttle in throttles {
            if let Some(previous) = by_name.insert(throttle.name().to_owned(), throttle)
                && let Some(current) = by_name.get(previous.name())
                && *current != previous
            {
                tracing::info!(
                    throttle = %current.name(),
                    replaced = %previous.spec(),
                    by = %current.spec(),
                    "a later definition of a throttle replaces an earlier one"
                );
            }
        }
        Self { by_name }
    }

    /// The throttle named `name`.
    pub(crate) fn get(&self, name: &str) -> Option<&Throttle> {
        self.by_name.get(name)
    }

    /// `true` when no throttle is defined.
    pub(crate) fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// The number of throttles.
    #[allow(dead_code, reason = "used by the tests and the start-up log")]
    pub(crate) fn len(&self) -> usize {
        self.by_name.len()
    }

    /// The definitions, for the heartbeat (`{"stripe": "c=25;w=5/1000 per key"}`).
    pub(crate) fn specs(&self) -> serde_json::Map<String, Value> {
        let mut names: Vec<&String> = self.by_name.keys().collect();
        names.sort();
        names
            .into_iter()
            .filter_map(|name| {
                let throttle = self.by_name.get(name)?;
                Some((name.clone(), Value::String(throttle.spec())))
            })
            .collect()
    }
}

/// Checks every throttle a process defines (`Error::Config` naming the first invalid one): the
/// check `HenchBuilder::build` runs on the throttles of the configuration, whose heartbeat
/// renews their permits every `heartbeat`.
pub(crate) fn validate_throttles<'a>(
    throttles: impl IntoIterator<Item = &'a Throttle>,
    heartbeat: Duration,
) -> Result<()> {
    for throttle in throttles {
        throttle.validate()?;
        throttle.check_renewals(heartbeat)?;
    }
    Ok(())
}

/// Checks that every registered job whose options name a throttle finds it among the process's
/// throttles (`config`'s and the jobs' own): `Error::Config` naming the job and the throttle.
pub(crate) fn check_registry(config: &Config, registry: &Registry) -> Result<()> {
    let throttles = Throttles::new(
        registry
            .names()
            .iter()
            .filter_map(|name| registry.get(name))
            .filter_map(|job| job.dyn_throttle())
            .chain(config.throttles.iter().cloned()),
    );
    validate_throttles(throttles.by_name.values(), config.heartbeat_interval)?;
    for name in registry.names() {
        let Some(job) = registry.get(name) else {
            continue;
        };
        if let Some(ThrottleOption::Enabled {
            name: Some(throttle),
            ..
        }) = &job.job_options().throttle
            && throttles.get(throttle).is_none()
        {
            return Err(Error::Config(format!(
                "job `{name}` names the throttle `{throttle}`, which is not defined: define it \
                 with `HenchBuilder::throttle(Throttle::new(\"{throttle}\")..)`, with the job \
                 (`Job::throttle`), or in the configuration file (`[throttles.{throttle}]`)"
            )));
        }
    }
    Ok(())
}

/// Warnings logged at most once per subject per minute.
static WARNED: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// How often a warning about one subject is logged.
const WARN_EVERY: Duration = Duration::from_secs(60);

/// Whether to log a warning about `subject` now (at most once per minute per subject).
pub(crate) fn warn_now(subject: &str) -> bool {
    let mut warned = WARNED.lock().unwrap_or_else(PoisonError::into_inner);
    let now = Instant::now();
    if warned.len() > 1_024 {
        warned.retain(|_, at| now.duration_since(*at) < WARN_EVERY);
    }
    match warned.get(subject) {
        Some(at) if now.duration_since(*at) < WARN_EVERY => false,
        _ => {
            warned.insert(subject.to_owned(), now);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limit::{Interval, Limit};
    use serde_json::json;

    #[test]
    fn throttles_build_and_advertise_their_specs() {
        let throttle = Throttle::new("stripe")
            .concurrency(25)
            .window_per_key(5, Duration::from_secs(1))
            .reschedules(Some(200))
            .fail_open(true);
        assert_eq!(throttle.spec(), "c=25;w=5/1000 per key");
        let registry = Throttles::new([Throttle::new("a").concurrency(1), throttle.clone()]);
        assert_eq!(registry.get("stripe"), Some(&throttle));
        assert_eq!(registry.specs()["a"], json!("c=1"));
        assert_eq!(registry.len(), 2);
        // A later definition replaces an earlier one.
        let later = Throttles::new([
            Throttle::new("a").concurrency(1),
            Throttle::new("a").concurrency(2),
        ]);
        assert_eq!(later.specs()["a"], json!("c=2"));
    }

    #[test]
    fn rules_name_global_and_per_key_limits() {
        let throttle = Throttle::new("stripe")
            .concurrency(25)
            .window_per_key(5, Duration::from_secs(1))
            .window(100, Duration::from_secs(1))
            .limit_per_key(Limit::Concurrency { limit: 1 })
            .leaky(10, Duration::from_secs(10))
            .limit(Limit::Points {
                capacity: 100,
                refill: Duration::from_secs(10),
            })
            .bucket_per_key(1_000, Interval::Day);
        let members: Vec<String> = throttle
            .rules(Some("42"))
            .unwrap()
            .into_iter()
            .map(|rule| rule.member)
            .collect();
        assert_eq!(
            members,
            [
                "c:stripe",
                "w:stripe:42",
                "w:stripe",
                "c:stripe:42",
                "g:stripe",
                "g:stripe~2",
                "b:stripe:42"
            ]
        );
        // Without a key, the global limits only.
        let members: Vec<String> = throttle
            .rules(None)
            .unwrap()
            .into_iter()
            .map(|rule| rule.member)
            .collect();
        assert_eq!(members, ["c:stripe", "w:stripe", "g:stripe", "g:stripe~2"]);
        // Every rule counts under the throttle's name.
        assert!(
            throttle
                .rules(Some("42"))
                .unwrap()
                .iter()
                .all(|rule| rule.counter == "stripe")
        );
        // A second window per key gets ~2 too; long keys are hashed.
        let doubled = Throttle::new("t")
            .window_per_key(1, Duration::from_secs(1))
            .window_per_key(10, Duration::from_secs(60));
        let long = "k".repeat(200);
        let rules = doubled.rules(Some(&long)).unwrap();
        assert!(rules[0].member.starts_with("w:t:#"));
        assert!(rules[1].member.starts_with("w:t~2:#"));
        // So are empty keys and keys with NUL (they come from data: never refused).
        let empty = doubled.rules(Some("")).unwrap();
        assert!(empty[0].member.starts_with("w:t:#"), "{}", empty[0].member);
    }

    #[test]
    fn definitions_are_checked() {
        assert!(Throttle::new("ok").concurrency(1).validate().is_ok());
        for (throttle, reason) in [
            (Throttle::new("no-limit"), "no limit"),
            (
                Throttle::new("bad name").concurrency(1),
                "invalid throttle name `bad name`",
            ),
            (Throttle::new("zero").concurrency(0), "at least 1"),
            (
                Throttle::new("lease")
                    .concurrency(1)
                    .lease(Duration::from_millis(10)),
                "lease",
            ),
            (
                Throttle::new("delay")
                    .concurrency(1)
                    .max_delay(Duration::ZERO),
                "max_delay",
            ),
        ] {
            let error = throttle.validate().unwrap_err().to_string();
            assert!(error.contains(reason), "{error}");
            assert!(throttle.rules(None).is_err());
        }
        let heartbeat = Duration::from_secs(10);
        assert!(validate_throttles(&[Throttle::new("x").concurrency(1)], heartbeat).is_ok());
    }

    /// A job naming the throttle `stripe`, defined elsewhere.
    struct NamesStripe;

    impl crate::Job for NamesStripe {
        const NAME: &'static str = "throttle-tests::NamesStripe";
        type Args = ();

        fn options() -> crate::JobOptions {
            crate::JobOptions::new().throttle("stripe")
        }

        async fn perform(
            &self,
            _ctx: &crate::JobContext,
            (): Self::Args,
        ) -> std::result::Result<(), crate::JobError> {
            Ok(())
        }
    }

    /// A job defining the throttle it names.
    struct DefinesItsOwn;

    impl crate::Job for DefinesItsOwn {
        const NAME: &'static str = "throttle-tests::DefinesItsOwn";
        type Args = ();

        fn options() -> crate::JobOptions {
            crate::JobOptions::new().throttle("own")
        }

        fn throttle() -> Option<Throttle> {
            Some(Throttle::new("own").concurrency(2))
        }

        async fn perform(
            &self,
            _ctx: &crate::JobContext,
            (): Self::Args,
        ) -> std::result::Result<(), crate::JobError> {
            Ok(())
        }
    }

    #[test]
    fn registered_jobs_must_find_their_throttles() {
        let mut registry = Registry::new();
        registry.register(NamesStripe).unwrap();
        registry.register(DefinesItsOwn).unwrap();
        let mut config = Config::default();
        // `stripe` is defined nowhere: the job naming it is refused, by name.
        let error = check_registry(&config, &registry).unwrap_err().to_string();
        assert!(
            error.contains("throttle-tests::NamesStripe") && error.contains("`stripe`"),
            "{error}"
        );
        // Defined on the builder: accepted (the other job's throttle comes with that job).
        config
            .throttles
            .push(Throttle::new("stripe").concurrency(5));
        check_registry(&config, &registry).unwrap();
        // An invalid definition is refused, whoever defines it (here the builder's replaces the
        // job's own).
        config.throttles.push(Throttle::new("own"));
        let error = check_registry(&config, &registry).unwrap_err().to_string();
        assert!(error.contains("invalid throttle `own`"), "{error}");
    }

    #[test]
    fn warnings_are_rate_limited_per_subject() {
        assert!(warn_now("throttle-test-subject"));
        assert!(!warn_now("throttle-test-subject"));
        assert!(warn_now("throttle-test-other"));
    }
}
