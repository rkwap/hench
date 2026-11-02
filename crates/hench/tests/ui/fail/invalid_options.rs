#[hench::job(queu = "default")]
async fn unknown_option() {}

#[hench::job(queue = "a", queue = "b")]
async fn duplicate_option() {}

#[hench::job(dead)]
async fn missing_value() {}

#[hench::job(queue = "")]
async fn empty_queue() {}

#[hench::job(queue = default)]
async fn unquoted_queue() {}

#[hench::job(retry = "yes")]
async fn retry_string() {}

#[hench::job(retry = -1)]
async fn negative_retry() {}

#[hench::job(backtrace = 1.5)]
async fn float_backtrace() {}

#[hench::job(dead = 0)]
async fn numeric_dead() {}

#[hench::job(tags = "billing")]
async fn tags_string() {}

#[hench::job(tags = ["billing", 7])]
async fn tags_number() {}

#[hench::job(retry_for = 1000000000)]
async fn retry_for_too_large() {}

#[hench::job(retry_for = "1h")]
async fn retry_for_string() {}

#[hench::job(name = "")]
async fn empty_name() {}

#[hench::job(crate = hench)]
async fn unquoted_crate() {}

fn main() {}

#[hench::job(log_level = "loud")]
async fn loud_level() {}
