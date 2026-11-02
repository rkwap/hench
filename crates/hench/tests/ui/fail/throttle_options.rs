#[hench::job(throttle = "")]
async fn empty_name() {}

#[hench::job(throttle = 5)]
async fn numeric_name() {}

#[hench::job(throttle(rate = 3))]
async fn unknown_limit() {}

#[hench::job(throttle(window = "5"))]
async fn period_missing() {}

#[hench::job(throttle(concurrency = 0))]
async fn zero_limit() {}

#[hench::job(throttle(window = "5/0s"))]
async fn zero_period() {}

#[hench::job(throttle())]
async fn no_limit() {}

#[hench::job(throttle_by = [])]
async fn empty_key() {}

#[hench::job(throttle_by = account)]
async fn unbracketed_key() {}

#[hench::job(throttle_by = [account])]
async fn key_without_throttle(account: i64) {
    let _ = account;
}

#[hench::job(throttle = "stripe", throttle_by = [acount])]
async fn unknown_parameter(account: i64) {
    let _ = account;
}

#[hench::job(throttle = "stripe", throttle_by = [ctx])]
async fn context_key(ctx: &hench::JobContext, account: i64) {
    let _ = (ctx, account);
}

#[hench::job(throttle(concurrency_per_key = 1))]
async fn per_key_without_key(account: i64) {
    let _ = account;
}

fn main() {}
