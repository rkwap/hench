#[hench::job(unique_for = 0)]
async fn zero_seconds() {}

#[hench::job(unique_for = 1000000000)]
async fn too_long() {}

#[hench::job(unique_for = "10m")]
async fn string_seconds() {}

#[hench::job(unique_until = "done")]
async fn unknown_policy() {}

#[hench::job(on_conflict = "skip")]
async fn unknown_strategy() {}

#[hench::job(unique_across_queues = 1)]
async fn numeric_flag() {}

#[hench::job(unique_by = [])]
async fn empty_key() {}

#[hench::job(unique_by = user_id)]
async fn unbracketed_key() {}

#[hench::job(unique_for = 60, unique_by = [user])]
async fn unknown_parameter(user_id: u64) {
    let _ = user_id;
}

#[hench::job(unique_for = 60, unique_by = [user_id, user_id])]
async fn listed_twice(user_id: u64) {
    let _ = user_id;
}

#[hench::job(unique_for = 60, unique_by = [ctx])]
async fn context_key(ctx: &hench::JobContext, user_id: u64) {
    let _ = (ctx, user_id);
}

#[hench::job(unique_for = 60, unique_by = [x])]
async fn no_arguments() {}

fn main() {}
