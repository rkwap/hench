//! docs-usability-13: the other unique-job options without `unique_for` are compile errors (they
//! made every push fail at runtime, or, for `unique_by`, nothing unique).

#[hench::job(on_conflict = "error")]
async fn told_of_duplicates() {}

#[hench::job(unique_until = "start")]
async fn released_at_start() {}

#[hench::job(unique_across_queues = true)]
async fn across_queues() {}

#[hench::job(unique_by = [user_id])]
async fn keyed(user_id: u64) {
    let _ = user_id;
}

#[hench::job(unique_by = [user_id], on_conflict = "drop")]
async fn both(user_id: u64) {
    let _ = user_id;
}

fn main() {}
