//! `log_level` sets the payload key read by the job logger.

use hench::Job;

#[hench::job(log_level = "debug", queue = "chatty")]
async fn chatty() {}

fn main() {
    let options = chatty::options();
    assert_eq!(options.custom["log_level"], "debug");
    assert_eq!(options.queue.as_deref(), Some("chatty"));
    assert_eq!(<chatty as Job>::NAME, "chatty");
}
