use std::rc::Rc;

#[hench::job]
async fn holds_rc_across_await(ctx: &hench::JobContext) {
    let shared = Rc::new(5);
    ctx.stopping().await;
    let _ = shared;
}

fn main() {}
