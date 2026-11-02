#![allow(unused)]

#[hench::job]
async fn generic<T>(value: T) {}

#[hench::job]
async fn where_clause() where u8: Copy {}

#[hench::job]
async unsafe fn not_safe() {}

#[hench::job]
async fn borrowed(name: &str) {}

#[hench::job]
async fn nested_borrow(names: Vec<&str>) {}

#[hench::job]
async fn opaque(value: impl Clone) {}

#[hench::job]
async fn context_second(id: u64, ctx: &hench::JobContext) {}

#[hench::job]
async fn context_by_value(ctx: hench::JobContext) {}

#[hench::job]
async fn context_mut(ctx: &mut hench::JobContext) {}

#[hench::job]
async fn several_problems<T>(name: &str) {}

fn main() {}
