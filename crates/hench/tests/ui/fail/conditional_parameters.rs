#![allow(unused)]

#[hench::job]
async fn conditional(#[cfg(unix)] extra: u8, id: u64) {}

fn main() {}
