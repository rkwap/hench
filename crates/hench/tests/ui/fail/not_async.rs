#![allow(unused)]

#[hench::job]
fn not_async(id: u64) {
    let _ = id;
}

fn main() {}
