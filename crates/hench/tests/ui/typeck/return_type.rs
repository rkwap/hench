#[hench::job]
async fn returns_a_value() -> Result<String, hench::JobError> {
    Ok(String::new())
}

#[hench::job]
async fn returns_a_number() -> u32 {
    5
}

fn main() {}
