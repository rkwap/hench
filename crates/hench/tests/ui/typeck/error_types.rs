// Error types that do not convert into `hench::JobError`: the common ones get the macro's own
// message, which says what to use instead.

#[hench::job]
async fn with_a_string_error() -> Result<(), String> {
    Ok(())
}

#[hench::job]
async fn with_a_boxed_error() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Ok(())
}

#[hench::job]
async fn with_a_boxed_error_that_is_not_send() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

fn main() {}
