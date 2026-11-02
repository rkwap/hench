//! A job's body keeps the deprecation warnings of what it uses: the code generated around it
//! allows no lint (`deprecated` here), deprecated jobs included.
#![deny(deprecated)]

mod old {
    #[deprecated(note = "use `new_api`")]
    pub fn old_api() -> u32 {
        1
    }

    #[deprecated(note = "use `NewType`")]
    pub struct OldType(pub u32);
}

#[hench::job]
async fn uses_a_deprecated_function() {
    let _ = old::old_api();
}

/// Being deprecated itself does not make the job's body allow deprecated items.
#[deprecated(note = "use `uses_a_deprecated_function`")]
#[hench::job]
async fn deprecated_job(n: u32) {
    let _ = old::OldType(n);
}

fn main() {}
