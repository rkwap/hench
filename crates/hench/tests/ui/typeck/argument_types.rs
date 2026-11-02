struct NotSerializable;

#[hench::job]
async fn takes_unserializable(value: NotSerializable) {
    let _ = value;
}

#[hench::job]
async fn wrong_error(fail: bool) -> Result<(), String> {
    if fail { Err("no".to_owned()) } else { Ok(()) }
}

fn main() {}
