//! The `hench` command; see the `hench_server` library for what it does.

use std::process::ExitCode;

use clap::Parser;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = hench_server::Cli::parse();
    hench_server::init_tracing(&cli);
    let mut stdout = std::io::stdout();
    match hench_server::run(cli, &mut stdout).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("hench: {error:#}");
            ExitCode::FAILURE
        }
    }
}
