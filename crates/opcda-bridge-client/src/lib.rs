pub mod cli;
pub mod commands;
pub mod config;
pub mod output;

#[cfg(test)]
mod test_support;

use std::process::ExitCode;

/// Parse CLI arguments and run, returning a process exit code.
///
/// Kept as a thin wrapper around [`run_with_cli`] so `main.rs` stays a
/// one-line delegator while the actual control flow (config loading, error
/// formatting) is exercised by tests against an already-parsed `Cli`,
/// without needing to control `std::env::args()`.
pub async fn run() -> ExitCode {
    use clap::Parser;
    run_with_cli(cli::Cli::parse()).await
}

/// Resolve config, dispatch the command, and format any error for
/// display.
///
/// CLI-only output format (`--json` / `--output` / `OPC_BRIDGE_OUTPUT`) is
/// resolved *before* loading the config file, so a config-load failure can
/// still be reported in the right format even though the config file's own
/// `output` key could never be known at that point. Once the config loads
/// successfully, the fully-resolved `CLI > env > config > default` format
/// applies to the command's own result.
async fn run_with_cli(cli: cli::Cli) -> ExitCode {
    let cli_format = output::resolve_from_cli(&cli);
    match config::load_config(cli.config.as_deref()) {
        Err(e) => fail(&e, cli_format.unwrap_or(output::OutputFormat::Table)),
        Ok(config) => {
            let format = config::resolve_output(cli_format, &config);
            match cli::run_command(cli, &config, format).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(&e, format),
            }
        }
    }
}

/// Print an error to stderr in the requested format and return a failure
/// exit code.
fn fail(err: &anyhow::Error, format: output::OutputFormat) -> ExitCode {
    eprintln!("{}", output::format_error(err, format));
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Commands};
    use crate::test_support::{MockBridgeService, start_mock_server};
    use std::path::PathBuf;

    fn base_cli(command: Commands) -> Cli {
        Cli {
            host: None,
            config: None,
            output: None,
            json: false,
            command,
        }
    }

    #[tokio::test]
    async fn test_run_with_cli_success_is_exit_success() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut cli = base_cli(Commands::Servers);
        cli.host = Some(host);
        assert_eq!(run_with_cli(cli).await, ExitCode::SUCCESS);
    }

    #[tokio::test]
    async fn test_run_with_cli_command_error_is_exit_failure() {
        // No --server and no config file `server` key: resolve_server errors
        // before any network call is made.
        let cli = base_cli(Commands::Browse {
            server: None,
            session_id: None,
            parent_node_key: None,
            page_token: None,
            page_size: None,
            all: false,
            max_results: None,
            refresh: false,
        });
        assert_eq!(run_with_cli(cli).await, ExitCode::FAILURE);
    }

    #[tokio::test]
    async fn test_run_with_cli_config_load_failure_is_exit_failure() {
        // An explicit --config path that doesn't exist is a hard error
        // (missing_is_error = true), exercising the pre-config-load branch.
        let mut cli = base_cli(Commands::Servers);
        cli.config = Some(PathBuf::from("/nonexistent/opcda-bridge-client.toml"));
        assert_eq!(run_with_cli(cli).await, ExitCode::FAILURE);
    }

    #[tokio::test]
    async fn test_run_with_cli_config_load_failure_uses_cli_only_format() {
        // Same as above but with --json set, to exercise the branch that
        // formats the config-load error using the CLI-only format rather
        // than the (unreachable, since config never loaded) full-precedence
        // format.
        let mut cli = base_cli(Commands::Servers);
        cli.config = Some(PathBuf::from("/nonexistent/opcda-bridge-client.toml"));
        cli.json = true;
        assert_eq!(run_with_cli(cli).await, ExitCode::FAILURE);
    }

    #[test]
    fn test_fail_returns_exit_failure() {
        let err = anyhow::anyhow!("boom");
        assert_eq!(fail(&err, output::OutputFormat::Table), ExitCode::FAILURE);
        assert_eq!(fail(&err, output::OutputFormat::Json), ExitCode::FAILURE);
    }
}
