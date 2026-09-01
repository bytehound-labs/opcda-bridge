pub mod browse;
pub mod config;
pub mod controller;
pub mod index;
pub mod logging;
pub mod opc;
pub mod run;
pub mod server;
pub mod service;

#[cfg(test)]
mod test_support;

#[cfg(target_os = "windows")]
pub mod opc_da_adapter;

#[cfg(not(target_os = "windows"))]
pub fn non_windows_run(cli: config::Cli) -> u8 {
    use config::ServiceCommand;

    if matches!(cli.command.as_ref(), Some(ServiceCommand::IndexPrepare)) {
        return match run::prepare_index(&cli) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("index preparation failed: {error:#}");
                1
            }
        };
    }
    eprintln!("opcda-bridge gateway requires Windows (COM/DCOM dependency)");
    1
}

#[cfg(all(test, not(target_os = "windows")))]
mod tests {
    use super::non_windows_run;
    use crate::config::Cli;
    use clap::Parser;
    use std::ffi::OsString;
    use std::fs;
    use tempfile::tempdir;

    fn index_prepare_cli(config: &std::path::Path) -> Cli {
        Cli::try_parse_from([
            OsString::from("opcda-bridge-gateway"),
            OsString::from("--config"),
            config.as_os_str().to_os_string(),
            OsString::from("index-prepare"),
        ])
        .unwrap()
    }

    #[test]
    fn non_windows_index_prepare_dispatches_successfully() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("index.sqlite3");
        let config = directory.path().join("gateway.toml");
        fs::write(
            &config,
            format!(
                "[index]\ndatabase_path = {:?}\n",
                database.to_string_lossy()
            ),
        )
        .unwrap();

        assert_eq!(non_windows_run(index_prepare_cli(&config)), 0);
    }

    #[test]
    fn non_windows_index_prepare_reports_configuration_errors() {
        let directory = tempdir().unwrap();
        let missing_config = directory.path().join("missing.toml");

        assert_eq!(non_windows_run(index_prepare_cli(&missing_config)), 1);
    }

    #[test]
    fn non_windows_gateway_mode_reports_windows_requirement() {
        let cli = Cli::try_parse_from([OsString::from("opcda-bridge-gateway")]).unwrap();
        assert_eq!(non_windows_run(cli), 1);
    }
}
