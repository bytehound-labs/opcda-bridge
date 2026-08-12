use clap::Parser;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Windows service management subcommands. Give flags such as `--port` or
/// `--log-dir` *before* the subcommand (e.g. `--log-dir C:\logs install`) —
/// they are only meaningful on `install`, where they're baked into the
/// registered service's launch arguments so it starts with the same
/// configuration every time the SCM launches it.
///
/// Running the executable with no subcommand at all (as the installed
/// service does) auto-detects whether it was launched by the Service
/// Control Manager or interactively, and behaves accordingly — see
/// `service::run_as_service`.
#[derive(clap::Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum ServiceCommand {
    /// Register the gateway as a Windows service (does not start it)
    Install,
    /// Remove the registered Windows service
    Uninstall,
    /// Start the registered Windows service
    Start,
    /// Stop the running Windows service
    Stop,
    /// Report the Windows service's current status
    Status,
}

/// Command-line interface for the gateway.
#[derive(Parser, Debug)]
#[command(
    name = "opcda-bridge-gateway",
    about = "OPC DA bridge gateway",
    version
)]
pub struct Cli {
    /// Windows service management command (install/uninstall/start/stop/status).
    /// Omit entirely to run the gateway itself, whether interactively or as
    /// an already-installed service.
    #[command(subcommand)]
    pub command: Option<ServiceCommand>,

    /// Path to a TOML config file (default: opcda-bridge-gateway.toml next to the executable)
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Port to listen on (default: 7600)
    #[arg(long, env = "OPC_BRIDGE_PORT")]
    pub port: Option<u16>,

    /// Log level / directive spec, e.g. "info" or "opcda_bridge_gateway=debug,tower=warn" (default: info)
    #[arg(long, env = "RUST_LOG")]
    pub log_level: Option<String>,

    /// Directory to write log files to (default: a "logs" directory next to the executable)
    #[arg(long, value_name = "PATH")]
    pub log_dir: Option<PathBuf>,

    /// Log output format: "pretty" or "json" (default: pretty)
    #[arg(long)]
    pub log_format: Option<String>,

    /// Log file rotation: "hourly", "daily", or "never" (default: daily)
    #[arg(long)]
    pub log_rotation: Option<String>,
}

/// Gateway configuration loaded from an optional TOML file. Every field is
/// optional; a value missing from the file (or the file itself missing)
/// falls back to the env var / CLI flag / built-in default resolution.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct GatewayConfig {
    pub port: Option<u16>,
    #[serde(default)]
    pub log: LogConfig,
}

/// Logging configuration keys, consumed once file-based logging lands.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct LogConfig {
    pub level: Option<String>,
    pub dir: Option<String>,
    pub format: Option<String>,
    pub rotation: Option<String>,
}

/// Derive the gateway's default config path from its own executable path:
/// `opcda-bridge-gateway.toml` next to the `.exe`.
pub fn config_path_from_exe(exe_path: &Path) -> PathBuf {
    exe_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opcda-bridge-gateway.toml")
}

/// Load a gateway config from `path`.
///
/// A missing file resolves to `Ok(GatewayConfig::default())` when
/// `missing_is_error` is false (the auto-discovered path may legitimately
/// not exist yet); with an explicit `--config` path a missing file is a
/// hard error instead. A file that exists but fails to parse as TOML is
/// always a hard error — a config typo should never be silently ignored.
pub fn load_config_file(path: &Path, missing_is_error: bool) -> anyhow::Result<GatewayConfig> {
    match std::fs::read_to_string(path) {
        Ok(contents) => toml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !missing_is_error => {
            Ok(GatewayConfig::default())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(anyhow::anyhow!("config file not found: {}", path.display()))
        }
        Err(e) => Err(anyhow::anyhow!(
            "failed to read config file {}: {e}",
            path.display()
        )),
    }
}

/// Resolve and load the gateway config: an explicit `--config` path if
/// given, otherwise the path auto-discovered next to the running
/// executable.
pub fn load_config(explicit_path: Option<&Path>) -> anyhow::Result<GatewayConfig> {
    match explicit_path {
        Some(path) => load_config_file(path, true),
        None => {
            let exe = std::env::current_exe().expect("failed to resolve current executable path");
            load_config_file(&config_path_from_exe(&exe), false)
        }
    }
}

/// Resolve the listen port with `CLI flag > env var > config file >
/// default` precedence. The env var is already folded into `cli_port` by
/// clap's `env` attribute on `Cli::port`.
pub fn resolve_port(cli_port: Option<u16>, config: &GatewayConfig) -> u16 {
    cli_port
        .or(config.port)
        .unwrap_or(opcda_bridge_proto::DEFAULT_BRIDGE_PORT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_config_path_from_exe_with_parent() {
        let path = config_path_from_exe(Path::new("/usr/local/bin/opcda-bridge-gateway"));
        assert_eq!(
            path,
            PathBuf::from("/usr/local/bin/opcda-bridge-gateway.toml")
        );
    }

    #[test]
    fn test_config_path_from_exe_no_parent() {
        // `Path::new("/").parent()` is `None` (root has no parent), unlike a
        // bare relative filename whose parent is `Some("")` — this is the
        // input that actually exercises the `unwrap_or_else` fallback.
        let path = config_path_from_exe(Path::new("/"));
        assert_eq!(path, PathBuf::from("./opcda-bridge-gateway.toml"));
    }

    #[test]
    fn test_load_config_file_valid() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "port = 8080\n[log]\nlevel = \"debug\"").unwrap();
        let config = load_config_file(file.path(), true).unwrap();
        assert_eq!(config.port, Some(8080));
        assert_eq!(config.log.level, Some("debug".to_string()));
    }

    #[test]
    fn test_load_config_file_empty_is_all_defaults() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let config = load_config_file(file.path(), true).unwrap();
        assert_eq!(config, GatewayConfig::default());
    }

    #[test]
    fn test_load_config_file_malformed() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "port = \"not a number\"").unwrap();
        let err = load_config_file(file.path(), true).unwrap_err();
        assert!(err.to_string().contains("failed to parse config file"));
    }

    #[test]
    fn test_load_config_file_missing_not_error() {
        let config =
            load_config_file(Path::new("/nonexistent/opcda-bridge-gateway.toml"), false).unwrap();
        assert_eq!(config, GatewayConfig::default());
    }

    #[test]
    fn test_load_config_file_missing_is_error() {
        let err = load_config_file(Path::new("/nonexistent/opcda-bridge-gateway.toml"), true)
            .unwrap_err();
        assert!(err.to_string().contains("config file not found"));
    }

    #[test]
    fn test_load_config_file_generic_io_error() {
        // Reading a directory as a file fails with an `IsADirectory`-style
        // error, distinct from `NotFound` — exercises the catch-all I/O
        // error branch (e.g. permission denied in real usage).
        let dir = tempfile::tempdir().unwrap();
        let err = load_config_file(dir.path(), true).unwrap_err();
        assert!(err.to_string().contains("failed to read config file"));
    }

    #[test]
    fn test_load_config_explicit_path() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "port = 9000").unwrap();
        let config = load_config(Some(file.path())).unwrap();
        assert_eq!(config.port, Some(9000));
    }

    #[test]
    fn test_load_config_explicit_path_missing_errors() {
        let err = load_config(Some(Path::new("/nonexistent/gateway.toml"))).unwrap_err();
        assert!(err.to_string().contains("config file not found"));
    }

    #[test]
    fn test_load_config_default_discovery() {
        // No file will exist next to the test binary's own executable path,
        // so this exercises the "missing, not an error" auto-discovery path.
        let config = load_config(None).unwrap();
        assert_eq!(config, GatewayConfig::default());
    }

    #[test]
    fn test_resolve_port_cli_wins() {
        let config = GatewayConfig {
            port: Some(1111),
            log: LogConfig::default(),
        };
        assert_eq!(resolve_port(Some(2222), &config), 2222);
    }

    #[test]
    fn test_resolve_port_config_wins_over_default() {
        let config = GatewayConfig {
            port: Some(1111),
            log: LogConfig::default(),
        };
        assert_eq!(resolve_port(None, &config), 1111);
    }

    #[test]
    fn test_resolve_port_default() {
        assert_eq!(
            resolve_port(None, &GatewayConfig::default()),
            opcda_bridge_proto::DEFAULT_BRIDGE_PORT
        );
    }

    #[test]
    fn test_cli_parses_config_flag() {
        let cli = Cli::try_parse_from(["opcda-bridge-gateway", "--config", "custom.toml"]).unwrap();
        assert_eq!(cli.config, Some(PathBuf::from("custom.toml")));
        assert_eq!(cli.port, None);
    }

    #[test]
    fn test_cli_parses_port_flag() {
        let cli = Cli::try_parse_from(["opcda-bridge-gateway", "--port", "9090"]).unwrap();
        assert_eq!(cli.port, Some(9090));
    }

    #[test]
    fn test_cli_defaults_to_none() {
        let cli = Cli::try_parse_from(["opcda-bridge-gateway"]).unwrap();
        assert_eq!(cli.command, None);
        assert_eq!(cli.config, None);
        assert_eq!(cli.port, None);
        assert_eq!(cli.log_level, None);
        assert_eq!(cli.log_dir, None);
        assert_eq!(cli.log_format, None);
        assert_eq!(cli.log_rotation, None);
    }

    #[test]
    fn test_cli_parses_service_subcommands() {
        for (arg, expected) in [
            ("install", ServiceCommand::Install),
            ("uninstall", ServiceCommand::Uninstall),
            ("start", ServiceCommand::Start),
            ("stop", ServiceCommand::Stop),
            ("status", ServiceCommand::Status),
        ] {
            let cli = Cli::try_parse_from(["opcda-bridge-gateway", arg]).unwrap();
            assert_eq!(cli.command, Some(expected));
        }
    }

    #[test]
    fn test_cli_flags_before_subcommand_apply_to_install() {
        // Flags must precede the subcommand: they configure what `install`
        // bakes into the service's launch arguments, not the subcommand
        // itself (which takes no flags of its own).
        let cli = Cli::try_parse_from([
            "opcda-bridge-gateway",
            "--port",
            "7700",
            "--log-dir",
            "/var/log/opcda",
            "install",
        ])
        .unwrap();
        assert_eq!(cli.command, Some(ServiceCommand::Install));
        assert_eq!(cli.port, Some(7700));
        assert_eq!(cli.log_dir, Some(PathBuf::from("/var/log/opcda")));
    }

    #[test]
    fn test_cli_rejects_unknown_subcommand() {
        let err = Cli::try_parse_from(["opcda-bridge-gateway", "bogus"])
            .err()
            .unwrap();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::InvalidSubcommand,
            "unrecognized \"bogus\" positional argument should fail to parse as a subcommand, not be silently accepted"
        );
    }

    #[test]
    fn test_cli_parses_log_flags() {
        let cli = Cli::try_parse_from([
            "opcda-bridge-gateway",
            "--log-level",
            "debug",
            "--log-dir",
            "/tmp/logs",
            "--log-format",
            "json",
            "--log-rotation",
            "hourly",
        ])
        .unwrap();
        assert_eq!(cli.log_level, Some("debug".to_string()));
        assert_eq!(cli.log_dir, Some(PathBuf::from("/tmp/logs")));
        assert_eq!(cli.log_format, Some("json".to_string()));
        assert_eq!(cli.log_rotation, Some("hourly".to_string()));
    }

    #[test]
    fn test_cli_version_flag() {
        let err = Cli::try_parse_from(["opcda-bridge-gateway", "--version"])
            .err()
            .unwrap();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
    }
}
