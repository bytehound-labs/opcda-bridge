use crate::output::OutputFormat;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Default gateway host:port the client connects to when nothing else specifies one.
pub const DEFAULT_HOST: &str = "localhost:7600";
/// Default cap on the number of tags a `browse` streams back.
pub const DEFAULT_MAX_TAGS: u32 = 1000;

/// Client configuration loaded from an optional TOML file. Every field is
/// optional; a value missing from the file (or the file itself missing)
/// falls back to the env var / CLI flag / built-in default resolution.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct ClientConfig {
    pub host: Option<String>,
    pub server: Option<String>,
    pub max_tags: Option<u32>,
    pub output: Option<OutputFormat>,
}

/// Resolve the client's default config path from raw environment values
/// rather than reading `std::env` directly — keeps discovery fully
/// unit-testable across every permutation without mutating real process
/// environment variables.
///
/// - Windows (`is_windows = true`): `%APPDATA%\opcda-bridge\client.toml`.
/// - Elsewhere: `$XDG_CONFIG_HOME/opcda-bridge/client.toml`, falling back
///   to `$HOME/.config/opcda-bridge/client.toml`.
pub fn config_path_from(
    xdg_config_home: Option<&str>,
    home: Option<&str>,
    appdata: Option<&str>,
    is_windows: bool,
) -> Option<PathBuf> {
    if is_windows {
        return appdata.map(|dir| Path::new(dir).join("opcda-bridge").join("client.toml"));
    }
    if let Some(dir) = xdg_config_home {
        return Some(Path::new(dir).join("opcda-bridge").join("client.toml"));
    }
    home.map(|dir| {
        Path::new(dir)
            .join(".config")
            .join("opcda-bridge")
            .join("client.toml")
    })
}

/// Load a client config from `path`.
///
/// A missing file resolves to `Ok(ClientConfig::default())` when
/// `missing_is_error` is false (the auto-discovered path may legitimately
/// not exist yet); with an explicit `--config` path a missing file is a
/// hard error instead. A file that exists but fails to parse as TOML is
/// always a hard error — a config typo should never be silently ignored.
pub fn load_config_file(path: &Path, missing_is_error: bool) -> anyhow::Result<ClientConfig> {
    match std::fs::read_to_string(path) {
        Ok(contents) => toml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !missing_is_error => {
            Ok(ClientConfig::default())
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

/// Resolve and load the client config: an explicit `--config` path if
/// given, otherwise the platform's auto-discovered path (silently falls
/// back to defaults if none of the relevant environment variables are
/// set, or if the discovered file doesn't exist).
pub fn load_config(explicit_path: Option<&Path>) -> anyhow::Result<ClientConfig> {
    match explicit_path {
        Some(path) => load_config_file(path, true),
        None => {
            let path = config_path_from(
                std::env::var("XDG_CONFIG_HOME").ok().as_deref(),
                std::env::var("HOME").ok().as_deref(),
                std::env::var("APPDATA").ok().as_deref(),
                cfg!(target_os = "windows"),
            );
            match path {
                Some(p) => load_config_file(&p, false),
                None => Ok(ClientConfig::default()),
            }
        }
    }
}

/// Resolve the gateway host with `CLI flag > env var > config file >
/// default` precedence. The env var is already folded into `cli_host` by
/// clap's `env` attribute on `Cli::host`.
pub fn resolve_host(cli_host: Option<String>, config: &ClientConfig) -> String {
    cli_host
        .or_else(|| config.host.clone())
        .unwrap_or_else(|| DEFAULT_HOST.to_string())
}

/// Resolve the OPC DA server ProgID with `CLI flag > config file`
/// precedence, erroring if neither is set (there's no sensible default).
pub fn resolve_server(cli_server: Option<String>, config: &ClientConfig) -> anyhow::Result<String> {
    cli_server.or_else(|| config.server.clone()).ok_or_else(|| {
        anyhow::anyhow!("no OPC server specified: pass --server or set `server` in the config file")
    })
}

/// Resolve the browse tag cap with `CLI flag > config file > default` precedence.
pub fn resolve_max_tags(cli_max_tags: Option<u32>, config: &ClientConfig) -> u32 {
    cli_max_tags.or(config.max_tags).unwrap_or(DEFAULT_MAX_TAGS)
}

/// Resolve the output format with `CLI flag/env > config file > default`
/// precedence. `cli_output` is already the CLI-only resolution (`--json`
/// wins over `--output`, which itself already folds in `OPC_BRIDGE_OUTPUT`
/// via clap's `env` attribute — see `output::resolve_from_cli`).
pub fn resolve_output(cli_output: Option<OutputFormat>, config: &ClientConfig) -> OutputFormat {
    cli_output.or(config.output).unwrap_or(OutputFormat::Table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_config_path_from_windows_with_appdata() {
        let path = config_path_from(None, None, Some(r"C:\Users\me\AppData\Roaming"), true);
        assert_eq!(
            path,
            Some(PathBuf::from(
                r"C:\Users\me\AppData\Roaming/opcda-bridge/client.toml"
            ))
        );
    }

    #[test]
    fn test_config_path_from_windows_no_appdata() {
        assert_eq!(
            config_path_from(Some("/xdg"), Some("/home"), None, true),
            None
        );
    }

    #[test]
    fn test_config_path_from_unix_xdg_config_home() {
        let path = config_path_from(Some("/xdg"), Some("/home/me"), None, false);
        assert_eq!(path, Some(PathBuf::from("/xdg/opcda-bridge/client.toml")));
    }

    #[test]
    fn test_config_path_from_unix_falls_back_to_home() {
        let path = config_path_from(None, Some("/home/me"), None, false);
        assert_eq!(
            path,
            Some(PathBuf::from("/home/me/.config/opcda-bridge/client.toml"))
        );
    }

    #[test]
    fn test_config_path_from_unix_no_env_vars() {
        assert_eq!(config_path_from(None, None, None, false), None);
    }

    #[test]
    fn test_config_path_from_unix_xdg_takes_precedence_over_home() {
        let path = config_path_from(Some("/xdg"), Some("/home/me"), None, false);
        assert_eq!(path, Some(PathBuf::from("/xdg/opcda-bridge/client.toml")));
    }

    #[test]
    fn test_load_config_file_valid() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "host = \"example:1234\"\nserver = \"S1\"\nmax_tags = 50"
        )
        .unwrap();
        let config = load_config_file(file.path(), true).unwrap();
        assert_eq!(config.host, Some("example:1234".to_string()));
        assert_eq!(config.server, Some("S1".to_string()));
        assert_eq!(config.max_tags, Some(50));
    }

    #[test]
    fn test_load_config_file_empty_is_all_defaults() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let config = load_config_file(file.path(), true).unwrap();
        assert_eq!(config, ClientConfig::default());
    }

    #[test]
    fn test_load_config_file_malformed() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "max_tags = \"not a number\"").unwrap();
        let err = load_config_file(file.path(), true).unwrap_err();
        assert!(err.to_string().contains("failed to parse config file"));
    }

    #[test]
    fn test_load_config_file_missing_not_error() {
        let config = load_config_file(Path::new("/nonexistent/client.toml"), false).unwrap();
        assert_eq!(config, ClientConfig::default());
    }

    #[test]
    fn test_load_config_file_missing_is_error() {
        let err = load_config_file(Path::new("/nonexistent/client.toml"), true).unwrap_err();
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
        writeln!(file, "host = \"custom:9999\"").unwrap();
        let config = load_config(Some(file.path())).unwrap();
        assert_eq!(config.host, Some("custom:9999".to_string()));
    }

    #[test]
    fn test_load_config_explicit_path_missing_errors() {
        let err = load_config(Some(Path::new("/nonexistent/client.toml"))).unwrap_err();
        assert!(err.to_string().contains("config file not found"));
    }

    // std::env::set_var/remove_var mutate process-global state, but `cargo
    // test` runs tests in parallel threads by default; this guards the one
    // test below that touches real XDG_CONFIG_HOME/HOME/APPDATA env vars.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_load_config_default_discovery_absent_env() {
        // With none of XDG_CONFIG_HOME/HOME/APPDATA visible, discovery
        // should yield no path and fall back to defaults without error.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let saved = [
            std::env::var("XDG_CONFIG_HOME").ok(),
            std::env::var("HOME").ok(),
            std::env::var("APPDATA").ok(),
        ];
        unsafe {
            std::env::remove_var("XDG_CONFIG_HOME");
            std::env::remove_var("HOME");
            std::env::remove_var("APPDATA");
        }
        let result = load_config(None);
        unsafe {
            for (var, value) in ["XDG_CONFIG_HOME", "HOME", "APPDATA"]
                .iter()
                .zip(saved.iter())
            {
                if let Some(v) = value {
                    std::env::set_var(var, v);
                }
            }
        }
        assert_eq!(result.unwrap(), ClientConfig::default());
    }

    #[test]
    fn test_resolve_host_cli_wins() {
        let config = ClientConfig {
            host: Some("configured:1".into()),
            ..Default::default()
        };
        assert_eq!(
            resolve_host(Some("cli:2".to_string()), &config),
            "cli:2".to_string()
        );
    }

    #[test]
    fn test_resolve_host_config_wins_over_default() {
        let config = ClientConfig {
            host: Some("configured:1".into()),
            ..Default::default()
        };
        assert_eq!(resolve_host(None, &config), "configured:1".to_string());
    }

    #[test]
    fn test_resolve_host_default() {
        assert_eq!(
            resolve_host(None, &ClientConfig::default()),
            DEFAULT_HOST.to_string()
        );
    }

    #[test]
    fn test_resolve_server_cli_wins() {
        let config = ClientConfig {
            server: Some("ConfigServer".into()),
            ..Default::default()
        };
        assert_eq!(
            resolve_server(Some("CliServer".to_string()), &config).unwrap(),
            "CliServer"
        );
    }

    #[test]
    fn test_resolve_server_config_fallback() {
        let config = ClientConfig {
            server: Some("ConfigServer".into()),
            ..Default::default()
        };
        assert_eq!(resolve_server(None, &config).unwrap(), "ConfigServer");
    }

    #[test]
    fn test_resolve_server_neither_set_errors() {
        let err = resolve_server(None, &ClientConfig::default()).unwrap_err();
        assert!(err.to_string().contains("no OPC server specified"));
    }

    #[test]
    fn test_resolve_max_tags_cli_wins() {
        let config = ClientConfig {
            max_tags: Some(10),
            ..Default::default()
        };
        assert_eq!(resolve_max_tags(Some(20), &config), 20);
    }

    #[test]
    fn test_resolve_max_tags_config_wins_over_default() {
        let config = ClientConfig {
            max_tags: Some(10),
            ..Default::default()
        };
        assert_eq!(resolve_max_tags(None, &config), 10);
    }

    #[test]
    fn test_resolve_max_tags_default() {
        assert_eq!(
            resolve_max_tags(None, &ClientConfig::default()),
            DEFAULT_MAX_TAGS
        );
    }

    #[test]
    fn test_resolve_output_cli_wins() {
        let config = ClientConfig {
            output: Some(OutputFormat::Json),
            ..Default::default()
        };
        assert_eq!(
            resolve_output(Some(OutputFormat::Table), &config),
            OutputFormat::Table
        );
    }

    #[test]
    fn test_resolve_output_config_wins_over_default() {
        let config = ClientConfig {
            output: Some(OutputFormat::Json),
            ..Default::default()
        };
        assert_eq!(resolve_output(None, &config), OutputFormat::Json);
    }

    #[test]
    fn test_resolve_output_default_is_table() {
        assert_eq!(
            resolve_output(None, &ClientConfig::default()),
            OutputFormat::Table
        );
    }

    #[test]
    fn test_load_config_file_output_key() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "output = \"json\"").unwrap();
        let config = load_config_file(file.path(), true).unwrap();
        assert_eq!(config.output, Some(OutputFormat::Json));
    }
}
