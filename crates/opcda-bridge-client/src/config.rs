use crate::output::OutputFormat;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Default gateway host:port the client connects to when nothing else specifies one.
pub const DEFAULT_HOST: &str = "localhost:7600";
/// Default number of children requested per browse page.
pub const DEFAULT_PAGE_SIZE: u32 = opcda_bridge::DEFAULT_PAGE_SIZE;
/// Default safety cap for the explicitly expensive `browse --all` mode.
pub const DEFAULT_BROWSE_ALL_LIMIT: u32 = 10_000;
/// Default maximum number of matches requested by `search`.
pub const DEFAULT_SEARCH_MAX_RESULTS: u32 = opcda_bridge::DEFAULT_SEARCH_MAX_RESULTS;
/// Default maximum number of matches requested by `index-search`.
pub const DEFAULT_INDEX_SEARCH_MAX_RESULTS: u32 = opcda_bridge::DEFAULT_INDEX_SEARCH_MAX_RESULTS;

/// Client configuration loaded from an optional TOML file. Every field is
/// optional; a value missing from the file (or the file itself missing)
/// falls back to the env var / CLI flag / built-in default resolution.
#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ClientConfig {
    pub host: Option<String>,
    pub server: Option<String>,
    pub page_size: Option<u32>,
    pub browse_all_limit: Option<u32>,
    pub search_max_results: Option<u32>,
    pub index_search_max_results: Option<u32>,
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

/// Resolve the browse page size with `CLI flag > config file > default` precedence.
pub fn resolve_page_size(cli_page_size: Option<u32>, config: &ClientConfig) -> u32 {
    cli_page_size
        .or(config.page_size)
        .unwrap_or(DEFAULT_PAGE_SIZE)
}

/// Resolve the `browse --all` safety cap.
pub fn resolve_browse_all_limit(cli_limit: Option<u32>, config: &ClientConfig) -> u32 {
    cli_limit
        .or(config.browse_all_limit)
        .unwrap_or(DEFAULT_BROWSE_ALL_LIMIT)
}

/// Resolve the search result cap.
pub fn resolve_search_max_results(cli_limit: Option<u32>, config: &ClientConfig) -> u32 {
    cli_limit
        .or(config.search_max_results)
        .unwrap_or(DEFAULT_SEARCH_MAX_RESULTS)
}

/// Resolve the persistent-index search result cap.
pub fn resolve_index_search_max_results(cli_limit: Option<u32>, config: &ClientConfig) -> u32 {
    cli_limit
        .or(config.index_search_max_results)
        .unwrap_or(DEFAULT_INDEX_SEARCH_MAX_RESULTS)
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
    use proptest::prelude::*;
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
            "host = \"example:1234\"\nserver = \"S1\"\npage_size = 50\nbrowse_all_limit = 500\nsearch_max_results = 75\nindex_search_max_results = 25"
        )
        .unwrap();
        let config = load_config_file(file.path(), true).unwrap();
        assert_eq!(config.host, Some("example:1234".to_string()));
        assert_eq!(config.server, Some("S1".to_string()));
        assert_eq!(config.page_size, Some(50));
        assert_eq!(config.browse_all_limit, Some(500));
        assert_eq!(config.search_max_results, Some(75));
        assert_eq!(config.index_search_max_results, Some(25));
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
        writeln!(file, "page_size = \"not a number\"").unwrap();
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
        // ENV_MUTEX serializes these Rust 2024 environment mutations.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe {
            std::env::remove_var("XDG_CONFIG_HOME");
            std::env::remove_var("HOME");
            std::env::remove_var("APPDATA");
        }
        let result = load_config(None);
        // ENV_MUTEX serializes this Rust 2024 environment mutation block.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
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
    fn test_resolve_page_size_cli_wins() {
        let config = ClientConfig {
            page_size: Some(10),
            ..Default::default()
        };
        assert_eq!(resolve_page_size(Some(20), &config), 20);
    }

    #[test]
    fn test_resolve_page_size_config_wins_over_default() {
        let config = ClientConfig {
            page_size: Some(10),
            ..Default::default()
        };
        assert_eq!(resolve_page_size(None, &config), 10);
    }

    #[test]
    fn test_resolve_page_size_default() {
        assert_eq!(
            resolve_page_size(None, &ClientConfig::default()),
            DEFAULT_PAGE_SIZE
        );
    }

    #[test]
    fn test_resolve_browse_all_limit_precedence() {
        let config = ClientConfig {
            browse_all_limit: Some(500),
            ..Default::default()
        };
        assert_eq!(resolve_browse_all_limit(Some(600), &config), 600);
        assert_eq!(resolve_browse_all_limit(None, &config), 500);
        assert_eq!(
            resolve_browse_all_limit(None, &ClientConfig::default()),
            DEFAULT_BROWSE_ALL_LIMIT
        );
    }

    #[test]
    fn test_resolve_search_max_results_precedence() {
        let config = ClientConfig {
            search_max_results: Some(50),
            ..Default::default()
        };
        assert_eq!(resolve_search_max_results(Some(60), &config), 60);
        assert_eq!(resolve_search_max_results(None, &config), 50);
        assert_eq!(
            resolve_search_max_results(None, &ClientConfig::default()),
            DEFAULT_SEARCH_MAX_RESULTS
        );
    }

    #[test]
    fn test_resolve_index_search_max_results_precedence() {
        let config = ClientConfig {
            index_search_max_results: Some(40),
            ..Default::default()
        };
        assert_eq!(resolve_index_search_max_results(Some(45), &config), 45);
        assert_eq!(resolve_index_search_max_results(None, &config), 40);
        assert_eq!(
            resolve_index_search_max_results(None, &ClientConfig::default()),
            DEFAULT_INDEX_SEARCH_MAX_RESULTS
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

    #[test]
    fn test_client_config_fixture() {
        let config: ClientConfig =
            toml::from_str(include_str!("../tests/fixtures/client-v0.3.toml")).unwrap();

        assert_eq!(config.host.as_deref(), Some("gateway:7600"));
        assert_eq!(config.server.as_deref(), Some("Kepware.KepServerEX.V5"));
        assert_eq!(config.page_size, Some(250));
        assert_eq!(config.browse_all_limit, Some(2_000));
        assert_eq!(config.search_max_results, Some(100));
        assert_eq!(config.index_search_max_results, None);
        assert_eq!(resolve_output(None, &config), OutputFormat::Table);
    }

    proptest::proptest! {
        #[test]
        fn prop_client_config_toml_round_trip(
            host in proptest::option::of("[a-zA-Z0-9:/._-]{0,32}"),
            server in proptest::option::of("[a-zA-Z0-9._-]{0,32}"),
            page_size in proptest::option::of(any::<u32>()),
            browse_all_limit in proptest::option::of(any::<u32>()),
            search_max_results in proptest::option::of(any::<u32>()),
            index_search_max_results in proptest::option::of(any::<u32>()),
            output in proptest::option::of(proptest::prop_oneof![
                Just(OutputFormat::Table),
                Just(OutputFormat::Json),
            ]),
        ) {
            let original = ClientConfig {
                host,
                server,
                page_size,
                browse_all_limit,
                search_max_results,
                index_search_max_results,
                output,
            };
            let encoded = toml::to_string(&original).unwrap();
            let decoded: ClientConfig = toml::from_str(&encoded).unwrap();
            prop_assert_eq!(decoded, original);
        }

        #[test]
        fn prop_malformed_client_toml_never_panics(input in any::<String>()) {
            let _ = toml::from_str::<ClientConfig>(&input);
        }
    }
}
