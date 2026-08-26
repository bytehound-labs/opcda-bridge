use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::opc::MAX_NATIVE_INVENTORY_BATCH_SIZE;

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
#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct GatewayConfig {
    pub port: Option<u16>,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub index: IndexConfig,
}

/// Logging configuration keys, consumed once file-based logging lands.
#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct LogConfig {
    pub level: Option<String>,
    pub dir: Option<String>,
    pub format: Option<String>,
    pub rotation: Option<String>,
}

/// Persistent namespace-index settings.
#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct IndexConfig {
    /// Service-writable SQLite path. If omitted, the platform data directory
    /// is used.
    pub database_path: Option<String>,
    /// Only these OPC DA ProgIDs may be indexed automatically.
    #[serde(default)]
    pub servers: Vec<String>,
    /// Set false to disable automatic indexing while retaining manual APIs.
    pub enabled: Option<bool>,
    pub refresh_interval_seconds: Option<u64>,
    /// Policy for creating the first generation when no complete index exists.
    #[serde(default)]
    pub initial_build_policy: Option<InitialBuildPolicy>,
    /// Delay after gateway startup before automatic indexing is considered.
    pub startup_grace_period_seconds: Option<u64>,
    /// Deterministic per-server delay added to scheduled refreshes.
    pub schedule_jitter_seconds: Option<u64>,
    /// Maximum entries requested from one native inventory slice.
    pub inventory_batch_size: Option<u32>,
    /// Maximum entries committed to SQLite in one transaction.
    pub commit_batch_size: Option<u32>,
    /// Maximum time pending entries may wait before an SQLite commit.
    pub commit_interval_ms: Option<u64>,
    pub batch_size: Option<u32>,
    pub item_rate_limit: Option<u32>,
    pub burst_size: Option<u32>,
    pub duty_cycle_percent: Option<u8>,
    /// Enable adaptive AIMD control for inventory pacing.
    pub adaptive: Option<bool>,
    /// Lower bound used after a health/resource backoff.
    pub minimum_item_rate: Option<u32>,
    pub minimum_batch_size: Option<u32>,
    pub minimum_duty_cycle_percent: Option<u8>,
    /// Conservative starting profile for each inventory build.
    pub canary_item_rate: Option<u32>,
    pub canary_batch_size: Option<u32>,
    pub canary_duty_cycle_percent: Option<u8>,
    pub adaptive_healthy_window_seconds: Option<u64>,
    pub adaptive_recovery_delay_seconds: Option<u64>,
    pub adaptive_max_recovery_delay_seconds: Option<u64>,
    pub sentinel_tag: Option<String>,
    pub sentinel_probe_interval_seconds: Option<u64>,
    pub minimum_free_space_bytes: Option<u64>,
    pub storage_headroom_bytes: Option<u64>,
    pub circuit_failure_threshold: Option<u32>,
    pub circuit_open_seconds: Option<u64>,
    pub quiet_period_seconds: Option<u64>,
    pub health_probe_interval_seconds: Option<u64>,
    pub health_latency_threshold_ms: Option<u64>,
    /// Maximum time allowed for one pre-build or health OPC operation.
    pub operation_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub maintenance_windows: Vec<String>,
    pub concurrency: Option<u32>,
    pub query_cache_capacity: Option<usize>,
    pub paused: Option<bool>,
    pub max_results: Option<u32>,
}

/// Resolved index settings used by the gateway runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIndexConfig {
    pub database_path: PathBuf,
    pub servers: Vec<String>,
    pub enabled: bool,
    pub refresh_interval_seconds: u64,
    pub initial_build_policy: InitialBuildPolicy,
    pub startup_grace_period_seconds: u64,
    pub schedule_jitter_seconds: u64,
    pub inventory_batch_size: u32,
    pub commit_batch_size: u32,
    pub commit_interval_ms: u64,
    pub batch_size: u32,
    pub item_rate_limit: u32,
    pub burst_size: u32,
    pub duty_cycle_percent: u8,
    pub adaptive: bool,
    pub minimum_item_rate: u32,
    pub minimum_batch_size: u32,
    pub minimum_duty_cycle_percent: u8,
    pub canary_item_rate: u32,
    pub canary_batch_size: u32,
    pub canary_duty_cycle_percent: u8,
    pub adaptive_healthy_window_seconds: u64,
    pub adaptive_recovery_delay_seconds: u64,
    pub adaptive_max_recovery_delay_seconds: u64,
    pub sentinel_tag: Option<String>,
    pub sentinel_probe_interval_seconds: u64,
    pub minimum_free_space_bytes: u64,
    pub storage_headroom_bytes: u64,
    pub circuit_failure_threshold: u32,
    pub circuit_open_seconds: u64,
    pub quiet_period_seconds: u64,
    pub health_probe_interval_seconds: u64,
    pub health_latency_threshold_ms: u64,
    pub operation_timeout_seconds: u64,
    pub maintenance_windows: Vec<String>,
    pub concurrency: u32,
    pub query_cache_capacity: usize,
    pub paused: bool,
    pub max_results: u32,
}

pub const DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS: u64 = 604_800;
pub const DEFAULT_INDEX_STARTUP_GRACE_PERIOD_SECONDS: u64 = 30;
pub const DEFAULT_INDEX_SCHEDULE_JITTER_SECONDS: u64 = 21_600;
pub const DEFAULT_INDEX_INVENTORY_BATCH_SIZE: u32 = 100;
pub const DEFAULT_INDEX_COMMIT_BATCH_SIZE: u32 = 100;
pub const DEFAULT_INDEX_COMMIT_INTERVAL_MS: u64 = 1_000;
pub const DEFAULT_INDEX_BATCH_SIZE: u32 = 100;
pub const DEFAULT_INDEX_ITEM_RATE: u32 = 250;
pub const DEFAULT_INDEX_BURST_SIZE: u32 = 100;
pub const DEFAULT_INDEX_DUTY_CYCLE_PERCENT: u8 = 20;
pub const DEFAULT_INDEX_ADAPTIVE: bool = true;
pub const DEFAULT_INDEX_MINIMUM_ITEM_RATE: u32 = 10;
pub const DEFAULT_INDEX_MINIMUM_BATCH_SIZE: u32 = 1;
pub const DEFAULT_INDEX_MINIMUM_DUTY_CYCLE_PERCENT: u8 = 1;
pub const DEFAULT_INDEX_CANARY_ITEM_RATE: u32 = 50;
pub const DEFAULT_INDEX_CANARY_BATCH_SIZE: u32 = 25;
pub const DEFAULT_INDEX_CANARY_DUTY_CYCLE_PERCENT: u8 = 5;
pub const DEFAULT_INDEX_ADAPTIVE_HEALTHY_WINDOW_SECONDS: u64 = 30;
pub const DEFAULT_INDEX_ADAPTIVE_RECOVERY_DELAY_SECONDS: u64 = 30;
pub const DEFAULT_INDEX_ADAPTIVE_MAX_RECOVERY_DELAY_SECONDS: u64 = 300;
pub const DEFAULT_INDEX_SENTINEL_PROBE_INTERVAL_SECONDS: u64 = 30;
pub const DEFAULT_INDEX_MINIMUM_FREE_SPACE_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_INDEX_STORAGE_HEADROOM_BYTES: u64 = 10 * 1024 * 1024;
pub const DEFAULT_INDEX_CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
pub const DEFAULT_INDEX_CIRCUIT_OPEN_SECONDS: u64 = 300;
pub const DEFAULT_INDEX_QUIET_PERIOD_SECONDS: u64 = 2;
pub const DEFAULT_INDEX_HEALTH_PROBE_INTERVAL_SECONDS: u64 = 30;
pub const DEFAULT_INDEX_HEALTH_LATENCY_THRESHOLD_MS: u64 = 500;
pub const DEFAULT_INDEX_OPERATION_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_INDEX_CONCURRENCY: u32 = 1;
pub const DEFAULT_INDEX_QUERY_CACHE_CAPACITY: usize = 256;
pub const DEFAULT_INDEX_MAX_RESULTS: u32 = 50;

/// Controls whether the first automatic generation may start without a
/// maintenance window.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InitialBuildPolicy {
    /// Start automatically only while a configured maintenance window is open.
    #[default]
    MaintenanceWindow,
    /// Start automatically after startup grace, even without a window.
    Immediate,
    /// Never start the first generation automatically.
    Manual,
}

/// Return the platform data path used for the gateway-owned index.
pub fn index_path_from(
    xdg_data_home: Option<&str>,
    home: Option<&str>,
    program_data: Option<&str>,
    is_windows: bool,
) -> Option<PathBuf> {
    if is_windows {
        return program_data.map(|dir| Path::new(dir).join("opcda-bridge").join("index.sqlite3"));
    }
    if let Some(dir) = xdg_data_home {
        return Some(Path::new(dir).join("opcda-bridge").join("index.sqlite3"));
    }
    home.map(|dir| {
        Path::new(dir)
            .join(".local")
            .join("share")
            .join("opcda-bridge")
            .join("index.sqlite3")
    })
}

pub fn resolve_index_config(config: &IndexConfig) -> ResolvedIndexConfig {
    let database_path = config
        .database_path
        .as_deref()
        .map(PathBuf::from)
        .or_else(|| {
            index_path_from(
                std::env::var("XDG_DATA_HOME").ok().as_deref(),
                std::env::var("HOME").ok().as_deref(),
                std::env::var("PROGRAMDATA").ok().as_deref(),
                cfg!(target_os = "windows"),
            )
        })
        .unwrap_or_else(|| PathBuf::from("opcda-bridge-index.sqlite3"));

    ResolvedIndexConfig {
        database_path,
        servers: config.servers.clone(),
        enabled: config.enabled.unwrap_or(true),
        refresh_interval_seconds: config
            .refresh_interval_seconds
            .unwrap_or(DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS),
        initial_build_policy: config.initial_build_policy.unwrap_or_default(),
        startup_grace_period_seconds: config
            .startup_grace_period_seconds
            .unwrap_or(DEFAULT_INDEX_STARTUP_GRACE_PERIOD_SECONDS),
        schedule_jitter_seconds: config
            .schedule_jitter_seconds
            .unwrap_or(DEFAULT_INDEX_SCHEDULE_JITTER_SECONDS),
        inventory_batch_size: config
            .inventory_batch_size
            .or(config.batch_size)
            .unwrap_or(DEFAULT_INDEX_INVENTORY_BATCH_SIZE)
            .clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE),
        commit_batch_size: config
            .commit_batch_size
            .or(config.batch_size)
            .unwrap_or(DEFAULT_INDEX_COMMIT_BATCH_SIZE)
            .max(1),
        commit_interval_ms: config
            .commit_interval_ms
            .unwrap_or(DEFAULT_INDEX_COMMIT_INTERVAL_MS)
            .max(1),
        batch_size: config
            .batch_size
            .unwrap_or(DEFAULT_INDEX_BATCH_SIZE)
            .clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE),
        item_rate_limit: config.item_rate_limit.unwrap_or(DEFAULT_INDEX_ITEM_RATE),
        burst_size: config.burst_size.unwrap_or(DEFAULT_INDEX_BURST_SIZE).max(1),
        duty_cycle_percent: config
            .duty_cycle_percent
            .unwrap_or(DEFAULT_INDEX_DUTY_CYCLE_PERCENT)
            .clamp(1, 100),
        adaptive: config.adaptive.unwrap_or(DEFAULT_INDEX_ADAPTIVE),
        minimum_item_rate: config
            .minimum_item_rate
            .unwrap_or(DEFAULT_INDEX_MINIMUM_ITEM_RATE)
            .max(1),
        minimum_batch_size: config
            .minimum_batch_size
            .unwrap_or(DEFAULT_INDEX_MINIMUM_BATCH_SIZE)
            .clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE),
        minimum_duty_cycle_percent: config
            .minimum_duty_cycle_percent
            .unwrap_or(DEFAULT_INDEX_MINIMUM_DUTY_CYCLE_PERCENT)
            .clamp(1, 100),
        canary_item_rate: config
            .canary_item_rate
            .unwrap_or(DEFAULT_INDEX_CANARY_ITEM_RATE)
            .max(1),
        canary_batch_size: config
            .canary_batch_size
            .unwrap_or(DEFAULT_INDEX_CANARY_BATCH_SIZE)
            .clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE),
        canary_duty_cycle_percent: config
            .canary_duty_cycle_percent
            .unwrap_or(DEFAULT_INDEX_CANARY_DUTY_CYCLE_PERCENT)
            .clamp(1, 100),
        adaptive_healthy_window_seconds: config
            .adaptive_healthy_window_seconds
            .unwrap_or(DEFAULT_INDEX_ADAPTIVE_HEALTHY_WINDOW_SECONDS)
            .max(1),
        adaptive_recovery_delay_seconds: config
            .adaptive_recovery_delay_seconds
            .unwrap_or(DEFAULT_INDEX_ADAPTIVE_RECOVERY_DELAY_SECONDS)
            .max(1),
        adaptive_max_recovery_delay_seconds: config
            .adaptive_max_recovery_delay_seconds
            .unwrap_or(DEFAULT_INDEX_ADAPTIVE_MAX_RECOVERY_DELAY_SECONDS)
            .max(1),
        sentinel_tag: config.sentinel_tag.clone(),
        sentinel_probe_interval_seconds: config
            .sentinel_probe_interval_seconds
            .unwrap_or(DEFAULT_INDEX_SENTINEL_PROBE_INTERVAL_SECONDS)
            .max(1),
        minimum_free_space_bytes: config
            .minimum_free_space_bytes
            .unwrap_or(DEFAULT_INDEX_MINIMUM_FREE_SPACE_BYTES),
        storage_headroom_bytes: config
            .storage_headroom_bytes
            .unwrap_or(DEFAULT_INDEX_STORAGE_HEADROOM_BYTES),
        circuit_failure_threshold: config
            .circuit_failure_threshold
            .unwrap_or(DEFAULT_INDEX_CIRCUIT_FAILURE_THRESHOLD)
            .max(1),
        circuit_open_seconds: config
            .circuit_open_seconds
            .unwrap_or(DEFAULT_INDEX_CIRCUIT_OPEN_SECONDS)
            .max(1),
        quiet_period_seconds: config
            .quiet_period_seconds
            .unwrap_or(DEFAULT_INDEX_QUIET_PERIOD_SECONDS),
        health_probe_interval_seconds: config
            .health_probe_interval_seconds
            .unwrap_or(DEFAULT_INDEX_HEALTH_PROBE_INTERVAL_SECONDS)
            .max(1),
        health_latency_threshold_ms: config
            .health_latency_threshold_ms
            .unwrap_or(DEFAULT_INDEX_HEALTH_LATENCY_THRESHOLD_MS)
            .max(1),
        operation_timeout_seconds: config
            .operation_timeout_seconds
            .unwrap_or(DEFAULT_INDEX_OPERATION_TIMEOUT_SECONDS)
            .max(1),
        maintenance_windows: config.maintenance_windows.clone(),
        concurrency: config
            .concurrency
            .unwrap_or(DEFAULT_INDEX_CONCURRENCY)
            .max(1),
        query_cache_capacity: config
            .query_cache_capacity
            .unwrap_or(DEFAULT_INDEX_QUERY_CACHE_CAPACITY)
            .max(1),
        paused: config.paused.unwrap_or(false),
        max_results: config
            .max_results
            .unwrap_or(DEFAULT_INDEX_MAX_RESULTS)
            .max(1),
    }
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
            // This path selects the adjacent user configuration file, not a
            // security-sensitive executable or library.
            // nosemgrep: rust.lang.security.current-exe.current-exe
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
    use proptest::prelude::*;
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
    fn test_previous_gateway_config_fixture_remains_compatible() {
        let config: GatewayConfig =
            toml::from_str(include_str!("../tests/fixtures/gateway-v0.1.toml")).unwrap();

        assert_eq!(config.port, Some(7700));
        assert_eq!(config.log.level.as_deref(), Some("debug"));
    }

    #[test]
    fn test_resolve_port_cli_wins() {
        let config = GatewayConfig {
            port: Some(1111),
            log: LogConfig::default(),
            index: IndexConfig::default(),
        };
        assert_eq!(resolve_port(Some(2222), &config), 2222);
    }

    #[test]
    fn test_resolve_port_config_wins_over_default() {
        let config = GatewayConfig {
            port: Some(1111),
            log: LogConfig::default(),
            index: IndexConfig::default(),
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
    fn test_index_path_discovery_covers_platform_precedence() {
        assert_eq!(
            index_path_from(
                Some("/xdg"),
                Some("/home/mike"),
                Some("/program-data"),
                false
            ),
            Some(PathBuf::from("/xdg/opcda-bridge/index.sqlite3"))
        );
        assert_eq!(
            index_path_from(None, Some("/home/mike"), Some("/program-data"), false),
            Some(PathBuf::from(
                "/home/mike/.local/share/opcda-bridge/index.sqlite3"
            ))
        );
        assert_eq!(
            index_path_from(None, None, Some("/program-data"), true),
            Some(PathBuf::from("/program-data/opcda-bridge/index.sqlite3"))
        );
        assert_eq!(index_path_from(None, None, None, true), None);
        assert_eq!(index_path_from(None, None, None, false), None);
    }

    #[test]
    fn test_resolve_index_config_applies_defaults_and_safe_bounds() {
        let config = IndexConfig {
            database_path: Some("custom.sqlite3".into()),
            servers: vec!["S".into()],
            enabled: Some(false),
            refresh_interval_seconds: Some(12),
            initial_build_policy: Some(InitialBuildPolicy::Immediate),
            startup_grace_period_seconds: Some(9),
            schedule_jitter_seconds: Some(8),
            inventory_batch_size: Some(11),
            commit_batch_size: Some(12),
            commit_interval_ms: Some(13),
            batch_size: Some(0),
            item_rate_limit: Some(0),
            burst_size: Some(0),
            duty_cycle_percent: Some(0),
            adaptive: Some(false),
            minimum_item_rate: Some(3),
            minimum_batch_size: Some(2),
            minimum_duty_cycle_percent: Some(4),
            canary_item_rate: Some(5),
            canary_batch_size: Some(6),
            canary_duty_cycle_percent: Some(7),
            adaptive_healthy_window_seconds: Some(8),
            adaptive_recovery_delay_seconds: Some(9),
            adaptive_max_recovery_delay_seconds: Some(10),
            sentinel_tag: Some("S.Health".into()),
            sentinel_probe_interval_seconds: Some(14),
            minimum_free_space_bytes: Some(15),
            storage_headroom_bytes: Some(16),
            circuit_failure_threshold: Some(17),
            circuit_open_seconds: Some(18),
            quiet_period_seconds: Some(3),
            health_probe_interval_seconds: Some(0),
            health_latency_threshold_ms: Some(4),
            operation_timeout_seconds: Some(0),
            maintenance_windows: vec!["22:00-06:00".into()],
            concurrency: Some(0),
            query_cache_capacity: Some(0),
            paused: Some(true),
            max_results: Some(0),
        };
        let resolved = resolve_index_config(&config);
        assert_eq!(resolved.database_path, PathBuf::from("custom.sqlite3"));
        assert_eq!(resolved.servers, vec!["S".to_string()]);
        assert!(!resolved.enabled);
        assert_eq!(resolved.refresh_interval_seconds, 12);
        assert_eq!(resolved.initial_build_policy, InitialBuildPolicy::Immediate);
        assert_eq!(resolved.startup_grace_period_seconds, 9);
        assert_eq!(resolved.schedule_jitter_seconds, 8);
        assert_eq!(resolved.inventory_batch_size, 11);
        assert_eq!(resolved.commit_batch_size, 12);
        assert_eq!(resolved.commit_interval_ms, 13);
        assert_eq!(resolved.batch_size, 1);
        assert_eq!(resolved.item_rate_limit, 0);
        assert_eq!(resolved.burst_size, 1);
        assert_eq!(resolved.duty_cycle_percent, 1);
        assert!(!resolved.adaptive);
        assert_eq!(resolved.minimum_item_rate, 3);
        assert_eq!(resolved.minimum_batch_size, 2);
        assert_eq!(resolved.minimum_duty_cycle_percent, 4);
        assert_eq!(resolved.canary_item_rate, 5);
        assert_eq!(resolved.canary_batch_size, 6);
        assert_eq!(resolved.canary_duty_cycle_percent, 7);
        assert_eq!(resolved.adaptive_healthy_window_seconds, 8);
        assert_eq!(resolved.adaptive_recovery_delay_seconds, 9);
        assert_eq!(resolved.adaptive_max_recovery_delay_seconds, 10);
        assert_eq!(resolved.sentinel_tag.as_deref(), Some("S.Health"));
        assert_eq!(resolved.sentinel_probe_interval_seconds, 14);
        assert_eq!(resolved.minimum_free_space_bytes, 15);
        assert_eq!(resolved.storage_headroom_bytes, 16);
        assert_eq!(resolved.circuit_failure_threshold, 17);
        assert_eq!(resolved.circuit_open_seconds, 18);
        assert_eq!(resolved.quiet_period_seconds, 3);
        assert_eq!(resolved.health_probe_interval_seconds, 1);
        assert_eq!(resolved.health_latency_threshold_ms, 4);
        assert_eq!(resolved.operation_timeout_seconds, 1);
        assert_eq!(resolved.maintenance_windows, vec!["22:00-06:00"]);
        assert_eq!(resolved.concurrency, 1);
        assert_eq!(resolved.query_cache_capacity, 1);
        assert!(resolved.paused);
        assert_eq!(resolved.max_results, 1);
    }

    #[test]
    fn test_resolve_index_config_uses_weekly_safe_scheduler_defaults() {
        let resolved = resolve_index_config(&IndexConfig::default());
        assert_eq!(resolved.refresh_interval_seconds, 604_800);
        assert_eq!(
            resolved.initial_build_policy,
            InitialBuildPolicy::MaintenanceWindow
        );
        assert_eq!(resolved.startup_grace_period_seconds, 30);
        assert_eq!(resolved.schedule_jitter_seconds, 21_600);
        assert_eq!(resolved.inventory_batch_size, 100);
        assert_eq!(resolved.commit_batch_size, 100);
        assert_eq!(resolved.commit_interval_ms, 1_000);
        assert_eq!(resolved.sentinel_tag, None);
        assert_eq!(resolved.operation_timeout_seconds, 30);
    }

    #[test]
    fn test_resolve_index_config_clamps_native_batch_sizes() {
        let oversized = MAX_NATIVE_INVENTORY_BATCH_SIZE + 1;
        let resolved = resolve_index_config(&IndexConfig {
            inventory_batch_size: Some(oversized),
            batch_size: Some(oversized),
            minimum_batch_size: Some(oversized),
            canary_batch_size: Some(oversized),
            ..IndexConfig::default()
        });

        assert_eq!(
            resolved.inventory_batch_size,
            MAX_NATIVE_INVENTORY_BATCH_SIZE
        );
        assert_eq!(resolved.batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE);
        assert_eq!(resolved.minimum_batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE);
        assert_eq!(resolved.canary_batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE);
        assert_eq!(resolved.commit_batch_size, oversized);
    }

    #[test]
    fn test_index_config_toml_round_trip() {
        let original = IndexConfig {
            database_path: Some("index.sqlite3".into()),
            servers: vec!["S1".into(), "S2".into()],
            enabled: Some(true),
            refresh_interval_seconds: Some(60),
            initial_build_policy: Some(InitialBuildPolicy::Manual),
            startup_grace_period_seconds: Some(45),
            schedule_jitter_seconds: Some(120),
            inventory_batch_size: Some(20),
            commit_batch_size: Some(7),
            commit_interval_ms: Some(250),
            batch_size: Some(10),
            item_rate_limit: Some(20),
            burst_size: Some(5),
            duty_cycle_percent: Some(50),
            adaptive: Some(true),
            minimum_item_rate: Some(10),
            minimum_batch_size: Some(2),
            minimum_duty_cycle_percent: Some(3),
            canary_item_rate: Some(20),
            canary_batch_size: Some(5),
            canary_duty_cycle_percent: Some(8),
            adaptive_healthy_window_seconds: Some(30),
            adaptive_recovery_delay_seconds: Some(60),
            adaptive_max_recovery_delay_seconds: Some(300),
            sentinel_tag: Some("S.Health".into()),
            sentinel_probe_interval_seconds: Some(45),
            minimum_free_space_bytes: Some(100),
            storage_headroom_bytes: Some(200),
            circuit_failure_threshold: Some(4),
            circuit_open_seconds: Some(600),
            quiet_period_seconds: Some(2),
            health_probe_interval_seconds: Some(30),
            health_latency_threshold_ms: Some(500),
            operation_timeout_seconds: Some(45),
            maintenance_windows: vec!["00:00-06:00".into()],
            concurrency: Some(1),
            query_cache_capacity: Some(10),
            paused: Some(false),
            max_results: Some(25),
        };
        let decoded: IndexConfig = toml::from_str(&toml::to_string(&original).unwrap()).unwrap();
        assert_eq!(decoded, original);
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

    proptest::proptest! {
        #[test]
        fn prop_gateway_config_toml_round_trip(
            port in proptest::option::of(any::<u16>()),
            level in proptest::option::of("[a-zA-Z0-9=,._-]{0,32}"),
            dir in proptest::option::of("[a-zA-Z0-9:/._-]{0,32}"),
            format in proptest::option::of("[a-zA-Z]{0,12}"),
            rotation in proptest::option::of("[a-zA-Z]{0,12}"),
        ) {
            let original = GatewayConfig {
                port,
                log: LogConfig {
                    level,
                    dir,
                    format,
                    rotation,
                },
                index: IndexConfig::default(),
            };
            let encoded = toml::to_string(&original).unwrap();
            let decoded: GatewayConfig = toml::from_str(&encoded).unwrap();
            prop_assert_eq!(decoded, original);
        }

        #[test]
        fn prop_malformed_gateway_toml_never_panics(input in any::<String>()) {
            let _ = toml::from_str::<GatewayConfig>(&input);
        }
    }
}
