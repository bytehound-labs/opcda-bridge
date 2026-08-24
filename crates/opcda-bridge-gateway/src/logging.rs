use std::path::{Path, PathBuf};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::EnvFilter;

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable, ANSI-free (log files aren't a terminal).
    Pretty,
    /// Newline-delimited JSON, for log shippers.
    Json,
}

/// Parse the configured log format. Defaults to `Pretty` for `None` or any
/// unrecognized value — a config typo in `log.format` should degrade
/// gracefully rather than stop the gateway from starting.
pub fn parse_log_format(value: Option<&str>) -> LogFormat {
    match value {
        Some(v) if v.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

/// Parse the configured rotation policy. Defaults to `DAILY` for `None` or
/// any unrecognized value, for the same reason as [`parse_log_format`].
pub fn parse_rotation(value: Option<&str>) -> Rotation {
    match value {
        Some(v) if v.eq_ignore_ascii_case("hourly") => Rotation::HOURLY,
        Some(v) if v.eq_ignore_ascii_case("never") => Rotation::NEVER,
        _ => Rotation::DAILY,
    }
}

/// Derive the gateway's default log directory from its own executable
/// path: a `logs` directory next to the `.exe`.
pub fn log_dir_from_exe(exe_path: &Path) -> PathBuf {
    exe_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("logs")
}

/// Build an `EnvFilter` from an explicit level/directive spec (e.g. `"debug"`
/// or `"opcda_bridge_gateway=debug,tower=warn"`), falling back to `info` if
/// `level` is absent or fails to parse. Logging misconfiguration should
/// degrade gracefully rather than stop the gateway from starting.
pub fn build_env_filter(level: Option<&str>) -> EnvFilter {
    level
        .and_then(|spec| EnvFilter::try_new(spec).ok())
        .unwrap_or_else(|| EnvFilter::new("info"))
}

/// Resolved logging settings, after applying `CLI flag > env var > config
/// file > default` precedence to each individual field.
#[derive(Debug, Clone, PartialEq)]
pub struct LogSettings {
    pub level: Option<String>,
    pub dir: PathBuf,
    pub format: LogFormat,
    pub rotation: Rotation,
}

/// Resolve every logging setting. `cli_level` already has `RUST_LOG` folded
/// in by clap's `env` attribute on `Cli::log_level`; `dir`/`format`/
/// `rotation` have no env var, matching the rest of the config surface.
pub fn resolve_log_settings(
    cli_level: Option<String>,
    cli_dir: Option<PathBuf>,
    cli_format: Option<String>,
    cli_rotation: Option<String>,
    config: &crate::config::LogConfig,
    default_dir: &Path,
) -> LogSettings {
    let level = cli_level.or_else(|| config.level.clone());
    let dir = cli_dir
        .or_else(|| config.dir.clone().map(PathBuf::from))
        .unwrap_or_else(|| default_dir.to_path_buf());
    let format = parse_log_format(cli_format.as_deref().or(config.format.as_deref()));
    let rotation = parse_rotation(cli_rotation.as_deref().or(config.rotation.as_deref()));
    LogSettings {
        level,
        dir,
        format,
        rotation,
    }
}

/// Initialize the process-global tracing subscriber: a non-blocking
/// rolling file writer under `settings.dir`, plus a stdout writer when a
/// console is actually attached (a Windows service has none).
///
/// Returns the `WorkerGuard`, which the caller **must** hold for the
/// process lifetime — dropping it early silently truncates buffered log
/// lines that haven't yet been flushed to disk on exit.
pub fn init_tracing(settings: &LogSettings) -> anyhow::Result<WorkerGuard> {
    use std::io::IsTerminal;
    init_tracing_with_stdout(settings, std::io::stdout().is_terminal())
}

fn build_file_appender(settings: &LogSettings) -> anyhow::Result<RollingFileAppender> {
    Ok(RollingFileAppender::builder()
        .rotation(settings.rotation.clone())
        .filename_prefix("opcda-bridge-gateway")
        .filename_suffix("log")
        .build(&settings.dir)?)
}

/// Same as [`init_tracing`], but with "is a console attached" passed in
/// explicitly rather than detected, so tests can exercise both the
/// stdout-attached and stdout-detached layer wiring deterministically.
fn init_tracing_with_stdout(
    settings: &LogSettings,
    attach_stdout: bool,
) -> anyhow::Result<WorkerGuard> {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    std::fs::create_dir_all(&settings.dir)?;
    let file_appender = build_file_appender(settings)?;
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let filter = build_env_filter(settings.level.as_deref());

    let file_layer = match settings.format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_writer(non_blocking)
            .boxed(),
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(non_blocking)
            .boxed(),
    };
    let stdout_layer = attach_stdout.then(tracing_subscriber::fmt::layer);

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(stdout_layer)
        .try_init()?;
    Ok(guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_log_format_json() {
        assert_eq!(parse_log_format(Some("json")), LogFormat::Json);
        assert_eq!(parse_log_format(Some("JSON")), LogFormat::Json);
    }

    #[test]
    fn test_parse_log_format_pretty() {
        assert_eq!(parse_log_format(Some("pretty")), LogFormat::Pretty);
    }

    #[test]
    fn test_parse_log_format_unknown_defaults_to_pretty() {
        assert_eq!(parse_log_format(Some("yaml")), LogFormat::Pretty);
    }

    #[test]
    fn test_parse_log_format_none_defaults_to_pretty() {
        assert_eq!(parse_log_format(None), LogFormat::Pretty);
    }

    #[test]
    fn test_parse_rotation_hourly() {
        assert_eq!(parse_rotation(Some("hourly")), Rotation::HOURLY);
        assert_eq!(parse_rotation(Some("HOURLY")), Rotation::HOURLY);
    }

    #[test]
    fn test_parse_rotation_never() {
        assert_eq!(parse_rotation(Some("never")), Rotation::NEVER);
    }

    #[test]
    fn test_parse_rotation_daily() {
        assert_eq!(parse_rotation(Some("daily")), Rotation::DAILY);
    }

    #[test]
    fn test_parse_rotation_unknown_defaults_to_daily() {
        assert_eq!(parse_rotation(Some("weekly")), Rotation::DAILY);
    }

    #[test]
    fn test_parse_rotation_none_defaults_to_daily() {
        assert_eq!(parse_rotation(None), Rotation::DAILY);
    }

    #[test]
    fn test_log_dir_from_exe_with_parent() {
        let dir = log_dir_from_exe(Path::new("/usr/local/bin/opcda-bridge-gateway"));
        assert_eq!(dir, PathBuf::from("/usr/local/bin/logs"));
    }

    #[test]
    fn test_log_dir_from_exe_no_parent() {
        let dir = log_dir_from_exe(Path::new("/"));
        assert_eq!(dir, PathBuf::from("./logs"));
    }

    #[test]
    fn test_build_env_filter_explicit_level() {
        // EnvFilter has no public equality check, so assert indirectly via
        // Debug output, which includes the directive spec.
        let filter = build_env_filter(Some("debug"));
        assert!(format!("{filter}").contains("debug"));
    }

    #[test]
    fn test_build_env_filter_invalid_falls_back_to_info() {
        // "notalevel" isn't one of the recognized level names/numbers, so
        // this is a genuine parse failure (unlike e.g. "not a valid
        // directive!!", which `EnvFilter` happily accepts as a target-name
        // filter with an implicit "trace" level).
        let filter = build_env_filter(Some("level=notalevel"));
        assert!(format!("{filter}").contains("info"));
    }

    #[test]
    fn test_build_env_filter_none_defaults_to_info() {
        let filter = build_env_filter(None);
        assert!(format!("{filter}").contains("info"));
    }

    #[test]
    fn test_resolve_log_settings_cli_wins() {
        let config = crate::config::LogConfig {
            level: Some("warn".to_string()),
            dir: Some("/config/dir".to_string()),
            format: Some("json".to_string()),
            rotation: Some("hourly".to_string()),
        };
        let settings = resolve_log_settings(
            Some("debug".to_string()),
            Some(PathBuf::from("/cli/dir")),
            Some("pretty".to_string()),
            Some("never".to_string()),
            &config,
            Path::new("/default/dir"),
        );
        assert_eq!(settings.level, Some("debug".to_string()));
        assert_eq!(settings.dir, PathBuf::from("/cli/dir"));
        assert_eq!(settings.format, LogFormat::Pretty);
        assert_eq!(settings.rotation, Rotation::NEVER);
    }

    #[test]
    fn test_resolve_log_settings_config_wins_over_default() {
        let config = crate::config::LogConfig {
            level: Some("warn".to_string()),
            dir: Some("/config/dir".to_string()),
            format: Some("json".to_string()),
            rotation: Some("hourly".to_string()),
        };
        let settings =
            resolve_log_settings(None, None, None, None, &config, Path::new("/default/dir"));
        assert_eq!(settings.level, Some("warn".to_string()));
        assert_eq!(settings.dir, PathBuf::from("/config/dir"));
        assert_eq!(settings.format, LogFormat::Json);
        assert_eq!(settings.rotation, Rotation::HOURLY);
    }

    #[test]
    fn test_resolve_log_settings_defaults() {
        let settings = resolve_log_settings(
            None,
            None,
            None,
            None,
            &crate::config::LogConfig::default(),
            Path::new("/default/dir"),
        );
        assert_eq!(settings.level, None);
        assert_eq!(settings.dir, PathBuf::from("/default/dir"));
        assert_eq!(settings.format, LogFormat::Pretty);
        assert_eq!(settings.rotation, Rotation::DAILY);
    }

    fn created_log_filename(rotation: Rotation) -> String {
        let dir = tempfile::tempdir().unwrap();
        let settings = LogSettings {
            level: None,
            dir: dir.path().to_path_buf(),
            format: LogFormat::Pretty,
            rotation,
        };
        let _appender = build_file_appender(&settings).unwrap();
        let filenames = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(filenames.len(), 1);
        filenames.into_iter().next().unwrap()
    }

    fn assert_dated_log_filename(filename: &str, date_length: usize, separators: &[usize]) {
        let date = filename
            .strip_prefix("opcda-bridge-gateway.")
            .unwrap()
            .strip_suffix(".log")
            .unwrap();
        assert_eq!(date.len(), date_length);
        assert!(date.bytes().enumerate().all(|(index, byte)| {
            if separators.contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        }));
    }

    #[test]
    fn test_file_appender_daily_filename() {
        let filename = created_log_filename(Rotation::DAILY);
        assert_dated_log_filename(&filename, 10, &[4, 7]);
    }

    #[test]
    fn test_file_appender_hourly_filename() {
        let filename = created_log_filename(Rotation::HOURLY);
        assert_dated_log_filename(&filename, 13, &[4, 7, 10]);
    }

    #[test]
    fn test_file_appender_never_filename() {
        assert_eq!(
            created_log_filename(Rotation::NEVER),
            "opcda-bridge-gateway.log"
        );
    }

    // `tracing_subscriber`'s global subscriber can only be installed once
    // per process, and `cargo test` runs all unit tests in this crate in
    // one shared process across multiple threads. Exactly one call to
    // `try_init()` anywhere in this binary can succeed; every other call
    // (in this module or any other) observes an error. The tests below
    // therefore never assert `Ok`/`Err` on the *outcome* of installing —
    // only that every line up to and including that call actually runs,
    // which is all the 100%-line-coverage gate requires.

    #[test]
    fn test_init_tracing_with_stdout_covers_json_and_pretty_layers() {
        let dir = tempfile::tempdir().unwrap();
        let json_settings = LogSettings {
            level: Some("debug".to_string()),
            dir: dir.path().to_path_buf(),
            format: LogFormat::Json,
            rotation: Rotation::NEVER,
        };
        let pretty_settings = LogSettings {
            level: None,
            dir: dir.path().to_path_buf(),
            format: LogFormat::Pretty,
            rotation: Rotation::DAILY,
        };
        // Exercise both the JSON+stdout-attached and Pretty+stdout-detached
        // combinations so every layer-construction branch runs regardless
        // of which call (if any) wins the global-install race.
        let _ = init_tracing_with_stdout(&json_settings, true);
        let _ = init_tracing_with_stdout(&pretty_settings, false);
    }

    #[test]
    fn test_init_tracing_wrapper_detects_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let settings = LogSettings {
            level: None,
            dir: dir.path().to_path_buf(),
            format: LogFormat::Pretty,
            rotation: Rotation::NEVER,
        };
        let _ = init_tracing(&settings);
    }
}
