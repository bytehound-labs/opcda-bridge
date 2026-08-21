use crate::output::OutputFormat;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "opcda-bridge", about = "OPC DA bridge client", version)]
pub struct Cli {
    // `global = true` on these four lets them be passed either before or
    // after the subcommand (e.g. both `--json read ...` and `read ... --json`
    // work), instead of clap's default of requiring them before it.
    #[arg(long, env = "OPC_BRIDGE_HOST", global = true)]
    pub host: Option<String>,

    /// Path to a TOML config file (default: platform config dir, see README)
    #[arg(long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    /// Output format: `table` (default) or `json`
    #[arg(
        long,
        value_enum,
        value_name = "FORMAT",
        env = "OPC_BRIDGE_OUTPUT",
        global = true
    )]
    pub output: Option<OutputFormat>,

    /// Shorthand for `--output json`. If both are set, `--json` wins.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// List available OPC DA servers
    Servers,
    /// Browse tags on a server
    Browse {
        /// OPC DA server ProgID (falls back to the config file's `server` key)
        #[arg(long)]
        server: Option<String>,
        /// Flat list (skip tree structure)
        #[arg(long)]
        flat: bool,
        /// Path to browse (default: root). Pass a `Branch` tag from a prior
        /// browse to drill down one level further.
        #[arg(long, default_value = "")]
        path: String,
        /// Cap on the number of tags streamed back (default: 1000)
        #[arg(long)]
        max_tags: Option<u32>,
    },
    /// Read tag values
    Read {
        /// OPC DA server ProgID (falls back to the config file's `server` key)
        #[arg(long)]
        server: Option<String>,
        /// Tag IDs to read
        tags: Vec<String>,
    },
    /// Write a value to a tag
    Write {
        /// OPC DA server ProgID (falls back to the config file's `server` key)
        #[arg(long)]
        server: Option<String>,
        /// Tag ID to write
        tag: String,
        /// Value to write (parsed as bool, int, float, or string)
        value: String,
    },
}

/// Dispatch a parsed `Cli` to the requested subcommand.
///
/// Takes an already-loaded `config` and already-resolved `format` rather
/// than loading the config itself, so a config-load failure (handled by
/// the caller, `lib::run`) can be reported in the right format even before
/// this function would otherwise learn the config file's `output` key.
pub async fn run_command(
    cli: Cli,
    config: &crate::config::ClientConfig,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let host = crate::config::resolve_host(cli.host, config);

    match cli.command {
        Commands::Servers => crate::commands::cmd_servers(host, format).await?,
        Commands::Browse {
            server,
            flat,
            path,
            max_tags,
        } => {
            let server = crate::config::resolve_server(server, config)?;
            let max_tags = crate::config::resolve_max_tags(max_tags, config);
            crate::commands::cmd_browse(host, server, flat, path, max_tags, format).await?
        }
        Commands::Read { server, tags } => {
            let server = crate::config::resolve_server(server, config)?;
            crate::commands::cmd_read(host, server, tags, format).await?
        }
        Commands::Write { server, tag, value } => {
            let server = crate::config::resolve_server(server, config)?;
            crate::commands::cmd_write(host, server, tag, value, format).await?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use clap::Parser;
    use opcda_bridge_proto::bridge::{BrowseResponse, WriteResponse};
    use std::sync::Mutex;

    // std::env::set_var/remove_var mutate process-global state, but `cargo
    // test` runs tests in parallel threads by default, so the tests below
    // that touch OPC_BRIDGE_HOST race with each other unless serialized.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn test_run_command_servers() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host: Some(host),
            config: None,
            output: None,
            json: false,
            command: Commands::Servers,
        };
        run_command(
            cli,
            &crate::config::ClientConfig::default(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_run_command_browse() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host: Some(host),
            config: None,
            output: None,
            json: false,
            command: Commands::Browse {
                server: Some("S".into()),
                flat: false,
                path: String::new(),
                max_tags: None,
            },
        };
        run_command(
            cli,
            &crate::config::ClientConfig::default(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_run_command_browse_no_server_errors() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host: Some(host),
            config: None,
            output: None,
            json: false,
            command: Commands::Browse {
                server: None,
                flat: false,
                path: String::new(),
                max_tags: None,
            },
        };
        let err = run_command(
            cli,
            &crate::config::ClientConfig::default(),
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("no OPC server specified"));
    }

    #[tokio::test]
    async fn test_run_command_read() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host: Some(host),
            config: None,
            output: None,
            json: false,
            command: Commands::Read {
                server: Some("S".into()),
                tags: vec![],
            },
        };
        run_command(
            cli,
            &crate::config::ClientConfig::default(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_run_command_write() {
        let host = start_mock_server(MockBridgeService {
            write_response: WriteResponse {
                tag_id: "t".into(),
                success: true,
                error: None,
            },
            ..Default::default()
        })
        .await;
        let cli = Cli {
            host: Some(host),
            config: None,
            output: None,
            json: false,
            command: Commands::Write {
                server: Some("S".into()),
                tag: "t".into(),
                value: "hello".into(),
            },
        };
        run_command(
            cli,
            &crate::config::ClientConfig::default(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_browse_drop_triggers_break() {
        use opcda_bridge_proto::bridge::BrowseRequest;
        use opcda_bridge_proto::bridge::bridge_client::BridgeClient;
        let host = start_mock_server(MockBridgeService {
            browse_responses: (0..300)
                .map(|i| BrowseResponse {
                    tag_id: format!("tag{i}"),
                    node_type: "Leaf".into(),
                })
                .collect(),
            ..Default::default()
        })
        .await;
        let mut client = BridgeClient::connect(format!("http://{host}"))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let mut stream = client
            .browse(BrowseRequest {
                server: "S".into(),
                flat: false,
                path: String::new(),
                max_tags: 1000,
            })
            .await
            .unwrap()
            .into_inner();
        let _first = stream.next().await;
        drop(stream);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    #[test]
    fn test_cli_default_host() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes these Rust 2024 unsafe environment mutations.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.host, None);
    }

    #[test]
    fn test_cli_version_flag() {
        let err = Cli::try_parse_from(["opcda-bridge", "--version"])
            .err()
            .unwrap();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
        assert!(err.to_string().contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn test_cli_custom_host() {
        let args =
            Cli::try_parse_from(["opcda-bridge", "--host", "192.168.1.1:9999", "servers"]).unwrap();
        assert_eq!(args.host, Some("192.168.1.1:9999".to_string()));
    }

    #[test]
    fn test_cli_global_flags_after_subcommand() {
        // host/config/output/json are `global = true` so they can be placed
        // after the subcommand too, not just before it.
        let args = Cli::try_parse_from([
            "opcda-bridge",
            "read",
            "--server",
            "MyServer",
            "tag1",
            "--host",
            "192.168.1.1:9999",
            "--json",
        ])
        .unwrap();
        assert_eq!(args.host, Some("192.168.1.1:9999".to_string()));
        assert!(args.json);
    }

    #[test]
    fn test_cli_config_flag() {
        let args =
            Cli::try_parse_from(["opcda-bridge", "--config", "custom.toml", "servers"]).unwrap();
        assert_eq!(args.config, Some(PathBuf::from("custom.toml")));
    }

    #[test]
    fn test_cli_servers_command() {
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert!(matches!(args.command, Commands::Servers));
    }

    #[test]
    fn test_cli_browse_command() {
        let args =
            Cli::try_parse_from(["opcda-bridge", "browse", "--server", "MyServer", "--flat"])
                .unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse { ref server, flat, .. } if server.as_deref() == Some("MyServer") && flat
        ));
    }

    #[test]
    fn test_cli_browse_no_flat() {
        let args = Cli::try_parse_from(["opcda-bridge", "browse", "--server", "MyServer"]).unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse { ref server, flat: false, .. } if server.as_deref() == Some("MyServer")
        ));
    }

    #[test]
    fn test_cli_browse_no_server() {
        let args = Cli::try_parse_from(["opcda-bridge", "browse"]).unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse { server: None, .. }
        ));
    }

    #[test]
    fn test_cli_browse_max_tags() {
        let args = Cli::try_parse_from([
            "opcda-bridge",
            "browse",
            "--server",
            "MyServer",
            "--max-tags",
            "50",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse {
                max_tags: Some(50),
                ..
            }
        ));
    }

    #[test]
    fn test_cli_browse_default_path_is_root() {
        let args = Cli::try_parse_from(["opcda-bridge", "browse", "--server", "MyServer"]).unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse { ref path, .. } if path.is_empty()
        ));
    }

    #[test]
    fn test_cli_browse_path_flag() {
        let args = Cli::try_parse_from([
            "opcda-bridge",
            "browse",
            "--server",
            "MyServer",
            "--path",
            "Simulink.Device1",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse { ref path, .. } if path == "Simulink.Device1"
        ));
    }

    #[test]
    fn test_cli_read_command() {
        let args = Cli::try_parse_from([
            "opcda-bridge",
            "read",
            "--server",
            "MyServer",
            "tag1",
            "tag2",
            "tag3",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Commands::Read { ref server, ref tags }
                if server.as_deref() == Some("MyServer")
                    && tags == &vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()]
        ));
    }

    #[test]
    fn test_cli_read_no_tags() {
        let args = Cli::try_parse_from(["opcda-bridge", "read", "--server", "MyServer"]).unwrap();
        assert!(matches!(
            args.command,
            Commands::Read { ref server, ref tags } if server.as_deref() == Some("MyServer") && tags.is_empty()
        ));
    }

    #[test]
    fn test_cli_write_command() {
        let args = Cli::try_parse_from([
            "opcda-bridge",
            "write",
            "--server",
            "MyServer",
            "Tag1",
            "42",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Commands::Write { ref server, ref tag, ref value }
                if server.as_deref() == Some("MyServer") && tag == "Tag1" && value == "42"
        ));
    }

    #[test]
    fn test_cli_host_from_env() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes these Rust 2024 unsafe environment mutations.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.host, Some("envhost:8888".to_string()));
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
    }

    #[test]
    fn test_cli_arg_overrides_env() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes these Rust 2024 unsafe environment mutations.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888") };
        let args =
            Cli::try_parse_from(["opcda-bridge", "--host", "arghost:7777", "servers"]).unwrap();
        assert_eq!(args.host, Some("arghost:7777".to_string()));
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
    }

    #[test]
    fn test_cli_default_output_is_none() {
        // OPC_BRIDGE_OUTPUT is read by every Cli::try_parse_from call, so
        // this must be guarded/cleared just like test_cli_default_host,
        // or a concurrently-running env-setting test in another thread
        // could leak a value in here.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes this Rust 2024 unsafe environment mutation.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_OUTPUT") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.output, None);
        assert!(!args.json);
    }

    #[test]
    fn test_cli_output_table_flag() {
        let args = Cli::try_parse_from(["opcda-bridge", "--output", "table", "servers"]).unwrap();
        assert_eq!(args.output, Some(OutputFormat::Table));
    }

    #[test]
    fn test_cli_output_json_flag() {
        let args = Cli::try_parse_from(["opcda-bridge", "--output", "json", "servers"]).unwrap();
        assert_eq!(args.output, Some(OutputFormat::Json));
    }

    #[test]
    fn test_cli_json_shorthand_flag() {
        // See test_cli_default_output_is_none: args.output is asserted here
        // too, so this needs the same guard/clear.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes this Rust 2024 unsafe environment mutation.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_OUTPUT") };
        let args = Cli::try_parse_from(["opcda-bridge", "--json", "servers"]).unwrap();
        assert!(args.json);
        assert_eq!(args.output, None);
    }

    #[test]
    fn test_cli_output_from_env() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes these Rust 2024 unsafe environment mutations.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::set_var("OPC_BRIDGE_OUTPUT", "json") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.output, Some(OutputFormat::Json));
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_OUTPUT") };
    }

    #[test]
    fn test_cli_json_flag_with_output_env_set_both_parse() {
        // `--json` and `--output` (even env-sourced) are not declared as
        // clap conflicts: resolve_from_cli resolves the precedence in code
        // (--json always wins) instead, so both can be present here.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes these Rust 2024 unsafe environment mutations.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::set_var("OPC_BRIDGE_OUTPUT", "table") };
        let args = Cli::try_parse_from(["opcda-bridge", "--json", "servers"]).unwrap();
        assert!(args.json);
        assert_eq!(args.output, Some(OutputFormat::Table));
        assert_eq!(
            crate::output::resolve_from_cli(&args),
            Some(OutputFormat::Json)
        );
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_OUTPUT") };
    }

    #[test]
    fn test_resolve_from_cli_json_wins_over_output() {
        let args = Cli::try_parse_from(["opcda-bridge", "--json", "--output", "table", "servers"])
            .unwrap();
        assert_eq!(
            crate::output::resolve_from_cli(&args),
            Some(OutputFormat::Json)
        );
    }

    #[test]
    fn test_resolve_from_cli_output_only() {
        let args = Cli::try_parse_from(["opcda-bridge", "--output", "json", "servers"]).unwrap();
        assert_eq!(
            crate::output::resolve_from_cli(&args),
            Some(OutputFormat::Json)
        );
    }

    #[test]
    fn test_resolve_from_cli_neither_set() {
        // args.output is env-sensitive when neither --output nor --json is
        // passed, so this needs the same guard/clear as
        // test_cli_default_output_is_none.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The mutex serializes this Rust 2024 unsafe environment mutation.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe { std::env::remove_var("OPC_BRIDGE_OUTPUT") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(crate::output::resolve_from_cli(&args), None);
    }
}
