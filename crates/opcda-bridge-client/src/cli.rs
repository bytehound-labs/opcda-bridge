use crate::output::OutputFormat;
use clap::{Parser, Subcommand, ValueEnum};
use opcda_bridge::SearchMatchMode;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "opcda-bridge", about = "OPC DA bridge client", version)]
pub struct Cli {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SearchMode {
    Exact,
    Prefix,
    Contains,
}

impl From<SearchMode> for SearchMatchMode {
    fn from(value: SearchMode) -> Self {
        match value {
            SearchMode::Exact => Self::Exact,
            SearchMode::Prefix => Self::Prefix,
            SearchMode::Contains => Self::Contains,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// List available OPC DA servers
    Servers,
    /// Show gateway and namespace capabilities for an OPC DA server
    Capabilities {
        #[arg(long)]
        server: Option<String>,
    },
    /// Browse one bounded page of immediate namespace children
    Browse {
        /// OPC DA server ProgID (falls back to the config file's `server` key)
        #[arg(long)]
        server: Option<String>,
        /// Existing browse session for child or continuation requests
        #[arg(long)]
        session_id: Option<String>,
        /// Opaque node key returned by an earlier browse/search result
        #[arg(long, requires = "session_id")]
        parent_node_key: Option<String>,
        /// Opaque continuation token returned by the preceding page
        #[arg(long, requires = "session_id")]
        page_token: Option<String>,
        /// Maximum children requested in each page
        #[arg(long)]
        page_size: Option<u32>,
        /// Follow continuation tokens until complete or capped; this may be expensive
        #[arg(long)]
        all: bool,
        /// Total-result safety cap used only with `--all`
        #[arg(long, requires = "all")]
        max_results: Option<u32>,
        /// Bypass cached namespace metadata
        #[arg(long)]
        refresh: bool,
    },
    /// Release an active browse session
    CloseBrowseSession {
        /// Opaque session ID returned by browse
        session_id: String,
    },
    /// Search the namespace with progressive results and progress events
    Search {
        /// Literal query to match
        query: String,
        #[arg(long)]
        server: Option<String>,
        /// Match mode: exact, prefix, or contains
        #[arg(long, value_enum, default_value_t = SearchMode::Contains)]
        match_mode: SearchMode,
        /// Existing browse session whose discovered namespace may be reused
        #[arg(long)]
        session_id: Option<String>,
        /// Restrict search to an opaque browse node
        #[arg(long, requires = "session_id")]
        scope_node_key: Option<String>,
        /// Maximum number of matches
        #[arg(long)]
        max_results: Option<u32>,
        /// Include branch-only nodes in matches
        #[arg(long)]
        include_branches: bool,
        /// Bypass cached namespace metadata
        #[arg(long)]
        refresh: bool,
    },
    /// Read tag values
    Read {
        #[arg(long)]
        server: Option<String>,
        /// Exact OPC DA ItemIDs to read
        tags: Vec<String>,
    },
    /// Write a value to a tag
    Write {
        #[arg(long)]
        server: Option<String>,
        /// Exact OPC DA ItemID to write
        tag: String,
        /// Value to write (parsed as bool, int, float, or string)
        value: String,
    },
}

pub async fn run_command(
    cli: Cli,
    config: &crate::config::ClientConfig,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let host = crate::config::resolve_host(cli.host, config);

    match cli.command {
        Commands::Servers => crate::commands::cmd_servers(host, format).await?,
        Commands::Capabilities { server } => {
            let server = crate::config::resolve_server(server, config)?;
            crate::commands::cmd_capabilities(host, server, format).await?
        }
        Commands::Browse {
            server,
            session_id,
            parent_node_key,
            page_token,
            page_size,
            all,
            max_results,
            refresh,
        } => {
            let server = crate::config::resolve_server(server, config)?;
            let page_size = crate::config::resolve_page_size(page_size, config);
            let max_results = crate::config::resolve_browse_all_limit(max_results, config);
            crate::commands::cmd_browse(
                host,
                server,
                session_id,
                parent_node_key,
                page_token,
                page_size,
                all,
                max_results,
                refresh,
                format,
            )
            .await?
        }
        Commands::CloseBrowseSession { session_id } => {
            crate::commands::cmd_close_browse_session(host, session_id, format).await?
        }
        Commands::Search {
            query,
            server,
            match_mode,
            session_id,
            scope_node_key,
            max_results,
            include_branches,
            refresh,
        } => {
            let server = crate::config::resolve_server(server, config)?;
            let max_results = crate::config::resolve_search_max_results(max_results, config);
            crate::commands::cmd_search(
                host,
                server,
                query,
                match_mode.into(),
                session_id,
                scope_node_key,
                max_results,
                include_branches,
                refresh,
                format,
            )
            .await?
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
    use opcda_bridge_proto::bridge::WriteResponse;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn cli(command: Commands, host: String) -> Cli {
        Cli {
            host: Some(host),
            config: None,
            output: None,
            json: false,
            command,
        }
    }

    #[tokio::test]
    async fn run_command_dispatches_all_surfaces() {
        let commands = vec![
            Commands::Servers,
            Commands::Capabilities {
                server: Some("S".into()),
            },
            Commands::Browse {
                server: Some("S".into()),
                session_id: None,
                parent_node_key: None,
                page_token: None,
                page_size: Some(20),
                all: false,
                max_results: None,
                refresh: false,
            },
            Commands::CloseBrowseSession {
                session_id: "session".into(),
            },
            Commands::Search {
                query: "PV".into(),
                server: Some("S".into()),
                match_mode: SearchMode::Exact,
                session_id: None,
                scope_node_key: None,
                max_results: Some(5),
                include_branches: false,
                refresh: false,
            },
            Commands::Read {
                server: Some("S".into()),
                tags: vec![],
            },
            Commands::Write {
                server: Some("S".into()),
                tag: "t".into(),
                value: "1".into(),
            },
        ];

        for command in commands {
            let host = start_mock_server(MockBridgeService {
                write_response: WriteResponse {
                    tag_id: "t".into(),
                    success: true,
                    error: None,
                },
                ..Default::default()
            })
            .await;
            run_command(
                cli(command, host),
                &crate::config::ClientConfig::default(),
                OutputFormat::Table,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn commands_requiring_server_fail_without_one() {
        let command = Commands::Browse {
            server: None,
            session_id: None,
            parent_node_key: None,
            page_token: None,
            page_size: None,
            all: false,
            max_results: None,
            refresh: false,
        };
        let err = run_command(
            cli(command, "unused".into()),
            &crate::config::ClientConfig::default(),
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("no OPC server specified"));
    }

    #[tokio::test]
    async fn mock_server_shutdown_completes() {
        let service = MockBridgeService::default();
        let shutdown = Arc::clone(&service.server_shutdown);
        let stopped = Arc::clone(&service.server_stopped);
        let _host = start_mock_server(service).await;
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(1), stopped.notified())
            .await
            .unwrap();
    }

    #[test]
    fn cli_parses_new_browse_and_search_flags() {
        let args = Cli::try_parse_from([
            "opcda-bridge",
            "browse",
            "--server",
            "S",
            "--session-id",
            "session",
            "--parent-node-key",
            "node",
            "--page-token",
            "token",
            "--page-size",
            "50",
            "--all",
            "--max-results",
            "500",
            "--refresh",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse {
                page_size: Some(50),
                all: true,
                max_results: Some(500),
                refresh: true,
                ..
            }
        ));

        let args = Cli::try_parse_from([
            "opcda-bridge",
            "search",
            "PV",
            "--server",
            "S",
            "--match-mode",
            "prefix",
            "--max-results",
            "20",
            "--include-branches",
        ])
        .unwrap();
        assert!(matches!(
            args.command,
            Commands::Search {
                match_mode: SearchMode::Prefix,
                max_results: Some(20),
                include_branches: true,
                ..
            }
        ));
    }

    #[test]
    fn browse_opaque_keys_require_a_session() {
        for flag in ["--parent-node-key", "--page-token"] {
            let args = ["opcda-bridge", "browse", "--server", "S", flag, "opaque"];
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn search_modes_map_to_library_modes() {
        assert_eq!(
            SearchMatchMode::from(SearchMode::Exact),
            SearchMatchMode::Exact
        );
        assert_eq!(
            SearchMatchMode::from(SearchMode::Prefix),
            SearchMatchMode::Prefix
        );
        assert_eq!(
            SearchMatchMode::from(SearchMode::Contains),
            SearchMatchMode::Contains
        );
    }

    #[test]
    fn global_flags_and_environment_parse() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe {
            std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888");
            std::env::set_var("OPC_BRIDGE_OUTPUT", "json");
        }
        let args = Cli::try_parse_from(["opcda-bridge", "servers", "--json"]).unwrap();
        assert_eq!(args.host.as_deref(), Some("envhost:8888"));
        assert_eq!(args.output, Some(OutputFormat::Json));
        assert!(args.json);
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        unsafe {
            std::env::remove_var("OPC_BRIDGE_HOST");
            std::env::remove_var("OPC_BRIDGE_OUTPUT");
        }
    }

    #[test]
    fn version_flag_is_available() {
        let err = Cli::try_parse_from(["opcda-bridge", "--version"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
        assert!(err.to_string().contains(env!("CARGO_PKG_VERSION")));
    }
}
