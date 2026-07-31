use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "opcda-bridge", about = "OPC DA bridge client", version)]
pub struct Cli {
    #[arg(long, env = "OPC_BRIDGE_HOST", default_value = "localhost:7600")]
    pub host: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// List available OPC DA servers
    Servers,
    /// Browse tags on a server
    Browse {
        /// OPC DA server ProgID
        #[arg(long)]
        server: String,
        /// Flat list (skip tree structure)
        #[arg(long)]
        flat: bool,
    },
    /// Read tag values
    Read {
        /// OPC DA server ProgID
        #[arg(long)]
        server: String,
        /// Tag IDs to read
        tags: Vec<String>,
    },
    /// Write a value to a tag
    Write {
        /// OPC DA server ProgID
        #[arg(long)]
        server: String,
        /// Tag ID to write
        tag: String,
        /// Value to write (parsed as bool, int, float, or string)
        value: String,
    },
}

pub async fn run_command(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Commands::Servers => crate::commands::cmd_servers(cli.host).await?,
        Commands::Browse { server, flat } => {
            crate::commands::cmd_browse(cli.host, server, flat).await?
        }
        Commands::Read { server, tags } => {
            crate::commands::cmd_read(cli.host, server, tags).await?
        }
        Commands::Write { server, tag, value } => {
            crate::commands::cmd_write(cli.host, server, tag, value).await?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use bridge_proto::bridge::{BrowseResponse, WriteResponse};
    use clap::Parser;
    use std::sync::Mutex;

    // std::env::set_var/remove_var mutate process-global state, but `cargo
    // test` runs tests in parallel threads by default, so the tests below
    // that touch OPC_BRIDGE_HOST race with each other unless serialized.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn test_run_command_servers() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host,
            command: Commands::Servers,
        };
        run_command(cli).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_command_browse() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host,
            command: Commands::Browse {
                server: "S".into(),
                flat: false,
            },
        };
        run_command(cli).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_command_read() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let cli = Cli {
            host,
            command: Commands::Read {
                server: "S".into(),
                tags: vec![],
            },
        };
        run_command(cli).await.unwrap();
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
            host,
            command: Commands::Write {
                server: "S".into(),
                tag: "t".into(),
                value: "hello".into(),
            },
        };
        run_command(cli).await.unwrap();
    }

    #[tokio::test]
    async fn test_browse_drop_triggers_break() {
        use bridge_proto::bridge::BrowseRequest;
        use bridge_proto::bridge::bridge_client::BridgeClient;
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
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.host, "localhost:7600");
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
        assert_eq!(args.host, "192.168.1.1:9999");
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
            Commands::Browse { ref server, flat } if server == "MyServer" && flat
        ));
    }

    #[test]
    fn test_cli_browse_no_flat() {
        let args = Cli::try_parse_from(["opcda-bridge", "browse", "--server", "MyServer"]).unwrap();
        assert!(matches!(
            args.command,
            Commands::Browse { ref server, flat: false } if server == "MyServer"
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
                if server == "MyServer"
                    && tags == &vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()]
        ));
    }

    #[test]
    fn test_cli_read_no_tags() {
        let args = Cli::try_parse_from(["opcda-bridge", "read", "--server", "MyServer"]).unwrap();
        assert!(matches!(
            args.command,
            Commands::Read { ref server, ref tags } if server == "MyServer" && tags.is_empty()
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
                if server == "MyServer" && tag == "Tag1" && value == "42"
        ));
    }

    #[test]
    fn test_cli_host_from_env() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.host, "envhost:8888");
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
    }

    #[test]
    fn test_cli_arg_overrides_env() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888") };
        let args =
            Cli::try_parse_from(["opcda-bridge", "--host", "arghost:7777", "servers"]).unwrap();
        assert_eq!(args.host, "arghost:7777");
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
    }
}
