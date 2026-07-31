use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "opcda-bridge", about = "OPC DA bridge client")]
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
    use bridge_proto::bridge::bridge_server::{Bridge, BridgeServer};
    use bridge_proto::bridge::{
        BrowseRequest, BrowseResponse, ListServersRequest, ListServersResponse, ReadRequest,
        ReadResponse, TagValue as ProtoTagValue, WriteRequest, WriteResponse,
    };
    use clap::Parser;
    use std::net::SocketAddr;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    struct MockBridgeService {
        response: MockResponse,
    }

    enum MockResponse {
        ListServers(ListServersResponse),
        BrowseResponses(Vec<BrowseResponse>),
        Read(ReadResponse),
        Write(WriteResponse),
    }

    #[tonic::async_trait]
    impl Bridge for MockBridgeService {
        async fn list_servers(
            &self,
            _request: Request<ListServersRequest>,
        ) -> Result<Response<ListServersResponse>, Status> {
            match &self.response {
                MockResponse::ListServers(r) => Ok(Response::new(r.clone())),
                _ => Ok(Response::new(ListServersResponse { servers: vec![] })),
            }
        }

        type BrowseStream = ReceiverStream<Result<BrowseResponse, Status>>;

        async fn browse(
            &self,
            _request: Request<BrowseRequest>,
        ) -> Result<Response<Self::BrowseStream>, Status> {
            let items = match &self.response {
                MockResponse::BrowseResponses(r) => r.clone(),
                _ => vec![],
            };
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                for item in items {
                    if tx.send(Ok(item)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn read(
            &self,
            _request: Request<ReadRequest>,
        ) -> Result<Response<ReadResponse>, Status> {
            match &self.response {
                MockResponse::Read(r) => Ok(Response::new(r.clone())),
                _ => Ok(Response::new(ReadResponse { values: vec![] })),
            }
        }

        async fn write(
            &self,
            _request: Request<WriteRequest>,
        ) -> Result<Response<WriteResponse>, Status> {
            match &self.response {
                MockResponse::Write(r) => Ok(Response::new(r.clone())),
                _ => Ok(Response::new(WriteResponse {
                    tag_id: String::new(),
                    success: true,
                    error: None,
                })),
            }
        }
    }

    async fn start_mock_server(response: MockResponse) -> String {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let svc = MockBridgeService { response };
        tokio::spawn(async move {
            Server::builder()
                .add_service(BridgeServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("127.0.0.1:{}", port)
    }

    #[tokio::test]
    async fn test_run_command_servers() {
        let host = start_mock_server(MockResponse::ListServers(ListServersResponse {
            servers: vec![],
        }))
        .await;
        let cli = Cli {
            host,
            command: Commands::Servers,
        };
        run_command(cli).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_command_browse() {
        let host = start_mock_server(MockResponse::BrowseResponses(vec![])).await;
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
        let host = start_mock_server(MockResponse::Read(ReadResponse { values: vec![] })).await;
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
        let host = start_mock_server(MockResponse::Write(WriteResponse {
            tag_id: "t".into(),
            success: true,
            error: None,
        }))
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
        let host = start_mock_server(MockResponse::BrowseResponses(
            (0..300)
                .map(|i| BrowseResponse {
                    tag_id: format!("tag{}", i),
                    node_type: "Leaf".into(),
                })
                .collect(),
        ))
        .await;
        let mut client = BridgeClient::connect(format!("http://{}", host))
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
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.host, "localhost:7600");
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
        unsafe { std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888") };
        let args = Cli::try_parse_from(["opcda-bridge", "servers"]).unwrap();
        assert_eq!(args.host, "envhost:8888");
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
    }

    #[test]
    fn test_cli_arg_overrides_env() {
        unsafe { std::env::set_var("OPC_BRIDGE_HOST", "envhost:8888") };
        let args =
            Cli::try_parse_from(["opcda-bridge", "--host", "arghost:7777", "servers"]).unwrap();
        assert_eq!(args.host, "arghost:7777");
        unsafe { std::env::remove_var("OPC_BRIDGE_HOST") };
    }
}
