mod commands;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "opcda-bridge", about = "OPC DA bridge client", version)]
struct Cli {
    #[arg(long, env = "OPC_BRIDGE_HOST", default_value = "localhost:7600")]
    host: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Servers => commands::cmd_servers(cli.host).await?,
        Commands::Browse { server, flat } => commands::cmd_browse(cli.host, server, flat).await?,
        Commands::Read { server, tags } => commands::cmd_read(cli.host, server, tags).await?,
        Commands::Write { server, tag, value } => {
            commands::cmd_write(cli.host, server, tag, value).await?
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::sync::Mutex;

    // std::env::set_var/remove_var mutate process-global state, but `cargo
    // test` runs tests in parallel threads by default, so the tests below
    // that touch OPC_BRIDGE_HOST race with each other unless serialized.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

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
        match args.command {
            Commands::Browse { server, flat } => {
                assert_eq!(server, "MyServer");
                assert!(flat);
            }
            _ => panic!("expected Browse"),
        }
    }

    #[test]
    fn test_cli_browse_no_flat() {
        let args = Cli::try_parse_from(["opcda-bridge", "browse", "--server", "MyServer"]).unwrap();
        match args.command {
            Commands::Browse { server, flat } => {
                assert_eq!(server, "MyServer");
                assert!(!flat);
            }
            _ => panic!("expected Browse"),
        }
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
        match args.command {
            Commands::Read { server, tags } => {
                assert_eq!(server, "MyServer");
                assert_eq!(tags, vec!["tag1", "tag2", "tag3"]);
            }
            _ => panic!("expected Read"),
        }
    }

    #[test]
    fn test_cli_read_no_tags() {
        let args = Cli::try_parse_from(["opcda-bridge", "read", "--server", "MyServer"]).unwrap();
        match args.command {
            Commands::Read { server, tags } => {
                assert_eq!(server, "MyServer");
                assert!(tags.is_empty());
            }
            _ => panic!("expected Read"),
        }
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
        match args.command {
            Commands::Write { server, tag, value } => {
                assert_eq!(server, "MyServer");
                assert_eq!(tag, "Tag1");
                assert_eq!(value, "42");
            }
            _ => panic!("expected Write"),
        }
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
