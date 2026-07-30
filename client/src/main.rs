mod commands;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "opcda-bridge", about = "OPC DA bridge client")]
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
