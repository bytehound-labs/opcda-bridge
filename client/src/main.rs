#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    opcda_bridge_client::cli::run_command(opcda_bridge_client::cli::Cli::parse()).await
}
