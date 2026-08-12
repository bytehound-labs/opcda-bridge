#[tokio::main]
async fn main() -> std::process::ExitCode {
    opcda_bridge_client::run().await
}
