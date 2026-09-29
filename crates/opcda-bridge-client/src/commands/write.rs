use crate::output::{self, OutputFormat};
use opcda_bridge::{Client, parse_value};
use serde::Serialize;
use tabled::Tabled;
use tabled::derive::display;

#[derive(Tabled, Serialize)]
struct WriteRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Success")]
    success: bool,
    #[tabled(rename = "Error", display("display::option", ""))]
    error: Option<String>,
}

pub async fn cmd_write(
    host: String,
    server: String,
    tag: String,
    value: String,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let parsed = parse_value(&value);
    let mut client = Client::connect(&host).await?;
    let result = client.write(server, tag, parsed).await?;
    let rendered = output::render(
        vec![WriteRow {
            tag_id: result.tag_id,
            success: result.success,
            error: result.error,
        }],
        format,
    )
    .expect("write rows contain only infallibly serializable scalar fields");
    println!("{rendered}");
    Ok(())
}
