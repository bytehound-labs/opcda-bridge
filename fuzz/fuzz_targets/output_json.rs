#![no_main]

use libfuzzer_sys::fuzz_target;
use opcda_bridge_client::output::{OutputFormat, format_error};

fuzz_target!(|data: &[u8]| {
    let message = String::from_utf8_lossy(data).into_owned();
    let error = anyhow::Error::msg(message);
    let rendered = format_error(&error, OutputFormat::Json);
    assert!(serde_json::from_str::<serde_json::Value>(&rendered).is_ok());
});
