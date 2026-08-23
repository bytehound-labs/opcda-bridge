//! Output format selection and rendering for client command results.
//!
//! Simple commands build row structs and route them through [`render`].
//! Browse uses a metadata-bearing JSON object, while search uses
//! newline-delimited JSON events so progressive results remain streaming.

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use tabled::{Table, Tabled};

/// How a command's result is printed.
#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Human-readable table (default).
    Table,
    /// Machine-readable JSON. Search emits newline-delimited event objects.
    Json,
}

/// Render a command's rows in the requested format.
pub fn render<T: Tabled + Serialize>(rows: Vec<T>, format: OutputFormat) -> anyhow::Result<String> {
    match format {
        OutputFormat::Table => Ok(Table::new(rows).to_string()),
        OutputFormat::Json => Ok(serde_json::to_string_pretty(&rows)?),
    }
}

/// Format an error for display, matching the requested output format.
///
/// The `Table` branch reproduces Rust's default `Termination` behavior for
/// `Err` (`"Error: {:?}"`, the `Debug` chain anyhow builds), so plain-table
/// users see the same error text as before this flag existed. The `Json`
/// branch emits `{"error": "<message>"}` so scripted consumers never have
/// to parse free-text stderr.
pub fn format_error(err: &anyhow::Error, format: OutputFormat) -> String {
    match format {
        OutputFormat::Table => format!("Error: {err:?}"),
        OutputFormat::Json => {
            let payload = serde_json::json!({ "error": err.to_string() });
            serde_json::to_string_pretty(&payload)
                .unwrap_or_else(|_| format!("{{\"error\": \"{err}\"}}"))
        }
    }
}

/// Extract the output format from CLI-only sources: `--json` (which wins if
/// both are set) or `--output` (which already folds in `OPC_BRIDGE_OUTPUT`
/// via clap's `env` attribute). Returns `None` if neither was given, so the
/// caller can still fall back to a config file.
///
/// Kept CLI-only (no config file access) because config loading can itself
/// fail, and an error at that stage must still be reportable in *some*
/// format before the config file's `output` key could ever be known.
pub fn resolve_from_cli(cli: &crate::cli::Cli) -> Option<OutputFormat> {
    if cli.json {
        Some(OutputFormat::Json)
    } else {
        cli.output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Tabled, Serialize)]
    struct Row {
        name: String,
        count: u32,
    }

    #[test]
    fn test_render_table() {
        let rows = vec![Row {
            name: "a".into(),
            count: 1,
        }];
        let out = render(rows, OutputFormat::Table).unwrap();
        assert!(out.contains("name"));
        assert!(out.contains("a"));
    }

    #[test]
    fn test_render_json_shape_and_keys() {
        let rows = vec![Row {
            name: "a".into(),
            count: 1,
        }];
        let out = render(rows, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "a");
        assert_eq!(arr[0]["count"], 1);
    }

    #[test]
    fn test_render_json_empty_is_bare_array_not_null() {
        let rows: Vec<Row> = vec![];
        let out = render(rows, OutputFormat::Json).unwrap();
        assert_eq!(out, "[]");
    }

    #[test]
    fn test_render_json_is_pretty_printed() {
        let rows = vec![Row {
            name: "a".into(),
            count: 1,
        }];
        let out = render(rows, OutputFormat::Json).unwrap();
        assert!(out.contains('\n'), "expected multi-line pretty JSON");
    }

    #[test]
    fn test_format_error_table_matches_debug_chain() {
        let err = anyhow::anyhow!("boom");
        let out = format_error(&err, OutputFormat::Table);
        assert_eq!(out, format!("Error: {err:?}"));
    }

    #[test]
    fn test_format_error_json_is_valid_json_with_message() {
        let err = anyhow::anyhow!("boom");
        let out = format_error(&err, OutputFormat::Json);
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["error"], "boom");
    }

    #[test]
    fn test_output_format_deserialize_lowercase() {
        #[derive(Deserialize)]
        struct Wrapper {
            output: OutputFormat,
        }
        let w: Wrapper = toml::from_str("output = \"json\"").unwrap();
        assert_eq!(w.output, OutputFormat::Json);
        let w: Wrapper = toml::from_str("output = \"table\"").unwrap();
        assert_eq!(w.output, OutputFormat::Table);
    }

    proptest::proptest! {
        #[test]
        fn prop_json_errors_are_parseable(message in any::<String>()) {
            let err = anyhow::anyhow!(message);
            let rendered = format_error(&err, OutputFormat::Json);
            prop_assert!(serde_json::from_str::<serde_json::Value>(&rendered).is_ok());
        }
    }
}
