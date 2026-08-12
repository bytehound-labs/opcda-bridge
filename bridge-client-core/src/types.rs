//! Plain data types returned by [`crate::Client`]'s methods.
//!
//! These mirror the shapes `opcda-bridge-client`'s CLI row structs
//! (`ServerRow`, `TagRow`, `ReadRow`, `WriteRow` in that crate's
//! `commands.rs`) build from gRPC responses, but carry no presentation
//! concerns — no `Tabled`, no `Serialize` — so depending on this crate never
//! pulls `tabled` (or `clap`, `serde_json`, `toml`) in transitively.

/// A single node returned by [`crate::Client::browse`]: one tag or branch in
/// the OPC DA server's tag tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseNode {
    pub tag_id: String,
    pub node_type: String,
}

/// A single tag's value returned by [`crate::Client::read`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag_id: String,
    pub value: String,
    pub quality: String,
    pub timestamp: String,
}

/// The result of a single [`crate::Client::write`] call.
///
/// `error` is `Option<String>` rather than collapsing "no error" and "an
/// empty error string" into the same `""` value, the same distinction
/// `opcda-bridge-client`'s own `WriteRow.error` makes (see that crate's
/// `commands.rs`): whether the gateway reported an error at all is a fact
/// about the RPC result itself, not a presentation choice, so it belongs
/// here rather than being introduced only at the CLI's rendering layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResult {
    pub tag_id: String,
    pub success: bool,
    pub error: Option<String>,
}

/// A tag value to write, parsed from a raw string via [`parse_value`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

/// Parse a raw string into the most specific [`Value`] variant it matches:
/// `bool`, then `i32`, then `f64`, falling back to `String`.
///
/// Moved here from `opcda-bridge-client`'s `commands.rs` unchanged: this
/// coercion was never CLI-specific, and any async Rust consumer of
/// [`crate::Client::write`] (not only the CLI's `write` subcommand) needs
/// the identical bool/int/float/string inference to turn a plain string
/// into a typed [`Value`].
pub fn parse_value(raw: &str) -> Value {
    if let Ok(b) = raw.parse::<bool>() {
        return Value::Bool(b);
    }
    if let Ok(i) = raw.parse::<i32>() {
        return Value::Int(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return Value::Float(f);
    }
    Value::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_value_bool_true() {
        assert!(matches!(parse_value("true"), Value::Bool(true)));
    }

    #[test]
    fn test_parse_value_bool_false() {
        assert!(matches!(parse_value("false"), Value::Bool(false)));
    }

    #[test]
    fn test_parse_value_int_positive() {
        assert!(matches!(parse_value("42"), Value::Int(42)));
    }

    #[test]
    fn test_parse_value_int_negative() {
        assert!(matches!(parse_value("-1"), Value::Int(-1)));
    }

    #[test]
    fn test_parse_value_int_zero() {
        assert!(matches!(parse_value("0"), Value::Int(0)));
    }

    #[test]
    fn test_parse_value_float_positive() {
        assert!(matches!(parse_value("9.5"), Value::Float(v) if (v - 9.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_value_float_negative() {
        assert!(matches!(parse_value("-2.5"), Value::Float(v) if (v + 2.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_value_float_exponential() {
        assert!(matches!(parse_value("1e10"), Value::Float(v) if (v - 1e10).abs() < 1.0));
    }

    #[test]
    fn test_parse_value_string_simple() {
        assert!(matches!(parse_value("hello"), Value::String(s) if s == "hello"));
    }

    #[test]
    fn test_parse_value_string_empty() {
        assert!(matches!(parse_value(""), Value::String(s) if s.is_empty()));
    }

    #[test]
    fn test_parse_value_string_numeric_string() {
        assert!(matches!(parse_value("42foo"), Value::String(s) if s == "42foo"));
    }

    #[test]
    fn test_parse_value_string_special_chars() {
        assert!(matches!(parse_value("hello world!"), Value::String(s) if s == "hello world!"));
    }

    #[test]
    fn test_browse_node_fields() {
        let node = BrowseNode {
            tag_id: "Simulink".into(),
            node_type: "Branch".into(),
        };
        assert_eq!(node.tag_id, "Simulink");
        assert_eq!(node.node_type, "Branch");
    }

    #[test]
    fn test_tag_value_fields() {
        let value = TagValue {
            tag_id: "t1".into(),
            value: "42".into(),
            quality: "Good".into(),
            timestamp: "now".into(),
        };
        assert_eq!(value.tag_id, "t1");
        assert_eq!(value.value, "42");
        assert_eq!(value.quality, "Good");
        assert_eq!(value.timestamp, "now");
    }

    #[test]
    fn test_write_result_success_has_no_error() {
        let result = WriteResult {
            tag_id: "t1".into(),
            success: true,
            error: None,
        };
        assert!(result.success);
        assert_eq!(result.error, None);
    }

    #[test]
    fn test_write_result_failure_carries_error() {
        let result = WriteResult {
            tag_id: "t1".into(),
            success: false,
            error: Some("access denied".into()),
        };
        assert!(!result.success);
        assert_eq!(result.error, Some("access denied".to_string()));
    }
}
