/// A single tag's semantic value returned by [`crate::Client::read`].
///
/// For an OPC DA `VT_BSTR`, `value` contains the exact BSTR contents. The
/// bridge does not add or remove quote characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag_id: String,
    pub value: String,
    pub quality: String,
    pub timestamp: String,
}

/// The result of a single [`crate::Client::write`] call.
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

/// Parse a raw string into bool, integer, float, or string form.
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
