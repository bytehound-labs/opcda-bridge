use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/bridge.proto");
    tonic_prost_build::compile_protos("proto/bridge.proto")?;

    println!("cargo:rerun-if-changed=compatibility.toml");
    let catalog = fs::read_to_string("compatibility.toml")?;
    let document: toml::Value = toml::from_str(&catalog)?;
    let protocol = document
        .get("protocol")
        .and_then(toml::Value::as_table)
        .ok_or("compatibility.toml is missing [protocol]")?;
    let schema_version = required_integer(&document, "schema_version")?;
    let catalog_version = document
        .get("catalog_version")
        .and_then(toml::Value::as_str)
        .ok_or("compatibility.toml is missing catalog_version")?;
    let core = required_protocol_integer(protocol, "core")?;
    let namespace = required_protocol_integer(protocol, "namespace")?;
    let indexed_search = required_protocol_integer(protocol, "indexed_search")?;
    let release_lines = document
        .get("release_lines")
        .and_then(toml::Value::as_array)
        .ok_or("compatibility.toml is missing release_lines")?;
    let evidence = document
        .get("evidence")
        .and_then(toml::Value::as_array)
        .ok_or("compatibility.toml is missing evidence")?;
    let mut generated = format!(
        "pub const SCHEMA_VERSION: u32 = {schema_version};\n\
         pub const CATALOG_VERSION: &str = {catalog_version:?};\n\
         pub const CORE_PROTOCOL_VERSION: u32 = {core};\n\
         pub const NAMESPACE_PROTOCOL_VERSION: u32 = {namespace};\n\
         pub const INDEXED_SEARCH_PROTOCOL_VERSION: u32 = {indexed_search};\n\
         pub const RELEASE_LINES: &[ReleaseLine] = &[\n"
    );
    for line in release_lines {
        let table = line.as_table().ok_or("release_lines must contain tables")?;
        let name = required_string(table, "name")?;
        let min_version = required_string(table, "min_version")?;
        let max_version = required_string(table, "max_version")?;
        let status = required_string(table, "status")?;
        let core_protocol = required_table_integer(table, "core_protocol")?;
        let namespace_protocol = required_table_integer(table, "namespace_protocol")?;
        let indexed_search_protocol = required_table_integer(table, "indexed_search_protocol")?;
        generated.push_str(&format!(
            "    ReleaseLine {{ name: {name:?}, min_version: {min_version:?}, \
             max_version: {max_version:?}, status: {status:?}, core_protocol: {core_protocol}, \
             namespace_protocol: {namespace_protocol}, \
             indexed_search_protocol: {indexed_search_protocol} }},\n"
        ));
    }
    generated.push_str(
        "];\n\
         pub const EVIDENCE: &[(&str, &str, &str, &str, &str)] = &[\n",
    );
    for entry in evidence {
        let table = entry.as_table().ok_or("evidence must contain tables")?;
        let client_line = required_string(table, "client_line")?;
        let gateway_line = required_string(table, "gateway_line")?;
        let status = required_string(table, "status")?;
        let client_version = optional_string(table, "client_version")?.unwrap_or_default();
        let gateway_version = optional_string(table, "gateway_version")?.unwrap_or_default();
        generated.push_str(&format!(
            "    ({client_line:?}, {gateway_line:?}, {status:?}, {client_version:?}, \
             {gateway_version:?}),\n"
        ));
    }
    generated.push_str("];\n");
    let out = PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?)
        .join("compatibility_generated.rs");
    fs::write(out, generated)?;
    Ok(())
}

fn required_string(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    table
        .get(key)
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("compatibility.toml is missing valid {key}").into())
}

fn optional_string(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_owned()))
            .ok_or_else(|| format!("compatibility.toml has invalid {key}").into()),
    }
}

fn required_integer(document: &toml::Value, key: &str) -> Result<u32, Box<dyn std::error::Error>> {
    document
        .get(key)
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| format!("compatibility.toml is missing valid {key}").into())
}

fn required_protocol_integer(
    protocol: &toml::map::Map<String, toml::Value>,
    key: &str,
) -> Result<u32, Box<dyn std::error::Error>> {
    protocol
        .get(key)
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| format!("compatibility.toml is missing valid protocol.{key}").into())
}

fn required_table_integer(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
) -> Result<u32, Box<dyn std::error::Error>> {
    table
        .get(key)
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| format!("compatibility.toml is missing valid {key}").into())
}
