use crate::output::{self, OutputFormat};
use opcda_bridge::{Client, CompatibilityFeature, CompatibilityReport, FeatureCompatibilityStatus};
use serde::Serialize;
use std::fmt::Write as _;
use tabled::Tabled;

#[derive(Tabled, Serialize)]
struct CompatibilityRow {
    #[tabled(rename = "Client")]
    client_version: String,
    #[tabled(rename = "Library")]
    library_version: String,
    #[tabled(rename = "Gateway")]
    gateway_version: String,
    #[tabled(rename = "Source")]
    source: String,
    #[tabled(rename = "Overall")]
    status: String,
    #[tabled(rename = "Evidence")]
    evidence: String,
    #[tabled(rename = "Feature")]
    feature: String,
    #[tabled(rename = "Feature Status")]
    feature_status: String,
    #[tabled(rename = "Client Versions")]
    client_versions: String,
    #[tabled(rename = "Gateway Versions")]
    gateway_versions: String,
    #[tabled(rename = "Negotiated")]
    negotiated_version: String,
    #[tabled(rename = "Reason")]
    reason: String,
}

pub(super) fn version_range(range: Option<opcda_bridge::ProtocolVersionRange>) -> String {
    match range {
        Some(range) if range.min == range.max => range.min.to_string(),
        Some(range) => format!("{}-{}", range.min, range.max),
        None => "unknown".into(),
    }
}

pub(super) fn render_compatibility(
    report: &CompatibilityReport,
    format: OutputFormat,
) -> anyhow::Result<String> {
    if format == OutputFormat::Json {
        return Ok(serde_json::to_string_pretty(report)?);
    }

    let rows = if report.features.is_empty() {
        vec![CompatibilityRow {
            client_version: report.client_version.clone(),
            library_version: report.library_version.clone(),
            gateway_version: report
                .gateway_version
                .as_deref()
                .unwrap_or("unknown")
                .into(),
            source: report.source.to_string(),
            status: report.status.to_string(),
            evidence: report.evidence.to_string(),
            feature: "none".into(),
            feature_status: "unknown".into(),
            client_versions: "unknown".into(),
            gateway_versions: "unknown".into(),
            negotiated_version: "none".into(),
            reason: "gateway did not provide a compatibility profile".into(),
        }]
    } else {
        report
            .features
            .iter()
            .map(|feature| CompatibilityRow {
                client_version: report.client_version.clone(),
                library_version: report.library_version.clone(),
                gateway_version: report
                    .gateway_version
                    .as_deref()
                    .unwrap_or("unknown")
                    .into(),
                source: report.source.to_string(),
                status: report.status.to_string(),
                evidence: report.evidence.to_string(),
                feature: feature.feature.to_string(),
                feature_status: feature.status.to_string(),
                client_versions: version_range(Some(feature.client_versions)),
                gateway_versions: version_range(feature.gateway_versions),
                negotiated_version: feature
                    .negotiated_version
                    .map_or_else(|| "none".into(), |version| version.to_string()),
                reason: feature.reason.clone(),
            })
            .collect()
    };
    output::render(rows, format)
}

/// Print the negotiated compatibility profile and enforce requested features.
pub async fn cmd_compatibility(
    host: String,
    server: Option<String>,
    required: Vec<CompatibilityFeature>,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let report = client
        .compatibility_with_client_version(server.as_deref(), env!("CARGO_PKG_VERSION"))
        .await?;
    println!("{}", render_compatibility(&report, format)?);

    let required = if required.is_empty() {
        vec![CompatibilityFeature::Core]
    } else {
        required
    };
    let failures = required
        .iter()
        .filter(|feature| {
            report
                .feature(**feature)
                .is_none_or(|result| result.status != FeatureCompatibilityStatus::Compatible)
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if failures.is_empty() {
        return Ok(());
    }
    let mut message = String::from("gateway compatibility check failed for ");
    let _ = write!(message, "{}", failures.join(", "));
    Err(anyhow::anyhow!(message))
}
