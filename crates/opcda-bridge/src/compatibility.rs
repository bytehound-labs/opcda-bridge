//! Protocol compatibility discovery and evaluation.

use crate::{Capabilities, Error, Result};
use opcda_bridge_proto::bridge as proto;
use serde::Serialize;
use std::fmt;

/// A protocol surface whose versions are negotiated independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityFeature {
    Core,
    Namespace,
    IndexedSearch,
}

impl CompatibilityFeature {
    fn from_proto(value: i32) -> Result<Self> {
        match proto::ProtocolFeatureKind::try_from(value).map_err(|_| {
            Error::Protocol(format!("gateway returned unknown protocol feature {value}"))
        })? {
            proto::ProtocolFeatureKind::Core => Ok(Self::Core),
            proto::ProtocolFeatureKind::Namespace => Ok(Self::Namespace),
            proto::ProtocolFeatureKind::IndexedSearch => Ok(Self::IndexedSearch),
            proto::ProtocolFeatureKind::Unspecified => Err(Error::Protocol(
                "gateway returned an unspecified protocol feature".into(),
            )),
        }
    }
}

impl fmt::Display for CompatibilityFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Core => "core",
            Self::Namespace => "namespace",
            Self::IndexedSearch => "indexed-search",
        })
    }
}

/// Inclusive protocol version range supported by one component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProtocolVersionRange {
    pub min: u32,
    pub max: u32,
}

impl ProtocolVersionRange {
    /// Construct a range, rejecting reversed bounds.
    pub const fn new(min: u32, max: u32) -> Option<Self> {
        if min > max {
            None
        } else {
            Some(Self { min, max })
        }
    }

    /// Construct a range containing exactly one protocol version.
    pub const fn exact(version: u32) -> Self {
        Self {
            min: version,
            max: version,
        }
    }

    /// Return whether two ranges share at least one version.
    pub const fn overlaps(self, other: Self) -> bool {
        self.min <= other.max && other.min <= self.max
    }

    const fn negotiated_version(self, other: Self) -> Option<u32> {
        if self.overlaps(other) {
            Some(if self.min > other.min {
                self.min
            } else {
                other.min
            })
        } else {
            None
        }
    }
}

/// One feature and the versions supported by a gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProtocolFeatureSupport {
    pub feature: CompatibilityFeature,
    pub versions: ProtocolVersionRange,
}

/// Gateway-wide protocol information returned without contacting an OPC server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatewayInfo {
    pub application_version: String,
    pub compatibility_schema_version: u32,
    pub features: Vec<ProtocolFeatureSupport>,
}

impl TryFrom<proto::GetGatewayInfoResponse> for GatewayInfo {
    type Error = Error;

    fn try_from(value: proto::GetGatewayInfoResponse) -> Result<Self> {
        let features = value
            .features
            .into_iter()
            .map(|feature| {
                let feature_kind = CompatibilityFeature::from_proto(feature.kind)?;
                let versions = ProtocolVersionRange::new(feature.min_version, feature.max_version)
                    .ok_or_else(|| {
                        Error::Protocol(format!(
                            "gateway returned reversed {feature_kind} protocol version range"
                        ))
                    })?;
                Ok(ProtocolFeatureSupport {
                    feature: feature_kind,
                    versions,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            application_version: value.application_version,
            compatibility_schema_version: value.compatibility_schema_version,
            features,
        })
    }
}

/// Where a gateway compatibility profile came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilitySource {
    GatewayInfo,
    LegacyCapabilities,
    Unknown,
}

impl fmt::Display for CompatibilitySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::GatewayInfo => "gateway-info",
            Self::LegacyCapabilities => "legacy-capabilities",
            Self::Unknown => "unknown",
        })
    }
}

/// A component's advertised protocol profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProtocolProfile {
    pub application_version: Option<String>,
    pub source: CompatibilitySource,
    pub features: Vec<ProtocolFeatureSupport>,
}

impl ProtocolProfile {
    /// Build a profile from the gateway-wide handshake.
    pub fn from_gateway_info(info: &GatewayInfo) -> Self {
        Self {
            application_version: Some(info.application_version.clone()),
            source: CompatibilitySource::GatewayInfo,
            features: info.features.clone(),
        }
    }

    fn feature(&self, feature: CompatibilityFeature) -> Option<ProtocolVersionRange> {
        self.features
            .iter()
            .find(|support| support.feature == feature)
            .map(|support| support.versions)
    }
}

/// The current reusable client's protocol profile.
pub fn current_client_profile(application_version: impl Into<String>) -> ProtocolProfile {
    let application_version = application_version.into();
    let release_line =
        opcda_bridge_proto::compatibility::release_line_for(env!("CARGO_PKG_VERSION"))
            .expect("reusable client package version must be in the compatibility catalog");
    ProtocolProfile {
        application_version: Some(application_version),
        source: CompatibilitySource::GatewayInfo,
        features: vec![
            ProtocolFeatureSupport {
                feature: CompatibilityFeature::Core,
                versions: ProtocolVersionRange::exact(release_line.core_protocol),
            },
            ProtocolFeatureSupport {
                feature: CompatibilityFeature::Namespace,
                versions: ProtocolVersionRange::exact(release_line.namespace_protocol),
            },
            ProtocolFeatureSupport {
                feature: CompatibilityFeature::IndexedSearch,
                versions: ProtocolVersionRange::exact(release_line.indexed_search_protocol),
            },
        ],
    }
}

/// Convert legacy per-server capabilities into a gateway profile.
pub fn legacy_gateway_profile(capabilities: &Capabilities) -> ProtocolProfile {
    let mut features = Vec::new();
    let release_line =
        opcda_bridge_proto::compatibility::release_line_for(&capabilities.application_version);
    if let Some(namespace_version) = parse_namespace_protocol(&capabilities.protocol_version) {
        features.push(ProtocolFeatureSupport {
            feature: CompatibilityFeature::Core,
            versions: ProtocolVersionRange::exact(release_line.map_or(
                opcda_bridge_proto::compatibility::CORE_PROTOCOL_VERSION,
                |line| line.core_protocol,
            )),
        });
        features.push(ProtocolFeatureSupport {
            feature: CompatibilityFeature::Namespace,
            versions: ProtocolVersionRange::exact(namespace_version),
        });
    }
    if capabilities.supports_indexed_search
        && let Some(index_protocol) =
            parse_index_protocol(&capabilities.indexed_search_protocol_version)
    {
        features.push(ProtocolFeatureSupport {
            feature: CompatibilityFeature::IndexedSearch,
            versions: ProtocolVersionRange::exact(index_protocol),
        });
    }
    ProtocolProfile {
        application_version: Some(capabilities.application_version.clone()),
        source: CompatibilitySource::LegacyCapabilities,
        features,
    }
}

fn catalog_line(version: &str) -> Option<&'static str> {
    opcda_bridge_proto::compatibility::release_line_for(version).map(|line| line.name)
}

fn evidence_status(value: &str) -> Option<CompatibilityEvidence> {
    match value {
        "contract-boundary-tested" => Some(CompatibilityEvidence::ContractBoundaryTested),
        "exact-pair-tested" => Some(CompatibilityEvidence::ExactPairTested),
        "unverified" => Some(CompatibilityEvidence::Unverified),
        _ => None,
    }
}

fn catalog_evidence(
    client_version: Option<&str>,
    gateway_version: Option<&str>,
) -> CompatibilityEvidence {
    let (Some(client_version), Some(gateway_version)) = (client_version, gateway_version) else {
        return CompatibilityEvidence::Unverified;
    };
    let (Some(client_line), Some(gateway_line)) =
        (catalog_line(client_version), catalog_line(gateway_version))
    else {
        return CompatibilityEvidence::Unverified;
    };

    for &(catalog_client_line, catalog_gateway_line, status, exact_client, exact_gateway) in
        opcda_bridge_proto::compatibility::EVIDENCE
    {
        if catalog_client_line == client_line
            && catalog_gateway_line == gateway_line
            && !exact_client.is_empty()
            && exact_client == client_version
            && exact_gateway == gateway_version
            && let Some(status) = evidence_status(status)
        {
            return status;
        }
    }
    for &(catalog_client_line, catalog_gateway_line, status, exact_client, exact_gateway) in
        opcda_bridge_proto::compatibility::EVIDENCE
    {
        if catalog_client_line == client_line
            && catalog_gateway_line == gateway_line
            && exact_client.is_empty()
            && exact_gateway.is_empty()
            && let Some(status) = evidence_status(status)
        {
            return status;
        }
    }
    CompatibilityEvidence::Unverified
}

fn parse_namespace_protocol(value: &str) -> Option<u32> {
    match value.trim() {
        "1" | "1.0" => Some(1),
        "2" | "2.0" => Some(2),
        "0.3" | "0.3.0" => Some(2),
        _ => None,
    }
}

fn parse_index_protocol(value: &str) -> Option<u32> {
    match value.trim() {
        "1" | "1.0" => Some(1),
        _ => None,
    }
}

/// Result for one feature comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeatureCompatibility {
    pub feature: CompatibilityFeature,
    pub status: FeatureCompatibilityStatus,
    pub client_versions: ProtocolVersionRange,
    pub gateway_versions: Option<ProtocolVersionRange>,
    pub negotiated_version: Option<u32>,
    pub reason: String,
}

/// Result status for one protocol feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureCompatibilityStatus {
    Compatible,
    Unsupported,
    Incompatible,
    Unknown,
}

impl fmt::Display for FeatureCompatibilityStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Compatible => "compatible",
            Self::Unsupported => "unsupported",
            Self::Incompatible => "incompatible",
            Self::Unknown => "unknown",
        })
    }
}

/// Overall result of comparing a client and gateway profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityStatus {
    Full,
    Partial,
    Incompatible,
    Unknown,
}

impl fmt::Display for CompatibilityStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full => "full",
            Self::Partial => "partial",
            Self::Incompatible => "incompatible",
            Self::Unknown => "unknown",
        })
    }
}

/// Evidence status for an otherwise protocol-compatible pairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityEvidence {
    ContractBoundaryTested,
    ExactPairTested,
    Unverified,
}

impl fmt::Display for CompatibilityEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ContractBoundaryTested => "contract-boundary-tested",
            Self::ExactPairTested => "exact-pair-tested",
            Self::Unverified => "unverified",
        })
    }
}

/// Full compatibility report for one connected gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompatibilityReport {
    pub client_version: String,
    pub library_version: String,
    pub gateway_version: Option<String>,
    pub source: CompatibilitySource,
    pub status: CompatibilityStatus,
    pub evidence: CompatibilityEvidence,
    pub features: Vec<FeatureCompatibility>,
}

impl CompatibilityReport {
    /// Return the result for one feature, if the report contains it.
    pub fn feature(&self, feature: CompatibilityFeature) -> Option<&FeatureCompatibility> {
        self.features
            .iter()
            .find(|result| result.feature == feature)
    }

    /// Return whether every requested feature negotiated successfully.
    pub fn satisfies(&self, required: &[CompatibilityFeature]) -> bool {
        required.iter().all(|feature| {
            self.feature(*feature)
                .is_some_and(|result| result.status == FeatureCompatibilityStatus::Compatible)
        })
    }
}

/// Compare one client profile against one gateway profile.
pub fn evaluate_compatibility(
    client: &ProtocolProfile,
    gateway: &ProtocolProfile,
) -> CompatibilityReport {
    let mut features = Vec::new();
    for client_support in &client.features {
        let gateway_versions = gateway.feature(client_support.feature);
        let (status, negotiated_version, reason) = match gateway_versions {
            Some(gateway_versions) if client_support.versions.overlaps(gateway_versions) => (
                FeatureCompatibilityStatus::Compatible,
                client_support.versions.negotiated_version(gateway_versions),
                format!(
                    "{feature} protocol ranges overlap",
                    feature = client_support.feature
                ),
            ),
            Some(gateway_versions) => (
                FeatureCompatibilityStatus::Incompatible,
                None,
                format!(
                    "{feature} protocol ranges do not overlap: client {}-{}, gateway {}-{}",
                    client_support.versions.min,
                    client_support.versions.max,
                    gateway_versions.min,
                    gateway_versions.max,
                    feature = client_support.feature
                ),
            ),
            None if client_support.feature == CompatibilityFeature::IndexedSearch => (
                FeatureCompatibilityStatus::Unsupported,
                None,
                "gateway does not advertise indexed search".into(),
            ),
            None => (
                FeatureCompatibilityStatus::Unknown,
                None,
                format!(
                    "gateway did not advertise the {feature} protocol",
                    feature = client_support.feature
                ),
            ),
        };
        features.push(FeatureCompatibility {
            feature: client_support.feature,
            status,
            client_versions: client_support.versions,
            gateway_versions,
            negotiated_version,
            reason,
        });
    }

    let core_status = features
        .iter()
        .find(|result| result.feature == CompatibilityFeature::Core)
        .map(|result| result.status);
    let status = match core_status {
        Some(FeatureCompatibilityStatus::Compatible) => {
            if features
                .iter()
                .all(|result| result.status == FeatureCompatibilityStatus::Compatible)
            {
                CompatibilityStatus::Full
            } else {
                CompatibilityStatus::Partial
            }
        }
        Some(FeatureCompatibilityStatus::Incompatible)
        | Some(FeatureCompatibilityStatus::Unsupported) => CompatibilityStatus::Incompatible,
        Some(FeatureCompatibilityStatus::Unknown) | None => CompatibilityStatus::Unknown,
    };

    CompatibilityReport {
        client_version: client
            .application_version
            .clone()
            .unwrap_or_else(|| "unknown".into()),
        library_version: env!("CARGO_PKG_VERSION").into(),
        gateway_version: gateway.application_version.clone(),
        source: gateway.source,
        status,
        evidence: catalog_evidence(
            client.application_version.as_deref(),
            gateway.application_version.as_deref(),
        ),
        features,
    }
}

/// Build an unknown report when an old gateway cannot answer either handshake.
pub fn unknown_compatibility_report(client_version: impl Into<String>) -> CompatibilityReport {
    CompatibilityReport {
        client_version: client_version.into(),
        library_version: env!("CARGO_PKG_VERSION").into(),
        gateway_version: None,
        source: CompatibilitySource::Unknown,
        status: CompatibilityStatus::Unknown,
        evidence: CompatibilityEvidence::Unverified,
        features: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities(protocol: &str, indexed: bool) -> Capabilities {
        Capabilities {
            application_version: "0.4.3".into(),
            protocol_version: protocol.into(),
            max_page_size: 200,
            supports_browse_sessions: true,
            supports_search: true,
            organization: crate::NamespaceOrganization::Hierarchical,
            source: crate::BrowseSource::Da2,
            supports_indexed_search: indexed,
            indexed_search_protocol_version: if indexed { "1" } else { "" }.into(),
            max_indexed_search_results: 50,
            search_index_state: crate::SearchIndexState::Ready,
        }
    }

    #[test]
    fn ranges_validate_and_negotiate() {
        assert!(ProtocolVersionRange::new(2, 1).is_none());
        let left = ProtocolVersionRange::new(1, 3).unwrap();
        let right = ProtocolVersionRange::new(3, 4).unwrap();
        assert!(left.overlaps(right));
        assert_eq!(left.negotiated_version(right), Some(3));
        assert!(!ProtocolVersionRange::exact(1).overlaps(ProtocolVersionRange::exact(2)));
    }

    #[test]
    fn feature_display_and_status_display_are_stable() {
        assert_eq!(
            CompatibilityFeature::IndexedSearch.to_string(),
            "indexed-search"
        );
        assert_eq!(
            FeatureCompatibilityStatus::Unsupported.to_string(),
            "unsupported"
        );
        assert_eq!(CompatibilityStatus::Partial.to_string(), "partial");
        assert_eq!(
            CompatibilityEvidence::ContractBoundaryTested.to_string(),
            "contract-boundary-tested"
        );
    }

    #[test]
    fn current_profile_uses_generated_contract_versions() {
        let profile = current_client_profile("0.4.3");
        assert_eq!(
            profile.feature(CompatibilityFeature::Core),
            Some(ProtocolVersionRange::exact(1))
        );
        assert_eq!(
            profile.feature(CompatibilityFeature::Namespace),
            Some(ProtocolVersionRange::exact(2))
        );
        assert_eq!(
            profile.feature(CompatibilityFeature::IndexedSearch),
            Some(ProtocolVersionRange::exact(1))
        );
        assert_eq!(profile.application_version.as_deref(), Some("0.4.3"));

        let relabeled = current_client_profile("application-build");
        assert_eq!(
            relabeled.feature(CompatibilityFeature::IndexedSearch),
            Some(ProtocolVersionRange::exact(1))
        );
    }

    #[test]
    fn legacy_profiles_map_supported_and_unknown_protocol_strings() {
        let profile = legacy_gateway_profile(&capabilities("2", true));
        assert_eq!(profile.source, CompatibilitySource::LegacyCapabilities);
        assert_eq!(
            profile.feature(CompatibilityFeature::Namespace),
            Some(ProtocolVersionRange::exact(2))
        );
        assert!(
            profile
                .feature(CompatibilityFeature::IndexedSearch)
                .is_some()
        );

        let old = legacy_gateway_profile(&capabilities("1.0", false));
        assert_eq!(
            old.feature(CompatibilityFeature::Namespace),
            Some(ProtocolVersionRange::exact(1))
        );
        assert!(old.feature(CompatibilityFeature::IndexedSearch).is_none());

        let shorthand = legacy_gateway_profile(&capabilities("0.3", false));
        assert_eq!(
            shorthand.feature(CompatibilityFeature::Namespace),
            Some(ProtocolVersionRange::exact(2))
        );

        let unknown = legacy_gateway_profile(&capabilities("future", false));
        assert!(unknown.features.is_empty());

        let mut unknown_version = capabilities("2", true);
        unknown_version.application_version = "future".into();
        let unknown_version_profile = legacy_gateway_profile(&unknown_version);
        assert_eq!(
            unknown_version_profile.feature(CompatibilityFeature::Core),
            Some(ProtocolVersionRange::exact(1))
        );
        assert_eq!(
            unknown_version_profile.feature(CompatibilityFeature::IndexedSearch),
            Some(ProtocolVersionRange::exact(1))
        );
        let mut invalid_index = capabilities("2", true);
        invalid_index.indexed_search_protocol_version = "future".into();
        assert!(
            legacy_gateway_profile(&invalid_index)
                .feature(CompatibilityFeature::IndexedSearch)
                .is_none()
        );
    }

    #[test]
    fn evaluates_full_partial_and_incompatible_profiles() {
        let client = current_client_profile("0.4.3");
        let full = evaluate_compatibility(
            &client,
            &ProtocolProfile {
                application_version: Some("0.4.3".into()),
                source: CompatibilitySource::GatewayInfo,
                features: client.features.clone(),
            },
        );
        assert_eq!(full.status, CompatibilityStatus::Full);
        assert_eq!(full.evidence, CompatibilityEvidence::ExactPairTested);
        assert_eq!(full.library_version, env!("CARGO_PKG_VERSION"));
        assert!(full.satisfies(&[CompatibilityFeature::Core]));

        let partial = evaluate_compatibility(
            &client,
            &ProtocolProfile {
                application_version: Some("0.3.2".into()),
                source: CompatibilitySource::LegacyCapabilities,
                features: vec![
                    ProtocolFeatureSupport {
                        feature: CompatibilityFeature::Core,
                        versions: ProtocolVersionRange::exact(1),
                    },
                    ProtocolFeatureSupport {
                        feature: CompatibilityFeature::Namespace,
                        versions: ProtocolVersionRange::exact(2),
                    },
                ],
            },
        );
        assert_eq!(partial.status, CompatibilityStatus::Partial);
        assert_eq!(
            partial.evidence,
            CompatibilityEvidence::ContractBoundaryTested
        );
        assert!(!partial.satisfies(&[CompatibilityFeature::IndexedSearch]));

        let incompatible = evaluate_compatibility(
            &client,
            &ProtocolProfile {
                application_version: Some("future".into()),
                source: CompatibilitySource::GatewayInfo,
                features: vec![ProtocolFeatureSupport {
                    feature: CompatibilityFeature::Core,
                    versions: ProtocolVersionRange::exact(2),
                }],
            },
        );
        assert_eq!(incompatible.status, CompatibilityStatus::Incompatible);
        assert_eq!(
            incompatible
                .feature(CompatibilityFeature::Core)
                .unwrap()
                .status,
            FeatureCompatibilityStatus::Incompatible
        );

        let unknown = evaluate_compatibility(
            &client,
            &ProtocolProfile {
                application_version: Some("0.4.3".into()),
                source: CompatibilitySource::GatewayInfo,
                features: Vec::new(),
            },
        );
        assert_eq!(unknown.status, CompatibilityStatus::Unknown);
        assert!(!unknown.satisfies(&[CompatibilityFeature::Core]));
    }

    #[test]
    fn evaluates_unknown_and_converts_gateway_info() {
        let unknown = unknown_compatibility_report("0.4.3");
        assert_eq!(unknown.status, CompatibilityStatus::Unknown);
        assert!(!unknown.satisfies(&[CompatibilityFeature::Core]));

        let info = GatewayInfo::try_from(proto::GetGatewayInfoResponse {
            application_version: "0.4.3".into(),
            compatibility_schema_version: 1,
            features: vec![proto::ProtocolFeature {
                kind: proto::ProtocolFeatureKind::Core as i32,
                min_version: 1,
                max_version: 2,
            }],
        })
        .unwrap();
        assert_eq!(info.features[0].versions.max, 2);
    }

    #[test]
    fn rejects_invalid_gateway_feature_data() {
        let error = GatewayInfo::try_from(proto::GetGatewayInfoResponse {
            features: vec![proto::ProtocolFeature {
                kind: 99,
                min_version: 1,
                max_version: 1,
            }],
            ..Default::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("unknown protocol feature"));

        let error = GatewayInfo::try_from(proto::GetGatewayInfoResponse {
            features: vec![proto::ProtocolFeature {
                kind: proto::ProtocolFeatureKind::Core as i32,
                min_version: 2,
                max_version: 1,
            }],
            ..Default::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("reversed core"));

        let error = GatewayInfo::try_from(proto::GetGatewayInfoResponse {
            features: vec![proto::ProtocolFeature {
                kind: proto::ProtocolFeatureKind::Unspecified as i32,
                min_version: 1,
                max_version: 1,
            }],
            ..Default::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("unspecified protocol feature"));
    }

    #[test]
    fn catalog_evidence_handles_exact_boundary_and_unknown_versions() {
        assert_eq!(catalog_line("0.3.1"), Some("legacy"));
        assert_eq!(catalog_line("0.3.2"), Some("paged"));
        assert_eq!(catalog_line("0.4.0"), Some("indexed"));
        assert_eq!(catalog_line("1.0.0"), None);
        assert_eq!(
            catalog_evidence(Some("0.4.0"), Some("0.4.3")),
            CompatibilityEvidence::ExactPairTested
        );
        assert_eq!(
            catalog_evidence(Some("0.4.3"), Some("0.3.2")),
            CompatibilityEvidence::ContractBoundaryTested
        );
        assert_eq!(
            catalog_evidence(Some("0.3.2"), Some("0.4.3")),
            CompatibilityEvidence::ContractBoundaryTested
        );
        assert_eq!(
            catalog_evidence(Some("0.3.1"), Some("0.3.1")),
            CompatibilityEvidence::Unverified
        );
        assert_eq!(
            catalog_evidence(Some("future"), Some("0.4.3")),
            CompatibilityEvidence::Unverified
        );
        assert_eq!(
            catalog_evidence(None, Some("0.4.3")),
            CompatibilityEvidence::Unverified
        );
        assert!(catalog_line("0.4").is_none());
        assert!(catalog_line("0.x.3").is_none());
        assert_eq!(evidence_status("invalid"), None);
    }
}
