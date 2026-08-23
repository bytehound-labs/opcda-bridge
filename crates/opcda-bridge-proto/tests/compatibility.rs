use opcda_bridge_proto::bridge::{
    BrowseNode, BrowseNodeKind, BrowsePage, BrowseRequest, BrowseSource, GetCapabilitiesResponse,
    IndexedSearchMatch, NamespaceOrganization, ReadRequest, SearchIndexResponse, SearchIndexState,
    SearchIndexStatus, WriteResponse,
};
use prost::Message;

#[test]
fn round_trips_paged_browse_request_and_response() {
    let request = BrowseRequest {
        server: "S".into(),
        session_id: Some("session".into()),
        parent_node_key: Some("parent".into()),
        page_token: Some("page".into()),
        page_size: 200,
        refresh: true,
    };
    let encoded_request = request.encode_to_vec();
    let decoded_request = BrowseRequest::decode(encoded_request.as_slice()).unwrap();
    assert_eq!(decoded_request, request);

    let response = BrowsePage {
        session_id: "session".into(),
        nodes: vec![BrowseNode {
            node_key: "node".into(),
            display_name: "PV".into(),
            kind: BrowseNodeKind::BranchAndItem as i32,
            item_id: Some("FCS0201!204FI00510.PV".into()),
        }],
        next_page_token: Some("next".into()),
        complete: false,
        organization: NamespaceOrganization::Hierarchical as i32,
        source: BrowseSource::Da2 as i32,
        warning: None,
    };
    let encoded_response = response.encode_to_vec();
    let decoded_response = BrowsePage::decode(encoded_response.as_slice()).unwrap();
    assert_eq!(decoded_response, response);
}

#[test]
fn rejects_the_old_streaming_browse_wire_shape() {
    let old_payload = [0x0a, 0x01, b'S', 0x10, 0x01, 0x1a, 0x03, b'A', b'/', b'B'];
    assert!(BrowseRequest::decode(old_payload.as_slice()).is_err());
}

#[test]
fn decodes_unchanged_read_request_payload() {
    let payload = [0x0a, 0x01, b'S', 0x12, 0x01, b'T'];
    let request = ReadRequest::decode(payload.as_slice()).unwrap();

    assert_eq!(request.server, "S");
    assert_eq!(request.tag_ids, vec!["T"]);
}

#[test]
fn decodes_unchanged_write_response_payload() {
    let payload = [0x0a, 0x01, b'T', 0x10, 0x01];
    let response = WriteResponse::decode(payload.as_slice()).unwrap();

    assert_eq!(response.tag_id, "T");
    assert!(response.success);
    assert_eq!(response.error, None);
}

#[test]
fn old_capabilities_payload_defaults_additive_index_fields() {
    let payload = [
        0x0a, 0x05, b'0', b'.', b'3', b'.', b'2', 0x12, 0x01, b'2', 0x18, 0xc8, 0x01,
    ];
    let response = GetCapabilitiesResponse::decode(payload.as_slice()).unwrap();

    assert_eq!(response.application_version, "0.3.2");
    assert!(!response.supports_indexed_search);
    assert!(response.indexed_search_protocol_version.is_empty());
    assert_eq!(response.max_indexed_search_results, 0);
    assert_eq!(
        response.search_index_state,
        SearchIndexState::Unspecified as i32
    );
}

#[test]
fn round_trips_indexed_search_response_without_session_tokens() {
    let response = SearchIndexResponse {
        matches: vec![IndexedSearchMatch {
            item_id: "FCS0201!204FI00510.PV".into(),
            display_name: "PV".into(),
            kind: BrowseNodeKind::Item as i32,
            breadcrumbs: vec!["FCS0201".into(), "204FI00510".into()],
        }],
        has_more: true,
        status: Some(SearchIndexStatus {
            server: "Yokogawa.CSHIS_OPC.1".into(),
            state: SearchIndexState::Ready as i32,
            configured: true,
            active_generation: 3,
            ..Default::default()
        }),
    };
    let encoded = response.encode_to_vec();
    let decoded = SearchIndexResponse::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded, response);
    assert_eq!(decoded.matches[0].item_id, "FCS0201!204FI00510.PV");
}
