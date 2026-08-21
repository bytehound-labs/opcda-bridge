use opcda_bridge_proto::bridge::{BrowseRequest, ReadRequest, WriteResponse};
use prost::Message;

#[test]
fn decodes_previous_browse_request_without_newer_fields() {
    let payload = [0x0a, 0x01, b'S', 0x10, 0x01, 0x1a, 0x03, b'A', b'/', b'B'];
    let request = BrowseRequest::decode(payload.as_slice()).unwrap();

    assert_eq!(request.server, "S");
    assert!(request.flat);
    assert_eq!(request.path, "A/B");
    assert_eq!(request.max_tags, 0);
}

#[test]
fn decodes_previous_read_request_payload() {
    let payload = [0x0a, 0x01, b'S', 0x12, 0x01, b'T'];
    let request = ReadRequest::decode(payload.as_slice()).unwrap();

    assert_eq!(request.server, "S");
    assert_eq!(request.tag_ids, vec!["T"]);
}

#[test]
fn decodes_previous_write_response_payload() {
    let payload = [0x0a, 0x01, b'T', 0x10, 0x01];
    let response = WriteResponse::decode(payload.as_slice()).unwrap();

    assert_eq!(response.tag_id, "T");
    assert!(response.success);
    assert_eq!(response.error, None);
}
