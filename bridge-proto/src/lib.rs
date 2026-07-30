pub mod bridge {
    tonic::include_proto!("bridge");
}

#[cfg(test)]
mod tests {
    use crate::bridge::write_request::TypedValue;
    use crate::bridge::{
        BrowseRequest, BrowseResponse, ListServersRequest, ListServersResponse, ReadRequest,
        ReadResponse, TagValue, WriteRequest, WriteResponse,
    };

    #[test]
    fn test_list_servers_request() {
        let req = ListServersRequest {
            host: "localhost".into(),
        };
        assert_eq!(req.host, "localhost");
    }

    #[test]
    fn test_list_servers_request_empty() {
        let req = ListServersRequest {
            host: String::new(),
        };
        assert!(req.host.is_empty());
    }

    #[test]
    fn test_list_servers_response_empty() {
        let resp = ListServersResponse { servers: vec![] };
        assert!(resp.servers.is_empty());
    }

    #[test]
    fn test_list_servers_response_with_data() {
        let resp = ListServersResponse {
            servers: vec!["S1".into(), "S2".into()],
        };
        assert_eq!(resp.servers.len(), 2);
        assert_eq!(resp.servers[0], "S1");
    }

    #[test]
    fn test_browse_request() {
        let req = BrowseRequest {
            server: "MyServer".into(),
            flat: true,
            path: "/root".into(),
            max_tags: 500,
        };
        assert_eq!(req.server, "MyServer");
        assert!(req.flat);
        assert_eq!(req.path, "/root");
        assert_eq!(req.max_tags, 500);
    }

    #[test]
    fn test_browse_request_defaults() {
        let req = BrowseRequest {
            server: String::new(),
            flat: false,
            path: String::new(),
            max_tags: 0,
        };
        assert!(!req.flat);
        assert_eq!(req.max_tags, 0);
    }

    #[test]
    fn test_browse_response() {
        let resp = BrowseResponse {
            tag_id: "Channel1.Device1.Tag1".into(),
            node_type: "Leaf".into(),
        };
        assert_eq!(resp.tag_id, "Channel1.Device1.Tag1");
        assert_eq!(resp.node_type, "Leaf");
    }

    #[test]
    fn test_read_request() {
        let req = ReadRequest {
            server: "MyServer".into(),
            tag_ids: vec!["t1".into(), "t2".into()],
        };
        assert_eq!(req.server, "MyServer");
        assert_eq!(req.tag_ids.len(), 2);
    }

    #[test]
    fn test_read_request_empty_tags() {
        let req = ReadRequest {
            server: "S".into(),
            tag_ids: vec![],
        };
        assert!(req.tag_ids.is_empty());
    }

    #[test]
    fn test_tag_value() {
        let tv = TagValue {
            tag_id: "t1".into(),
            value: "42.5".into(),
            quality: "Good".into(),
            timestamp: "2026-01-01 00:00:00".into(),
        };
        assert_eq!(tv.tag_id, "t1");
        assert_eq!(tv.value, "42.5");
        assert_eq!(tv.quality, "Good");
        assert_eq!(tv.timestamp, "2026-01-01 00:00:00");
    }

    #[test]
    fn test_read_response() {
        let resp = ReadResponse {
            values: vec![TagValue {
                tag_id: "t1".into(),
                value: "42".into(),
                quality: "Good".into(),
                timestamp: "now".into(),
            }],
        };
        assert_eq!(resp.values.len(), 1);
    }

    #[test]
    fn test_read_response_empty() {
        let resp = ReadResponse { values: vec![] };
        assert!(resp.values.is_empty());
    }

    #[test]
    fn test_write_request_string() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::StringValue("hello".into())),
        };
        assert_eq!(req.server, "S");
        assert_eq!(req.tag_id, "t1");
        assert!(matches!(req.typed_value, Some(TypedValue::StringValue(ref s)) if s == "hello"));
    }

    #[test]
    fn test_write_request_int() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::IntValue(42)),
        };
        assert!(matches!(req.typed_value, Some(TypedValue::IntValue(42))));
    }

    #[test]
    fn test_write_request_float() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::FloatValue(3.14)),
        };
        assert!(
            matches!(req.typed_value, Some(TypedValue::FloatValue(v)) if (v - 3.14).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_write_request_bool() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::BoolValue(true)),
        };
        assert!(matches!(req.typed_value, Some(TypedValue::BoolValue(true))));
    }

    #[test]
    fn test_write_request_no_value() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: None,
        };
        assert!(req.typed_value.is_none());
    }

    #[test]
    fn test_write_response_success() {
        let resp = WriteResponse {
            tag_id: "t1".into(),
            success: true,
            error: None,
        };
        assert_eq!(resp.tag_id, "t1");
        assert!(resp.success);
        assert_eq!(resp.error, None);
    }

    #[test]
    fn test_write_response_failure() {
        let resp = WriteResponse {
            tag_id: "t1".into(),
            success: false,
            error: Some("access denied".into()),
        };
        assert!(!resp.success);
        assert_eq!(resp.error, Some("access denied".into()));
    }

    #[test]
    fn test_typed_value_string() {
        let tv = TypedValue::StringValue("test".into());
        assert!(matches!(tv, TypedValue::StringValue(ref s) if s == "test"));
    }

    #[test]
    fn test_typed_value_int() {
        let tv = TypedValue::IntValue(-1);
        assert!(matches!(tv, TypedValue::IntValue(-1)));
    }

    #[test]
    fn test_typed_value_float() {
        let tv = TypedValue::FloatValue(0.0);
        assert!(matches!(tv, TypedValue::FloatValue(v) if v == 0.0));
    }

    #[test]
    fn test_typed_value_bool() {
        let tv = TypedValue::BoolValue(false);
        assert!(matches!(tv, TypedValue::BoolValue(false)));
    }
}
