#![no_main]

use libfuzzer_sys::fuzz_target;
use opcda_bridge_proto::bridge::{BrowseRequest, ReadRequest, WriteRequest};
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let _ = BrowseRequest::decode(data);
    let _ = ReadRequest::decode(data);
    let _ = WriteRequest::decode(data);
});
