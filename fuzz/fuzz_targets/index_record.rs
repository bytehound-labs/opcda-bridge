#![no_main]

use libfuzzer_sys::fuzz_target;
use opcda_bridge_gateway::index::IndexedMatch;
use opcda_bridge_gateway::index::fuzzing::{parse_breadcrumbs, search_all_modes};
use opcda_bridge_gateway::opc::{InventoryEntry, InventoryNodeKind};

fn text(data: &[u8]) -> String {
    let length = data.len().min(128);
    String::from_utf8_lossy(&data[..length]).into_owned()
}

fn field(data: &[u8], index: usize) -> String {
    let chunk_size = data.len().div_ceil(4).max(1);
    text(data.chunks(chunk_size).nth(index).unwrap_or_default())
}

fuzz_target!(|data: &[u8]| {
    let _ = parse_breadcrumbs(text(data));

    let mut item_id = field(data, 0);
    if item_id.is_empty() {
        item_id = "fuzz-item".into();
    }
    let display_name = field(data, 1);
    let kind = if data.first().is_some_and(|byte| *byte & 1 == 1) {
        InventoryNodeKind::BranchAndItem
    } else {
        InventoryNodeKind::Item
    };
    let breadcrumbs = field(data, 3)
        .split('\u{1f}')
        .take(8)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let entry = InventoryEntry {
        item_id,
        display_name,
        kind,
        breadcrumbs,
    };
    let expected = IndexedMatch {
        item_id: entry.item_id.clone(),
        display_name: entry.display_name.clone(),
        kind: entry.kind,
        breadcrumbs: entry.breadcrumbs.clone(),
    };

    let results = search_all_modes(&entry.item_id, std::slice::from_ref(&entry), 10).unwrap();
    assert_eq!(results[1].as_slice(), std::slice::from_ref(&expected));
});
