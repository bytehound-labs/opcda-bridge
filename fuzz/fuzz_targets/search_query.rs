#![no_main]

use libfuzzer_sys::fuzz_target;
use opcda_bridge_gateway::index::IndexedMatch;
use opcda_bridge_gateway::index::fuzzing::{SearchAllModesError, search_all_modes};
use opcda_bridge_gateway::opc::{InventoryEntry, InventoryNodeKind};

fn text(data: &[u8]) -> String {
    let length = data.len().min(128);
    String::from_utf8_lossy(&data[..length]).into_owned()
}

fn item_ids(matches: &[IndexedMatch]) -> Vec<&str> {
    matches.iter().map(|entry| entry.item_id.as_str()).collect()
}

fuzz_target!(|data: &[u8]| {
    let query = format!("fuzz{}", text(data));
    let item_prefix = format!("{query}\0prefix");
    let item_contains = format!("contains:{query}:suffix");
    let make_entry = |item_id: String, display_name: String, kind| InventoryEntry {
        item_id,
        display_name,
        kind,
        breadcrumbs: vec!["root".into()],
    };
    let entries = [
        make_entry("0".into(), query.clone(), InventoryNodeKind::Item),
        make_entry(
            query.clone(),
            "zzzz".into(),
            InventoryNodeKind::BranchAndItem,
        ),
        make_entry(
            "2".into(),
            format!("{query} suffix"),
            InventoryNodeKind::Item,
        ),
        make_entry(
            item_prefix.clone(),
            "zzzz".into(),
            InventoryNodeKind::BranchAndItem,
        ),
        make_entry(
            "4".into(),
            format!("prefix {query} suffix"),
            InventoryNodeKind::Item,
        ),
        make_entry(
            item_contains.clone(),
            "zzzz".into(),
            InventoryNodeKind::BranchAndItem,
        ),
        make_entry("6".into(), "zzzz".into(), InventoryNodeKind::Item),
    ];

    let results = match search_all_modes(&query, &entries, 10) {
        Ok(results) => results,
        Err(SearchAllModesError::QueryRejected(_)) => return,
        Err(SearchAllModesError::Setup(error)) => {
            panic!("unable to prepare indexed-search fuzz input: {error:#}")
        }
    };
    assert_eq!(results[0], results[3]);
    if query.bytes().all(|byte| (b' '..=b'~').contains(&byte)) {
        assert_eq!(item_ids(&results[1]), vec!["0", query.as_str()]);
        assert_eq!(
            item_ids(&results[2]),
            vec!["0", query.as_str(), "2", item_prefix.as_str()]
        );
        assert_eq!(
            item_ids(&results[3]),
            vec![
                "0",
                query.as_str(),
                "2",
                item_prefix.as_str(),
                "4",
                item_contains.as_str(),
            ]
        );
    }
});
