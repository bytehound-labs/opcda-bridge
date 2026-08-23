# opcda-bridge

Reusable async Rust client for the [`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge)
gateway's gRPC API. It provides typed capabilities, lazy paged browsing, progressive live search,
persistent indexed search and index controls, list-servers, read, and write operations without
bringing in the command-line client's presentation dependencies.

```toml
[dependencies]
opcda-bridge = "0.4"
```

The library exposes typed capabilities, one-page browse sessions, explicit continuation and
session close operations, progressive search events, and read/write methods. Browse pages are not
automatically drained; callers that need bulk results must collect pages explicitly.

```rust,no_run
use opcda_bridge::{
    BrowsePageRequest, Client, SearchIndexRequest, SearchMatchMode, SearchRequest,
};

# async fn example() -> opcda_bridge::Result<()> {
let mut client = Client::connect("localhost:7600").await?;
let root = client.browse("Kepware.KepServerEX.V5", 200).await?;

if let Some(token) = root.next_page_token.clone() {
    let request = BrowsePageRequest::next(
        "Kepware.KepServerEX.V5",
        root.session_id.clone(),
        None,
        token,
        200,
    );
    let _next_page = client.browse_page(request).await?;
}

let mut search = client
    .search_stream(SearchRequest::new(
        "Kepware.KepServerEX.V5",
        "Device1",
        SearchMatchMode::Contains,
    ))
    .await?;
while let Some(event) = search.message().await? {
    println!("{event:?}");
}

let status = client.search_index_status("Kepware.KepServerEX.V5").await?;
if status.configured {
    let indexed = client
        .search_index(SearchIndexRequest::new(
            "Kepware.KepServerEX.V5",
            "Device1 PV",
            SearchMatchMode::Contains,
        ))
        .await?;
    for found in indexed.matches {
        println!("{}: {}", found.display_name, found.item_id);
    }
}
client.close_browse_session(root.session_id).await?;
# Ok(())
# }
```

Indexed search never falls back to live namespace traversal. Each response includes the index
readiness state and `has_more`; returned ItemIDs preserve the server's exact identity and do not
contain browse-session node keys. Use `refresh_search_index` and `control_search_index` for
explicit refresh, pause, resume, and cancel operations.

See the crate documentation for method signatures and the repository README for gateway setup and
protocol details.
