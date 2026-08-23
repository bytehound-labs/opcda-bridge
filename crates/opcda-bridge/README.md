# opcda-bridge

Reusable async Rust client for the [`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge)
gateway's gRPC API. It provides typed capabilities, lazy paged browsing, progressive search,
list-servers, read, and write operations without bringing in the command-line client's
presentation dependencies.

```toml
[dependencies]
opcda-bridge = "0.3"
```

The library exposes typed capabilities, one-page browse sessions, explicit continuation and
session close operations, progressive search events, and read/write methods. Browse pages are not
automatically drained; callers that need bulk results must collect pages explicitly.

```rust,no_run
use opcda_bridge::{BrowsePageRequest, Client, SearchMatchMode, SearchRequest};

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
client.close_browse_session(root.session_id).await?;
# Ok(())
# }
```

See the crate documentation for method signatures and the repository README for gateway setup and
protocol details.
