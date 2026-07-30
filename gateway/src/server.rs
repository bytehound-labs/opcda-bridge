use bridge_proto::bridge::{
    BrowseRequest, BrowseResponse, ListServersRequest, ListServersResponse, ReadRequest,
    ReadResponse, TagValue as ProtoTagValue, WriteRequest, WriteResponse, bridge_server::Bridge,
    write_request::TypedValue as ProtoTypedValue,
};
use opc_da_client::{OpcDaWrapper, OpcProvider, OpcValue};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

const NODE_TYPE_LEAF: &str = "Leaf";

pub struct BridgeService {
    client: OpcDaWrapper,
}

impl Default for BridgeService {
    fn default() -> Self {
        Self {
            client: OpcDaWrapper::default(),
        }
    }
}

#[tonic::async_trait]
impl Bridge for BridgeService {
    async fn list_servers(
        &self,
        request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        let req = request.into_inner();
        let host = if req.host.is_empty() {
            "localhost"
        } else {
            &req.host
        };

        let servers = self.client.list_servers(host).await.map_err(internal)?;

        Ok(Response::new(ListServersResponse { servers }))
    }

    type BrowseStream = ReceiverStream<std::result::Result<BrowseResponse, Status>>;

    async fn browse(
        &self,
        request: Request<BrowseRequest>,
    ) -> Result<Response<Self::BrowseStream>, Status> {
        let req = request.into_inner();
        let max_tags = if req.max_tags == 0 {
            1000
        } else {
            req.max_tags as usize
        };

        let tags_sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let discovered = self
            .client
            .browse_tags(&req.server, max_tags, progress.clone(), tags_sink.clone())
            .await
            .map_err(internal)?;

        let tags = if req.flat {
            match tags_sink.lock() {
                Ok(guard) => guard.clone(),
                Err(_) => return Err(Status::internal("browse lock poisoned")),
            }
        } else {
            discovered
        };

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            for tag_id in tags {
                if tx
                    .send(Ok(BrowseResponse {
                        tag_id,
                        node_type: NODE_TYPE_LEAF.to_string(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        let values = self
            .client
            .read_tag_values(&req.server, req.tag_ids)
            .await
            .map_err(internal)?;

        let proto_values: Vec<ProtoTagValue> = values
            .into_iter()
            .map(|v| ProtoTagValue {
                tag_id: v.tag_id,
                value: v.value,
                quality: v.quality,
                timestamp: v.timestamp,
            })
            .collect();

        Ok(Response::new(ReadResponse {
            values: proto_values,
        }))
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();

        let opc_value = req
            .typed_value
            .ok_or_else(|| Status::invalid_argument("no typed_value provided"))?;

        let value = match opc_value {
            ProtoTypedValue::StringValue(s) => OpcValue::String(s),
            ProtoTypedValue::IntValue(i) => OpcValue::Int(i),
            ProtoTypedValue::FloatValue(f) => OpcValue::Float(f),
            ProtoTypedValue::BoolValue(b) => OpcValue::Bool(b),
        };

        let result = self
            .client
            .write_tag_value(&req.server, &req.tag_id, value)
            .await
            .map_err(internal)?;

        Ok(Response::new(WriteResponse {
            tag_id: result.tag_id,
            success: result.success,
            error: result.error,
        }))
    }
}
