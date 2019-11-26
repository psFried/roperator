use crate::runner::client::{Client, Patch};
use crate::runner::{RuntimeConfig, duration_to_millis};
use crate::runner::informer::{ResourceMessage, EventType};
use crate::handler::{SyncRequest, FinalizeResponse, Handler};
use crate::resource::K8sResource;
use super::{UpdateError, update_status_if_different, SyncHandler};

use serde_json::Value;

use std::time::{Instant, Duration};
use std::sync::Arc;


fn get_index_of_parent_finalizer(req: &SyncRequest, runtime_config: &RuntimeConfig) -> Option<usize> {
    let finalizer_name = runtime_config.operator_name.as_str();
    req.parent.as_ref().pointer("/metadata/finalizers")
            .and_then(Value::as_array)
            .and_then(|array| {
                array.iter().position(|name| name.as_str() == Some(finalizer_name))
            })
}

pub(crate) async fn handle_finalize(handler: SyncHandler) {
    let SyncHandler { mut sender, request, handler, client, runtime_config, parent_index_key, } = handler;

    let parent_id = request.parent.get_object_id().into_owned();
    let parent_type = runtime_config.parent_type.clone();

    let result = get_finalize_result(request, handler, client, &*runtime_config).await;
    let retry = match result {
        Ok(retry) => {
            log::debug!("Finalize handler for parent: {} completed without error", parent_id);
            retry
        }
        Err(err) => {
            runtime_config.metrics.parent_sync_error(&parent_id);
            log::error!("Failed to finalize parent: {}, err: {}", parent_id, err);
            // here again, we should change this to use an incremental backoff instead of these fixed delays
            tokio::timer::delay_for(Duration::from_secs(5)).await;
            true
        }
    };
    let message = ResourceMessage {
        event_type: EventType::UpdateOperationComplete { retry },
        resource_type: parent_type,
        resource_id: parent_id,
        index_key: Some(parent_index_key),
    };
    let _ = sender.send(message).await;
}


async fn get_finalize_result(request: SyncRequest, handler: Arc<dyn Handler>, client: Client, runtime_config: &RuntimeConfig) -> Result<bool, UpdateError> {
    let parent_finalizer_index = get_index_of_parent_finalizer(&request, runtime_config);
    if parent_finalizer_index.is_none() {
        return Ok(false);
    }

    let (req, finalize_result) = tokio_executor::blocking::run(move || {
        let start_time = Instant::now();
        let result = handler.finalize(&request).map_err(|e| UpdateError::HandlerError(e));
        {
            log::debug!("finished invoking handler for parent: {} in {}ms", request.parent.get_object_id(), duration_to_millis(start_time.elapsed()));
        }
        (request, result)
    }).await;
    let FinalizeResponse{ finalized, status } = finalize_result?;

    let request: SyncRequest = req;
    let current_gen = request.parent.generation();
    let parent_resource_version = request.parent.get_resource_version();
    let old_status = request.parent.status();
    let parent_id = request.parent.get_object_id();

    update_status_if_different(&parent_id, parent_resource_version, &client, runtime_config, current_gen, old_status, status).await?;

    if finalized {
        log::info!("handler response indicates that parent: {} has been finalized", parent_id);
        remove_finalizer(&client, runtime_config, &request.parent).await?;
    } else {
        log::info!("handler response indicates that parent: {} has not been finalized. Will re-try later", parent_id);
        tokio::timer::delay_for(Duration::from_secs(3)).await;
    }

    Ok(!finalized)
}

async fn remove_finalizer<'a>(client: &Client, runtime_config: &RuntimeConfig, parent: &K8sResource) -> Result<(), UpdateError> {
    let id = parent.get_object_id();
    let k8s_type = &*runtime_config.parent_type;
    let patch = Patch::remove_finalizer(parent, runtime_config.operator_name.as_str());
    client.patch_resource(k8s_type, &id, &patch).await?;
    Ok(())
}
