mod compare;
mod sync;
mod finalize;

use crate::runner::client::{self, Client};
use crate::runner::RuntimeConfig;
use crate::runner::informer::ResourceMessage;
use crate::resource::{InvalidResourceError, K8sTypeRef, ObjectIdRef};
use crate::handler::{SyncRequest, Handler};

use tokio::sync::mpsc::Sender;
use serde_json::Value;

use std::sync::Arc;
use std::fmt::{self, Display};


pub(crate) struct SyncHandler {
    pub sender: Sender<ResourceMessage>,
    pub request: SyncRequest,
    pub handler: Arc<dyn Handler>,
    pub client: Client,
    pub runtime_config: Arc<RuntimeConfig>,
    pub parent_index_key: String,
}

impl SyncHandler {

    pub fn start_sync(self) {
        tokio::spawn(async move {
            if self.should_finalize() {
                self::finalize::handle_finalize(self).await;
            } else {
                self::sync::handle_sync(self).await;
            }
        });
    }

    fn should_finalize(&self) -> bool {
        self.request.parent.is_deletion_timestamp_set()
    }

}

#[derive(Debug)]
pub enum UpdateError {
    Client(client::Error),
    InvalidHandlerResponse(InvalidResourceError),
    UnknownChildType(K8sTypeRef<'static>),
    HandlerError(crate::handler::Error),
}

impl Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UpdateError::Client(e) => write!(f, "Client Error: {}", e),
            UpdateError::InvalidHandlerResponse(e) => write!(f, "Invalid response from Handler: {}", e),
            UpdateError::UnknownChildType(child_type) => write!(f, "No configuration exists for child with type: {}", child_type),
            UpdateError::HandlerError(err) => write!(f, "Handler error: {}", err),
        }
    }
}

impl From<client::Error> for UpdateError {
    fn from(err: client::Error) -> UpdateError {
        UpdateError::Client(err)
    }
}

impl From<InvalidResourceError> for UpdateError {
    fn from(err: InvalidResourceError) -> UpdateError {
        UpdateError::InvalidHandlerResponse(err)
    }
}

pub(crate) async fn update_status_if_different(parent_id: &ObjectIdRef<'_>, parent_resource_version: &str, client: &Client, runtime_config: &RuntimeConfig, current_gen: i64, old_status: Option<&Value>, mut new_status: Value) -> Result<(), UpdateError> {
    if new_status.is_null() {
        return Ok(());
    }
    if let Some(s) = new_status.as_object_mut() {
        s.insert("observedGeneration".to_owned(), current_gen.into());
    }
    let should_update = if let Some(old) = old_status {
        let diffs = compare::compare_values(old, &new_status);
        let update_required = diffs.non_empty();
        if update_required {
            log::info!("Found diffs in existing vs desired status for parent: {}: {}", parent_id, diffs);
        } else {
            log::debug!("Current and desired status are the same for parent: {}", parent_id);
        }
        update_required
    } else {
        log::info!("Current status for parent: {} is null", parent_id);
        !new_status.is_null()
    };

    let mut metadata = serde_json::json!({
        "name": parent_id.name(),
        "resourceVersion": parent_resource_version,
    });
    if let Some(ns) = parent_id.namespace() {
        let obj = metadata.as_object_mut().unwrap();
        obj.insert("namespace".to_owned(), Value::String(ns.to_owned()));
    }
    let new_status = serde_json::json!({
        "apiVersion": runtime_config.parent_type.format_api_version(),
        "kind": runtime_config.parent_type.kind.clone(),
        "metadata": metadata,
        "status": new_status,
    });
    if should_update {
        client.update_status(&*runtime_config.parent_type, parent_id, &new_status).await?;
    }
    Ok(())
}
