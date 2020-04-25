pub(crate) mod compare;
mod finalize;
mod sync;

use crate::error::Error;
use crate::handler::{Handler, SyncRequest};
use crate::resource::{InvalidResourceError, K8sResource};
use crate::runner::client::{self, Client};
use crate::runner::informer::ResourceMessage;
use crate::runner::RuntimeConfig;

use serde_json::Value;
use tokio::sync::mpsc::Sender;

use std::fmt::{self, Display};
use std::sync::Arc;

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
        self.runtime_config
            .metrics
            .parent_sync_started(&self.request.parent.get_object_id());
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
    UnknownChildType(String, String),
    HandlerError(Error),
    TaskCancelled,
}

impl Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UpdateError::Client(e) => write!(f, "Client Error: {}", e),
            UpdateError::InvalidHandlerResponse(e) => {
                write!(f, "Invalid response from Handler: {}", e)
            }
            UpdateError::UnknownChildType(api_version, kind) => write!(
                f,
                "No configuration exists for child with api_version: {}, kind: {}",
                api_version, kind
            ),
            UpdateError::HandlerError(err) => write!(f, "Handler error: {}", err),
            UpdateError::TaskCancelled => write!(f, "Task was cancelled"),
        }
    }
}

impl From<tokio::task::JoinError> for UpdateError {
    fn from(err: tokio::task::JoinError) -> UpdateError {
        if err.is_cancelled() {
            UpdateError::TaskCancelled
        } else {
            UpdateError::HandlerError(crate::error::Error::from(HandlerPanic))
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

#[derive(Debug)]
struct HandlerPanic;
impl std::error::Error for HandlerPanic {}

impl Display for HandlerPanic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Handler paniced")
    }
}

pub(crate) async fn update_status_if_different(
    existing_parent: &K8sResource,
    client: &Client,
    runtime_config: &RuntimeConfig,
    mut new_status: Value,
) -> Result<(), UpdateError> {
    let parent_id = existing_parent.get_object_id();
    let old_status = existing_parent.status();
    let parent_resource_version = existing_parent.resource_version();
    let current_gen = existing_parent.generation();

    if let Some(s) = new_status.as_object_mut() {
        s.insert("observedGeneration".to_owned(), current_gen.into());
    }
    let should_update = if let Some(old) = old_status {
        let diffs = compare::compare_values(old, &new_status);
        let update_required = diffs.non_empty();
        if update_required {
            log::info!(
                "Found diffs in existing vs desired status for parent: {}: {}",
                parent_id,
                diffs
            );
        } else {
            log::debug!(
                "Current and desired status are the same for parent: {}",
                parent_id
            );
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
        "apiVersion": runtime_config.parent_type.api_version,
        "kind": runtime_config.parent_type.kind,
        "metadata": metadata,
        "status": new_status,
    });
    if should_update {
        client
            .update_status(&*runtime_config.parent_type, &parent_id, &new_status)
            .await?;
    }
    Ok(())
}

fn does_finalizer_exist(resource: &Value, runtime_config: &RuntimeConfig) -> bool {
    let finalizer_name = runtime_config.operator_name.as_str();
    resource
        .pointer("/metadata/finalizers")
        .and_then(Value::as_array)
        .map(|array| {
            array
                .iter()
                .any(|name| name.as_str() == Some(finalizer_name))
        })
        .unwrap_or(false)
}
