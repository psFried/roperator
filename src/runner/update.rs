use crate::handler::{SyncRequst, SyncResponse, Handler};
use crate::runner::client::{self, Client};
use crate::config::{UpdateStrategy};
use crate::resource::{ObjectId, InvalidResourceError, JsonObject, object_id, type_ref, K8sTypeRef, ObjectIdRef};
use crate::runner::controller::{ResourceMessage, EventType, to_raw_api, convert_request};
use crate::runner::{duration_to_millis, RuntimeConfig, ChildRuntimeConfig};
use crate::runner::compare::{compare_values};

use tokio::timer::delay_for;
use tokio::sync::mpsc::Sender;
use serde_json::{json, Value};
use kube::api::PostParams;

use std::sync::Arc;
use std::time::Instant;
use std::collections::HashSet;
use std::fmt::{self, Display};


#[derive(Debug)]
pub enum UpdateError {
    Client(client::Error),
    InvalidHandlerResponse(InvalidResourceError),
    UnknownChildType(K8sTypeRef<'static>),
}

impl Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UpdateError::Client(e) => write!(f, "Client Error: {}", e),
            UpdateError::InvalidHandlerResponse(e) => write!(f, "Invalid response from Handler: {}", e),
            UpdateError::UnknownChildType(child_type) => write!(f, "No configuration exists for child with type: {}", child_type),
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

pub struct RequestHandler {
    pub sender: Sender<ResourceMessage>,
    pub request: SyncRequst,
    pub handler: Arc<dyn Handler>,
    pub client: Client,
    pub runtime_config: Arc<RuntimeConfig>,
    pub parent_index_key: String,
}


impl RequestHandler {

    pub fn handle_update(self) {
        let RequestHandler { mut sender, request, handler, client, runtime_config, parent_index_key } = self;
        let parent_id = request.parent.get_object_id().into_owned();

        let start_time = Instant::now();

        tokio::spawn(async move {

            let (request, response) = {
                tokio_executor::blocking::run(move || {
                    let result = handler.sync(&request);
                    log::debug!("finished invoking handler for parent: {} in {}ms", request.parent.get_object_id(), duration_to_millis(start_time.elapsed()));
                    (request, result)
                }).await
            };

            let result = update_all(request, response, client, runtime_config.clone()).await;
            if let Err(err) = result {
                log::error!("Error while syncing parent: {}: {:?}", parent_id, err);
                // we'll delay for a while before sending the message that we've finished so that we can
                // prevent the main loop from re-trying too soon. Eventually we should implement a backoff
                delay_for(std::time::Duration::from_secs(10)).await;
            } else {
                log::info!("Finished sync for parent: {}", parent_id);
            }
            let message = ResourceMessage {
                event_type: EventType::UpdateOperationComplete,
                resource_id: parent_id,
                resource_type: runtime_config.parent_type.clone(),
                index_key: Some(parent_index_key),
            };
            let _ = sender.send(message).await;
        });
    }
}


async fn update_all(request: SyncRequst, handler_response: SyncResponse, client: Client, runtime_config: Arc<RuntimeConfig>) -> Result<(), UpdateError> {
    let start_time = Instant::now();
    let SyncResponse {status, children} = handler_response;
    let parent_id = request.parent.get_object_id().into_owned();
    let current_generation = request.parent.generation();
    update_status_if_different(&parent_id, &client, &*runtime_config, current_generation, request.parent.status(), status).await?;
    log::debug!("Successfully updated status for parent: {} in {}ms", parent_id, duration_to_millis(start_time.elapsed()));
    let child_ids = update_children(&client, &*runtime_config, &request, children).await?;
    log::debug!("Successfully updated all {} children of parent: {} in {}ms", child_ids.len(), parent_id,
            duration_to_millis(start_time.elapsed()));

    // now that all the child updates have completed successfully, we'll delete any children that are no longer desired
    delete_undesired_children(&client, &*runtime_config, &child_ids, &request).await?;
    Ok(())
}

async fn delete_undesired_children(client: &Client, runtime_config: &RuntimeConfig, desired_children: &HashSet<ObjectId>, sync_request: &SyncRequst) -> Result<(), client::Error> {
    for existing_child in sync_request.children.iter() {
        let child_id = existing_child.get_object_id();
        if !desired_children.contains(&child_id) {
            log::info!("Need to delete child: {} of parent: {} because it was not included in the handler response",
                    child_id, sync_request.parent.get_object_id());
            let child_type = runtime_config.type_for(&existing_child.get_type_ref())
                    .expect("No configuration found for existing child type");
            client.delete_resource(child_type, &child_id).await?;
        }
    }
    Ok(())
}

async fn update_status_if_different(parent_id: &ObjectId, client: &Client, runtime_config: &RuntimeConfig, current_gen: i64, old_status: Option<&Value>, mut new_status: Value) -> Result<(), UpdateError> {
    if let Some(s) = new_status.as_object_mut() {
        s.insert("observedGeneration".to_owned(), current_gen.into());
    }
    let should_update = if let Some(old) = old_status {
        let diffs = compare_values(old, &new_status);
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

    if should_update {
        do_status_update(parent_id, client, runtime_config, new_status).await?;
    }
    Ok(())
}

async fn do_status_update(parent_id: &ObjectId, client: &Client, runtime_config: &RuntimeConfig, new_status: Value) -> Result<(), UpdateError> {
    let mut raw = to_raw_api(runtime_config.parent_type.as_ref());
    raw.namespace = parent_id.namespace().map(String::from);
    let serialized = serde_json::to_vec(&new_status).map_err(client::Error::from)?;
    let post_params = PostParams {
        dry_run: false,
    };

    let req = convert_request(raw.replace_status(parent_id.name(), &post_params, serialized).unwrap());
    let resp = client.get_response(req).await?;
    if !resp.status().is_success() {
        Err(client::Error::http(resp.status().clone()).into())
    } else {
        Ok(())
    }
}

async fn update_children(client: &Client, runtime_config: &RuntimeConfig, req: &SyncRequst, response_children: Vec<Value>) -> Result<HashSet<ObjectIdRef<'static>>, UpdateError> {
    let parent_uid = req.parent.uid();
    let parent_id = req.parent.get_object_id();
    let mut child_ids = HashSet::new();
    for mut child in response_children {
        let child_id = object_id(&child).ok_or_else(|| {
            InvalidResourceError::new("missing name", child.clone())
        })?.into_owned();
        let child_type = type_ref(&child).ok_or_else(|| {
            InvalidResourceError::new("missing either apiVersion or kind", child.clone())
        })?.into_owned();

        // ensure that the child has the same namespace as the parent. This is a deliberate constraint that
        // we place on users of this library, as having non-namespaced children of namespaced parents would
        // add considerable complexity.
        if child_id.namespace() != parent_id.namespace() {
            const MESSAGE: &str = "Child namespace does not match the namespace of the parent";
            return Err(InvalidResourceError::new(MESSAGE, child.clone()).into());
        }

        // get the configuration for this child type, and bail if it doesn't exist
        let child_config = runtime_config.child_types.get(&child_type).ok_or_else(|| {
            UpdateError::UnknownChildType(child_type.clone())
        })?;
        add_parent_references(runtime_config, parent_id.name(), parent_uid, &mut child)?;
        let is_update_required = is_child_update_required(child_config, req, &child_type, &child_id, &child)?;
        if is_update_required {
            log::debug!("Starting child update for parent_uid: {}, child_type: {}, child_id: {}",
                parent_uid, child_type, child_id);
            let start_time = Instant::now();
            let result = do_child_update(child_config, client, &child).await;
            let total_millis = duration_to_millis(start_time.elapsed());
            log::debug!("Finshed child update for {} in {}ms with result: {:?}", child_id, total_millis, result);
            result?; // return early if it failed
        }
        child_ids.insert(child_id);
    }
    Ok(child_ids)
}

async fn do_child_update(child_config: &ChildRuntimeConfig, client: &Client, desired_child: &Value) -> Result<(), client::Error> {
    let k8s_type = &child_config.child_type;
    let child_id = object_id(desired_child).unwrap();
    match child_config.update_strategy {
        UpdateStrategy::OnDelete => {
            client.create_resource(k8s_type, desired_child).await
        }
        UpdateStrategy::Recreate => {
            client.delete_resource(k8s_type, &child_id).await?;
            client.create_resource(k8s_type, desired_child).await
        }
        UpdateStrategy::Replace => {
            client.replace_resource(k8s_type, &child_id, desired_child).await
        }
    }
}

fn is_child_update_required(child_config: &ChildRuntimeConfig, req: &SyncRequst, child_type: &K8sTypeRef<'_>, child_id: &ObjectIdRef<'_>, child: &Value) -> Result<bool, UpdateError> {
    let (api_version, kind) = child_type.as_parts();
    let (namespace, name) = child_id.as_parts();
    let parent_id = req.parent.get_object_id();

    let should_update = match (req.find_child(api_version, kind, namespace, name), child_config.update_strategy) {
        (Some(_), UpdateStrategy::OnDelete) => {
            log::debug!("UpdateStrategy for child type {} is OnDelete and child {} already exists, so will not update",
                    child_type, child_id);

            false
        }
        (Some(existing_child), _) => {
            let diffs = compare_values(existing_child.as_ref(), child);
            if diffs.non_empty() {
                log::info!("Found {} diffs in child of parent: {} with type: {} and id: {}, diffs: {}",
                        diffs.len(), parent_id, child_type, child_id, diffs);
            } else {
                log::debug!("No difference in child of parent: {}, with type: {} and id: {}", parent_id, child_type, child_id);
            }
            diffs.non_empty()
        }
        (None, _) => {
            log::debug!("No existing child of parent: {} with type: {} and id: {}", parent_id, child_type, child_id);
            true
        }
    };
    Ok(should_update)
}

fn add_parent_references(runtime_config: &RuntimeConfig, parent_name: &str, parent_uid: &str, child: &mut Value) -> Result<(), InvalidResourceError> {
    let meta = require_object_mut(child, "/metadata", "child object is missing 'metadata'")?;
    if !meta.contains_key("labels") || !meta.get("labels").unwrap().is_object() {
        meta.insert("labels".to_owned(), Value::Object(JsonObject::new()));
    }
    {
        let labels = meta.get_mut("labels").unwrap().as_object_mut().unwrap(); // we just ensured this above
        labels.insert(runtime_config.correlation_label_name.clone(), parent_uid.into());
        labels.insert(runtime_config.controller_label_name.clone(), runtime_config.operator_name.as_str().into());
    }
    if !meta.contains_key("ownerReferences") || !meta.get("ownerReferences").unwrap().is_array() {
        meta.insert("ownerReferences".to_owned(), Value::Array(Vec::new()));
    }
    let owner_refs = meta.get_mut("ownerReferences").unwrap().as_array_mut().unwrap();
    let new_ref = make_owner_ref(parent_uid, parent_name, runtime_config);
    if !owner_refs.contains(&new_ref) {
        owner_refs.push(new_ref);
    }
    Ok(())
}

fn make_owner_ref(parent_uid: &str, parent_name: &str, runtime_config: &RuntimeConfig) -> Value {
    json!({
        "apiVersion": runtime_config.parent_type.format_api_version(),
        "controller": true,
        "kind": runtime_config.parent_type.kind.clone(),
        "name": parent_name,
        "uid": parent_uid,
    })
}

fn require_object_mut<'a>(value: &'a mut Value, pointer: &'static str, err_msg: &'static str) -> Result<&'a mut JsonObject, InvalidResourceError> {
    if value.pointer(pointer).map(Value::is_object).unwrap_or(false) {
        Ok(value.pointer_mut(pointer).and_then(Value::as_object_mut).unwrap())
    } else {
        Err(InvalidResourceError::new(err_msg, value.clone()))
    }
}
