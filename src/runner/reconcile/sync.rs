use crate::handler::{SyncRequest, SyncResponse, Handler};
use crate::runner::client::{self, Client};
use crate::config::UpdateStrategy;
use crate::resource::{K8sResource, ObjectId, InvalidResourceError, JsonObject, object_id, type_ref, K8sTypeRef, ObjectIdRef};
use crate::runner::informer::{ResourceMessage, EventType};
use crate::runner::{duration_to_millis, RuntimeConfig, ChildRuntimeConfig};
use crate::runner::reconcile::compare::{compare_values};
use crate::runner::reconcile::{SyncHandler, UpdateError, update_status_if_different};

use tokio::timer::delay_for;
use serde_json::{json, Value};

use std::sync::Arc;
use std::time::Instant;
use std::collections::HashSet;




pub(crate) async fn handle_sync(handler: SyncHandler) {
    let SyncHandler { mut sender, request, handler, client, runtime_config, parent_index_key } = handler;
    let parent_id = request.parent.get_object_id().into_owned();

    let start_time = Instant::now();
    let result = private_handle_sync(start_time, request, handler, client, &*runtime_config).await;

    let retry = if let Err(err) = result {
        log::error!("Error while syncing parent: {}: {:?}", parent_id, err);
        // we'll delay for a while before sending the message that we've finished so that we can
        // prevent the main loop from re-trying too soon. Eventually we should implement a backoff
        delay_for(std::time::Duration::from_secs(10)).await;
        true
    } else {
        log::info!("Finished sync for parent: {}", parent_id);
        false
    };
    let message = ResourceMessage {
        event_type: EventType::UpdateOperationComplete { retry },
        resource_id: parent_id,
        resource_type: runtime_config.parent_type.clone(),
        index_key: Some(parent_index_key),
    };
    let _ = sender.send(message).await;
}

async fn private_handle_sync(start_time: Instant, request: SyncRequest, handler: Arc<dyn Handler>, client: Client, runtime_config: &RuntimeConfig) -> Result<(), UpdateError> {
    let (request, sync_result) = {
        tokio_executor::blocking::run(move || {
            let result = handler.sync(&request);
            log::debug!("finished invoking handler for parent: {} in {}ms", request.parent.get_object_id(), duration_to_millis(start_time.elapsed()));
            (request, result)
        }).await
    };
    let response = sync_result.map_err(|e| UpdateError::HandlerError(e))?;
    update_all(request, response, client, runtime_config).await
}


async fn update_all(request: SyncRequest, handler_response: SyncResponse, client: Client, runtime_config: &RuntimeConfig) -> Result<(), UpdateError> {
    let start_time = Instant::now();
    let SyncResponse {status, children} = handler_response;
    let parent_id = request.parent.get_object_id().into_owned();
    let parent_resource_version = request.parent.get_resource_version();
    let current_generation = request.parent.generation();
    update_status_if_different(&parent_id, parent_resource_version, &client, runtime_config, current_generation, request.parent.status(), status).await?;
    log::debug!("Successfully updated status for parent: {} in {}ms", parent_id, duration_to_millis(start_time.elapsed()));
    let child_ids = update_children(&client, runtime_config, &request, children).await?;
    log::debug!("Successfully updated all {} children of parent: {} in {}ms", child_ids.len(), parent_id,
            duration_to_millis(start_time.elapsed()));

    // now that all the child updates have completed successfully, we'll delete any children that are no longer desired
    delete_undesired_children(&client, runtime_config, &child_ids, &request).await?;
    Ok(())
}

async fn delete_undesired_children(client: &Client, runtime_config: &RuntimeConfig, desired_children: &HashSet<ObjectId>, sync_request: &SyncRequest) -> Result<(), client::Error> {
    for existing_child in sync_request.children.iter() {
        let child_id = existing_child.get_object_id();
        if !desired_children.contains(&child_id) && !existing_child.is_deletion_timestamp_set() {
            log::info!("Need to delete child: {} of parent: {} because it was not included in the handler response",
                    child_id, sync_request.parent.get_object_id());
            let child_type = runtime_config.type_for(&existing_child.get_type_ref())
                    .expect("No configuration found for existing child type");
            client.delete_resource(child_type, &child_id).await?;
        }
    }
    Ok(())
}

async fn update_children(client: &Client, runtime_config: &RuntimeConfig, req: &SyncRequest, response_children: Vec<Value>) -> Result<HashSet<ObjectIdRef<'static>>, UpdateError> {
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
            log::error!("Child {} is not in the same namespace as parent: {}", child_id, parent_id);
            const MESSAGE: &str = "Child namespace does not match the namespace of the parent";
            return Err(InvalidResourceError::new(MESSAGE, child.clone()).into());
        }

        // get the configuration for this child type, and bail if it doesn't exist
        let child_config = runtime_config.child_types.get(&child_type).ok_or_else(|| {
            UpdateError::UnknownChildType(child_type.clone())
        })?;
        add_parent_references(runtime_config, parent_id.name(), parent_uid, &mut child)?;
        let existing_child = find_existing_child(req, &child_type, &child_id);
        let update_required = is_child_update_required(&parent_id, child_config, existing_child, &child_type, &child_id, &child)?;
        if let Some(update_type) = update_required {
            let start_time = Instant::now();
            log::debug!("Starting child update for parent_uid: {}, child_type: {}, child_id: {}",
                parent_uid, child_type, child_id);
            let result = do_child_update(update_type, child_config, client, child).await;
            let total_millis = duration_to_millis(start_time.elapsed());
            log::debug!("Finshed child update for {} in {}ms with result: {:?}", child_id, total_millis, result);
            result?; // return early if it failed
        }
        child_ids.insert(child_id);
    }
    Ok(child_ids)
}

async fn do_child_update(update_type: UpdateType, child_config: &ChildRuntimeConfig, client: &Client, mut desired_child: Value) -> Result<(), client::Error> {
    let k8s_type = &child_config.child_type;
    match update_type {
        UpdateType::Create => {
            client.create_resource(k8s_type, &desired_child).await
        }
        UpdateType::Replace(resource_version) => {
            { // if we're replacing the resource, then we need to specify the old resourceVersion
                let obj = desired_child.pointer_mut("/metadata").and_then(Value::as_object_mut).unwrap();
                obj.insert("resourceVersion".to_owned(), Value::String(resource_version));
            }
            let child_id = object_id(&desired_child).unwrap();
            client.replace_resource(k8s_type, &child_id, &desired_child).await
        }
        UpdateType::Delete => {
            let child_id = object_id(&desired_child).unwrap();
            // TODO: Deleting a resource could return a 409 error if it's already being deleted. Figure out how to deal with that
            client.delete_resource(k8s_type, &child_id).await
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum UpdateType {
    Create,
    Replace(String),
    Delete,
}

fn find_existing_child<'a, 'b>(req: &'a SyncRequest, child_type: &'b K8sTypeRef<'_>, child_id: &'b ObjectIdRef<'_>) -> Option<&'a K8sResource> {
    let (api_version, kind) = child_type.as_parts();
    let (namespace, name) = child_id.as_parts();
    req.raw_child(api_version, kind, namespace, name)
}

fn is_child_update_required(parent_id: &ObjectIdRef<'_>, child_config: &ChildRuntimeConfig, existing_child: Option<&K8sResource>, child_type: &K8sTypeRef<'_>, child_id: &ObjectIdRef<'_>, child: &Value) -> Result<Option<UpdateType>, UpdateError> {
    let update_type = match (existing_child, child_config.update_strategy) {
        (Some(_), UpdateStrategy::OnDelete) => {
            log::debug!("UpdateStrategy for child type {} is OnDelete and child {} already exists, so will not update",
                    child_type, child_id);
            None
        }
        (Some(existing_child), update_strategy) => {
            let diffs = compare_values(existing_child.as_ref(), child);
            if diffs.non_empty() {
                log::info!("Found {} diffs in child of parent: {} with type: {} and id: {}, diffs: {}",
                        diffs.len(), parent_id, child_type, child_id, diffs);
                determine_update_type(existing_child, update_strategy)
            } else {
                log::debug!("No difference in child of parent: {}, with type: {} and id: {}", parent_id, child_type, child_id);
                None
            }
        }
        (None, _) => {
            log::debug!("No existing child of parent: {} with type: {} and id: {}", parent_id, child_type, child_id);
            Some(UpdateType::Create)
        }
    };
    Ok(update_type)
}

/// figures out what to do when the desired child is different from the existing one. The answer
/// may be to do nothing for now, if the existing child is in the process of being deleted
fn determine_update_type(existing_child: &K8sResource, update_strategy: UpdateStrategy) -> Option<UpdateType> {
    if existing_child.is_deletion_timestamp_set() {
        log::debug!("Will skip updating child: {} : {} on this loop because it is currently being deleted",
                existing_child.get_type_ref(), existing_child.get_object_id());
        None
    } else if update_strategy == UpdateStrategy::Recreate {
        // the existing child is not yet being deleted, but we'll need to delete it before we can re-create it.
        // When updateStrategy is recreate, we only delete it on the first go around and then we'll do a Create
        // once the delete has finished. This allows us to continue to make progress on the rest of the sync operations
        // since deletion can sometimes take quite a while due to finalizers needing to run.
        Some(UpdateType::Delete)
    } else {
        let resource_version = existing_child.get_resource_version();
        Some(UpdateType::Replace(resource_version.to_owned()))
    }
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
