mod controller;
mod update;
mod compare;
mod client;
mod request;

use crate::resource::{K8sResource, ObjectId, K8sTypeRef};
use crate::config::{OperatorConfig, ClientConfig, K8sType, UpdateStrategy};
use crate::runner::controller::{ResourceMessage, ResourceMonitor, EventType, LabelToIdIndex, UidToIdIndex};
use crate::runner::update::RequestHandler;
use client::Client;
use crate::handler::{SyncRequest, Handler};

use failure::Error;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::prelude::*;

use std::time::{Instant, Duration};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub struct ChildRuntimeConfig {
    update_strategy: UpdateStrategy,
    child_type: Arc<K8sType>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    pub child_types: HashMap<K8sTypeRef<'static>, ChildRuntimeConfig>,
    pub parent_type: Arc<K8sType>,
    pub correlation_label_name: String,
    pub controller_label_name: String,
    pub operator_name: String,
}

impl RuntimeConfig {

    fn type_for<'a, 'b>(&'a self, type_ref: &'b K8sTypeRef<'a>) -> Option<&'a K8sType> {
        self.child_types.get(type_ref).map(|c| &*c.child_type)
    }

}

pub fn run_operator(config: OperatorConfig, client_config: ClientConfig, handler: Arc<dyn Handler>) -> Result<(), Error> {
    let runtime = tokio::runtime::Runtime::new()?;

    runtime.block_on(async move {
        let client = Client::new(client_config)?;
        run_with_client(config, client, handler).await
    })
}

async fn run_with_client(config: OperatorConfig, client: Client, handler: Arc<dyn Handler>) -> Result<(), Error> {
    let OperatorConfig {parent, child_types, namespace, operator_name, tracking_label_name, ownership_label_name} = config;

    let parent_type = Arc::new(parent);

    let (tx, rx) = tokio::sync::mpsc::channel::<ResourceMessage>(1024);

    let parent_monitor = controller::start_parent_monitor(namespace.clone(), parent_type.clone(), client.clone(), tx.clone()).await;

    let mut child_runtime_config = HashMap::with_capacity(4);
    let mut children = HashMap::with_capacity(4);

    for (child_type, child_conf) in child_types {
        let type_key = child_type.to_type_ref();
        let child_type = Arc::new(child_type);
        let runtime_conf = ChildRuntimeConfig {
            child_type: child_type.clone(),
            update_strategy: child_conf.update_strategy,
        };
        child_runtime_config.insert(type_key, runtime_conf);
        let child_monitor = controller::start_child_monitor(tracking_label_name.clone(), namespace.clone(),
                child_type.clone(), client.clone(), tx.clone()).await;
        children.insert(child_type, child_monitor);
    }
    let runtime_config = Arc::new(RuntimeConfig {
        child_types: child_runtime_config,
        parent_type: parent_type.clone(),
        correlation_label_name: tracking_label_name,
        controller_label_name: ownership_label_name,
        operator_name,
    });

    let mut state = OperatorState {
        parent_type,
        parents: parent_monitor,
        children,
        sender: tx,
        receiver: rx,
        in_progress_updates: HashMap::new(),
        client,
        runtime_config,
    };

    state.run(handler).await;
    Ok(())
}

type HandlerRef = Arc<dyn Handler>;

struct InProgressUpdate {
    parent_id: ObjectId,
    start_time: Instant,
}

impl InProgressUpdate {
    fn new(parent_id: ObjectId) -> InProgressUpdate {
        InProgressUpdate {
            parent_id,
            start_time: Instant::now(),
        }
    }
}

struct OperatorState {
    parent_type: Arc<K8sType>,
    parents: ResourceMonitor<UidToIdIndex>,
    children: HashMap<Arc<K8sType>, ResourceMonitor<LabelToIdIndex>>,
    sender: Sender<ResourceMessage>,
    receiver: Receiver<ResourceMessage>,
    in_progress_updates: HashMap<String, InProgressUpdate>,
    client: Client,
    runtime_config: Arc<RuntimeConfig>,
}


impl OperatorState {

    async fn run(&mut self, handler: HandlerRef) {
        let mut parent_ids_to_sync: HashSet<String> = HashSet::with_capacity(16);
        let mut deleted_parent_ids: HashSet<String> = HashSet::with_capacity(16);
        let mut synced_parents: HashSet<String> = HashSet::with_capacity(16);

        loop {
            log::debug!("Starting sync loop with {} existing parents needing to sync", parent_ids_to_sync.len());
            self.get_parent_uids_to_update(&mut parent_ids_to_sync, &mut deleted_parent_ids).await;
            for parent_uid in parent_ids_to_sync.iter() {
                let result = self.sync_parent(parent_uid.as_str(), handler.clone()).await;
                if let Err(err) = result {
                    log::error!("Cannot sync parent with uid: {} due to error: {:?}", parent_uid, err);
                } else {
                    synced_parents.insert(parent_uid.clone());
                }
            }

            // TODO: invoke finalize for deleted parents
            for id in synced_parents.drain() {
                deleted_parent_ids.remove(&id);
                parent_ids_to_sync.remove(&id);
            }
        }
    }


    fn is_update_in_progress(&self, parent_uid: &str) -> bool {
        self.in_progress_updates.contains_key(parent_uid)
    }

    async fn sync_parent(&mut self, parent_uid: &str, handler: HandlerRef) -> Result<(), Error> {
        let parent = match self.get_parent(parent_uid).await? {
            Some(p) => p,
            None => {
                log::warn!("Cannot sync parent with uid: '{}' because resource has been subsequently deleted", parent_uid);
                return Ok(());
            }
        };
        let children = self.get_all_children(parent_uid).await?;
        log::info!("Starting sync request for parent: '{}/{}' with uid: '{}'", parent.namespace().unwrap_or(""), parent.name(), parent.uid());

        let parent_id = parent.get_object_id().into_owned();
        self.in_progress_updates.insert(parent_uid.to_owned(), InProgressUpdate::new(parent_id));

        let request = SyncRequest { parent: parent, children };
        let handler = RequestHandler {
            sender: self.sender.clone(),
            request,
            handler: handler.clone(),
            client: self.client.clone(),
            runtime_config: self.runtime_config.clone(),
            parent_index_key: parent_uid.to_owned(),
        };
        handler.handle_update();

        Ok(())
    }

    async fn get_parent(&self, parent_uid: &str) -> Result<Option<K8sResource>, Error> {
        let parent_lock = self.parents.lock_state().await?;
        Ok(parent_lock.get_by_uid(parent_uid))
    }


    async fn get_all_children(&self, parent_uid: &str) -> Result<Vec<K8sResource>, Error> {
        let mut request_children = Vec::with_capacity(8);

        for children_monitor in self.children.values() {
            let lock = children_monitor.lock_state().await?;
            let kids_of_type = lock.get_all_resources_by_index_key(parent_uid);
            request_children.extend(kids_of_type);
        }

        Ok(request_children)
    }


    /// Tries to receive a whole batch of messages, so that we can consolidate them by parent id
    /// TODO: make the timeouts for these batches configurable
    async fn get_parent_uids_to_update(&mut self, to_sync: &mut HashSet<String>, to_finalize: &mut HashSet<String>) {
        let starting_to_sync_len = to_sync.len();
        let starting_to_finalize_len = to_finalize.len();
        let start_time = Instant::now();
        let mut first_receive_time = start_time;
        // the initial timeout will be pretty long, but as soon as we receive the first message
        // we'll start to use a much shorter timeout for receiving subsequent messages
        let mut timeout = Duration::from_secs(60);
        let mut total_messages: usize = 0;

        while let Some(message) = self.recv_next(timeout).await {
            if total_messages == 0 {
                first_receive_time = Instant::now();
            }
            total_messages += 1;
            log::trace!("Received: {:?}", message);
            self.handle_received_message(message, to_sync, to_finalize);

            // if we've been going for over a second, then we'll use a super short timeout so that
            // we can start syncing as soon as possible
            let elapsed = first_receive_time.elapsed();
            timeout = Duration::from_millis(500).checked_sub(elapsed).unwrap_or(Duration::from_millis(1));
        }
        let elapsed_millis = duration_to_millis(start_time.elapsed());
        let new_to_sync = to_sync.len() - starting_to_sync_len;
        let new_to_finalize = to_finalize.len() - starting_to_finalize_len;
        log::debug!("Received: {} messages to sync {} and finalize {} new parents in {}ms",
                total_messages, new_to_sync, new_to_finalize, elapsed_millis);
    }

    fn handle_received_message(&mut self, message: ResourceMessage, to_sync: &mut HashSet<String>, to_finalize: &mut HashSet<String>) {
        if message.index_key.is_none() {
            log::error!("Received a message with no index_key: {:?}", message);
            return;
        }
        let ResourceMessage { index_key, event_type, resource_type, resource_id } = message;
        let uid = index_key.unwrap();
        // TODO: check whether there's an in progress update before adding this to the set
        // TODO: check the message type and remove in_progress updates from the state if needed
        match event_type {
            EventType::UpdateOperationComplete => {
                if let Some(prev) = self.in_progress_updates.remove(&uid) {
                    let duration_millis = duration_to_millis(prev.start_time.elapsed());
                    log::info!("Completed sync of parent: {} with uid: {} in {}ms", prev.parent_id, uid, duration_millis);
                } else {
                    log::error!("Got updateOperationComplete when there was no in-progress operation for uid: {}", uid);
                }
                if !to_finalize.contains(&uid) {
                    to_sync.insert(uid);
                } else {
                    log::info!("Sync operation just completed for a parent that was just deleted, will finalize parent: {}", uid);
                }
            }
            _ => {
                // TODO: If this is a parent type, then look at deletionTimestamp to see if the object needs to be finalized, also remove our finalizer from the list when we finalize it
                if !self.is_update_in_progress(uid.as_str()) {
                    if to_sync.insert(uid) {
                        log::info!("Triggering sync due to event: {:?}, on resource: {} {} ", event_type, resource_type, resource_id);
                    }
                }
            }
        }
    }

    async fn recv_next(&mut self, timeout: Duration) -> Option<ResourceMessage> {
        // TODO: handle actual receive errors that are not from timeouts
        match self.receiver.recv().timeout(timeout).await {
            Err(_) => None,
            Ok(val) => val,
        }
    }

}

pub(crate) fn duration_to_millis(duration: Duration) -> u64 {
    let mut millis = duration.as_secs() * 1000;
    let nanos = duration.subsec_nanos() as u64;
    if nanos > 1_000_000 {
        millis = millis.saturating_add(nanos / 1_000_000)
    }
    millis
}
