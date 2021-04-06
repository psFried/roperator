use crate::k8s_types::K8sType;
use crate::resource::{InvalidResourceError, K8sResource, ObjectId};
use anyhow::Error;

#[cfg(feature = "testkit")]
use crate::resource::ObjectIdRef;

use crate::runner::client::{ApiError, Client, Error as ClientError, ObjectList, WatchEvent};
use crate::runner::metrics::WatcherMetrics;
use crate::runner::resource_map::{IdSet, ResourceMap};

use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{error::SendError, Sender};
use tokio::sync::{Mutex, MutexGuard};

use std::collections::HashMap;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct LabelToIdIndex {
    label_name: String,
    entries: HashMap<String, IdSet>,
}

impl LabelToIdIndex {
    pub fn new(label_name: String) -> Self {
        Self {
            label_name,
            entries: HashMap::new(),
        }
    }
}

impl ReverseIndex for LabelToIdIndex {
    type Value = IdSet;

    fn get_key<'a, 'b>(&'a self, res: &'b K8sResource) -> Option<&'b str> {
        res.get_label_value(self.label_name.as_str())
    }

    fn insert(&mut self, key: &str, res: &K8sResource) {
        let set = self
            .entries
            .entry(key.to_owned())
            .or_insert_with(IdSet::new);
        let id = res.get_object_id();
        if !set.contains(&id) {
            set.insert(id.to_owned());
        }
    }

    fn remove_one(&mut self, key: &str, id: &ObjectId) {
        if let Some(set) = self.entries.get_mut(key) {
            set.remove(id);
        }
    }

    fn remove_all(&mut self, key: &str) -> Option<IdSet> {
        self.entries.remove(key)
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn lookup<'a, 'b>(&'a self, key: &'b str) -> Option<&'a IdSet> {
        self.entries.get(key)
    }
}

#[derive(Debug)]
pub struct UidToIdIndex(HashMap<String, ObjectId>);

impl UidToIdIndex {
    pub fn new() -> UidToIdIndex {
        UidToIdIndex(HashMap::new())
    }
}

impl ReverseIndex for UidToIdIndex {
    type Value = ObjectId;

    fn get_key<'a, 'b>(&'a self, res: &'b K8sResource) -> Option<&'b str> {
        Some(res.uid())
    }

    fn insert(&mut self, key: &str, value: &K8sResource) {
        self.0
            .insert(key.to_owned(), value.get_object_id().to_owned());
    }

    fn remove_one(&mut self, key: &str, _: &ObjectId) {
        self.0.remove(key);
    }

    fn remove_all(&mut self, key: &str) -> Option<Self::Value> {
        self.0.remove(key)
    }

    fn clear(&mut self) {
        self.0.clear();
    }

    fn lookup<'a, 'b>(&'a self, key: &'b str) -> Option<&'a Self::Value> {
        self.0.get(key)
    }
}

pub trait ReverseIndex: Send + 'static {
    type Value: std::fmt::Debug + 'static;

    fn get_key<'a, 'b>(&'a self, res: &'b K8sResource) -> Option<&'b str>;
    fn insert(&mut self, key: &str, res: &K8sResource);
    fn remove_one(&mut self, key: &str, id: &ObjectId);
    fn remove_all(&mut self, key: &str) -> Option<Self::Value>;
    fn clear(&mut self);
    fn lookup<'a, 'b>(&'a self, key: &'b str) -> Option<&'a Self::Value>;
}

#[derive(Debug)]
struct CacheAndIndex<I: ReverseIndex> {
    cache: ResourceMap,
    index: I,
    error: Option<Error>,
    is_initialized: bool,
}

impl<I: ReverseIndex> CacheAndIndex<I> {
    fn new(index: I) -> Self {
        CacheAndIndex {
            error: Some(MonitorBackendErr::StateUnininitialized.into_boxed_error()),
            cache: ResourceMap::new(),
            index,
            is_initialized: false,
        }
    }

    fn add(&mut self, resource: K8sResource) {
        if let Some(key) = self.index.get_key(&resource) {
            self.index.insert(key, &resource);
        }
        self.cache.insert(resource);
    }

    fn remove(&mut self, id: &ObjectId, resource: &K8sResource) {
        let key = self.index.get_key(resource);
        if let Some(k) = key {
            self.index.remove_one(k, &id);
        }
        self.cache.remove(id);
    }

    fn clear_all(&mut self) {
        self.index.clear();
        self.cache.clear();
        if let Some(err) = self.error.take() {
            log::info!("Clearing previous_error: {}", err);
        }
    }

    fn resource_count(&self) -> usize {
        self.cache.len()
    }
}

impl CacheAndIndex<UidToIdIndex> {
    fn get_by_uid(&self, uid: &str) -> Option<K8sResource> {
        let CacheAndIndex {
            ref index,
            ref cache,
            ..
        } = *self;
        index.lookup(uid).and_then(|id| cache.get_copy(id))
    }
}

impl CacheAndIndex<LabelToIdIndex> {
    pub fn get_all_resources_by_index_key(&self, key: &str) -> Vec<K8sResource> {
        let mut results = Vec::new();
        if let Some(ids) = self.index.lookup(key) {
            for id in ids.iter() {
                let obj = self
                    .cache
                    .get_copy(id)
                    .expect("cache and index are inconsistent");
                results.push(obj);
            }
        }
        results
    }
}

#[derive(Debug)]
pub enum EventType {
    Created,
    Updated,
    Finalizing,
    Deleted,
    UpdateOperationComplete {
        result: Result<Option<Duration>, ()>,
    },
    TriggerResync {
        resync_round: u32,
    },
}

#[derive(Debug)]
pub struct ResourceMessage {
    pub event_type: EventType,
    pub resource_type: &'static K8sType,
    pub resource_id: ObjectId,
    pub index_key: Option<String>,
}

pub struct ResourceState<'a, I: ReverseIndex>(MutexGuard<'a, CacheAndIndex<I>>);

impl<'a, I: ReverseIndex> ResourceState<'a, I> {
    #[cfg(feature = "testkit")]
    pub fn get_by_id(&self, id: &ObjectIdRef<'_>) -> Option<K8sResource> {
        self.0.cache.get_copy(id)
    }
}

impl<'a> ResourceState<'a, UidToIdIndex> {
    pub fn get_by_uid(&self, uid: &str) -> Option<K8sResource> {
        self.0.get_by_uid(uid)
    }
}

impl<'a> ResourceState<'a, LabelToIdIndex> {
    pub fn get_all_resources_by_index_key(&self, key: &str) -> Vec<K8sResource> {
        self.0.get_all_resources_by_index_key(key)
    }
}

#[derive(Debug, Clone)]
pub struct ResourceMonitor<I: ReverseIndex> {
    cache_and_index: Arc<Mutex<CacheAndIndex<I>>>,
}

impl<I: ReverseIndex> ResourceMonitor<I> {
    pub async fn lock_state(&self) -> Result<ResourceState<'_, I>, Error> {
        let mut lock = self.cache_and_index.lock().await;
        if let Some(err) = lock.error.take() {
            Err(err)
        } else if !lock.is_initialized {
            Err(MonitorBackendErr::StateUnininitialized.into_boxed_error())
        } else {
            Ok(ResourceState(lock))
        }
    }
}

#[derive(Debug)]
enum MonitorBackendErr {
    SendErr,
    ClientErr(ClientError),
    ResourceVersionExpired,
    InvalidResource(InvalidResourceError),
    Api(ApiError),
    StateUnininitialized,
}

impl Display for MonitorBackendErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MonitorBackendErr::SendErr => f.write_str("Sender channel closed"),
            MonitorBackendErr::StateUnininitialized => f.write_str("Resource cache has not been initialized or may be temporarily recovering from an error"),
            MonitorBackendErr::ClientErr(err) => write!(f, "Client Error: {}", err),
            MonitorBackendErr::ResourceVersionExpired => f.write_str("Resource Version has expired, watcher is out of sync"),
            MonitorBackendErr::InvalidResource(e) => write!(f, "Invalid resource returned from api server: {}", e),
            MonitorBackendErr::Api(e) => write!(f, "Watcher received api error: {}", e)
        }
    }
}

impl std::error::Error for MonitorBackendErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MonitorBackendErr::ClientErr(err) => Some(err),
            MonitorBackendErr::InvalidResource(e) => Some(e),
            MonitorBackendErr::Api(e) => Some(e),
            _ => None,
        }
    }
}

impl MonitorBackendErr {
    fn into_boxed_error(self) -> Error {
        Error::new(self)
    }

    fn is_resource_version_expired(&self) -> bool {
        match self {
            MonitorBackendErr::ResourceVersionExpired => true,
            _ => false,
        }
    }

    fn is_send_err(&self) -> bool {
        match self {
            MonitorBackendErr::SendErr => true,
            _ => false,
        }
    }
}

impl From<ApiError> for MonitorBackendErr {
    fn from(err: ApiError) -> MonitorBackendErr {
        if err.code == 410 {
            MonitorBackendErr::ResourceVersionExpired
        } else {
            MonitorBackendErr::Api(err)
        }
    }
}

impl From<InvalidResourceError> for MonitorBackendErr {
    fn from(err: InvalidResourceError) -> MonitorBackendErr {
        MonitorBackendErr::InvalidResource(err)
    }
}

impl From<ClientError> for MonitorBackendErr {
    fn from(err: ClientError) -> MonitorBackendErr {
        if err.is_http_410() {
            MonitorBackendErr::ResourceVersionExpired
        } else {
            MonitorBackendErr::ClientErr(err)
        }
    }
}

impl<T> From<SendError<T>> for MonitorBackendErr {
    fn from(_: SendError<T>) -> MonitorBackendErr {
        MonitorBackendErr::SendErr
    }
}

pub fn start_child_monitor(
    executor: Handle,
    label_name: String,
    namespace: Option<String>,
    k8s_type: &'static K8sType,
    client: Client,
    sender: Sender<ResourceMessage>,
    watcher_metrics: WatcherMetrics,
) -> ResourceMonitor<LabelToIdIndex> {
    let index = LabelToIdIndex::new(label_name.clone());
    start_monitor(
        executor,
        index,
        k8s_type,
        namespace,
        Some(label_name),
        client,
        sender,
        watcher_metrics,
    )
}

pub fn start_parent_monitor(
    executor: Handle,
    namespace: Option<String>,
    k8s_type: &'static K8sType,
    client: Client,
    sender: Sender<ResourceMessage>,
    watcher_metrics: WatcherMetrics,
) -> ResourceMonitor<UidToIdIndex> {
    start_monitor(
        executor,
        UidToIdIndex::new(),
        k8s_type,
        namespace,
        None,
        client,
        sender,
        watcher_metrics,
    )
}

#[allow(clippy::too_many_arguments)]
fn start_monitor<I: ReverseIndex>(
    executor: Handle,
    index: I,
    k8s_type: &'static K8sType,
    namespace: Option<String>,
    label_selector: Option<String>,
    client: Client,
    sender: Sender<ResourceMessage>,
    watcher_metrics: WatcherMetrics,
) -> ResourceMonitor<I> {
    let cache_and_index = Arc::new(Mutex::new(CacheAndIndex::new(index)));
    let frontend = ResourceMonitor {
        cache_and_index: cache_and_index.clone(),
    };

    let backend = ResourceMonitorBackend {
        metrics: watcher_metrics,
        cache_and_index,
        client,
        k8s_type,
        sender,
        label_selector,
        namespace,
    };
    executor.spawn(Box::pin(async move {
        backend.run().await;
    }));
    frontend
}

struct ResourceMonitorBackend<I: ReverseIndex> {
    metrics: WatcherMetrics,
    cache_and_index: Arc<Mutex<CacheAndIndex<I>>>,
    client: Client,
    k8s_type: &'static K8sType,
    sender: Sender<ResourceMessage>,
    label_selector: Option<String>,
    namespace: Option<String>,
}

impl<I: ReverseIndex> ResourceMonitorBackend<I> {
    async fn run(mut self) {
        log::debug!(
            "Starting monitoring resources of type: {:?} with selector: {:?}",
            self.k8s_type,
            self.label_selector
        );

        loop {
            let result = self.seed_cache().await;
            match result {
                Ok(resource_version) => {
                    let result = self.run_inner(resource_version).await;
                    log::info!("Watch ended with result: {:?}", result);
                    if let Err(err) = result {
                        if !self.handle_error(err).await {
                            break;
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "Error seeding cache for type: {:?}: {:?}",
                        self.k8s_type,
                        err
                    );
                    if !self.handle_error(err).await {
                        break;
                    }
                }
            }
        }
        log::info!("Ending monitor for resources: {:?}", self.k8s_type);
    }

    async fn handle_error(&mut self, error: MonitorBackendErr) -> bool {
        let is_http_410 = error.is_resource_version_expired();
        let is_send_err = error.is_send_err();
        log::error!(
            "Error in monitor for type: {:?}, err: {:?}",
            self.k8s_type,
            error
        );
        let mut lock = self.cache_and_index.lock().await;
        lock.error = Some(error.into_boxed_error());
        lock.is_initialized = false;

        if !is_http_410 {
            self.metrics.error();
            let duration = std::time::Duration::from_secs(10);
            tokio::time::sleep(duration).await;
        }
        // if it's a send error, then we'll return false so that we can stop the loop
        !is_send_err
    }

    async fn run_inner(&mut self, mut resource_version: String) -> Result<(), MonitorBackendErr> {
        loop {
            self.metrics.request_started();
            let result = self.do_watch(&resource_version).await;
            log::debug!(
                "Watch of {:?} ended with result: {:?}",
                self.k8s_type,
                result
            );

            match result {
                Ok(Some(vers)) => {
                    resource_version = vers;
                }
                Ok(None) => {}
                Err(MonitorBackendErr::ResourceVersionExpired) => {
                    log::warn!("ResourceVersion is too old for type: {:?}", self.k8s_type);
                    return Err(MonitorBackendErr::ResourceVersionExpired);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }

    async fn do_watch(
        &mut self,
        resource_version: &str,
    ) -> Result<Option<String>, MonitorBackendErr> {
        log::debug!(
            "Starting watch of: {:?} with resourceVersion: {:?}",
            self.k8s_type,
            resource_version
        );

        let mut lines = self
            .client
            .watch(
                &*self.k8s_type,
                self.namespace.as_ref().map(String::as_str),
                Some(resource_version),
                self.label_selector.as_ref().map(String::as_str),
            )
            .await?;

        let mut new_version: Option<String> = None;
        loop {
            let maybe_next = lines.next().await;
            if let Some(result) = maybe_next {
                self.metrics.event_received();
                let event = result?;
                let event_version = self.handle_event(event).await?;
                new_version = Some(event_version);
            } else {
                break;
            }
        }
        Ok(new_version)
    }

    async fn handle_event(&mut self, event: WatchEvent) -> Result<String, MonitorBackendErr> {
        let (event_type, object) = match event {
            WatchEvent::Added(res) => (EventType::Created, res),
            WatchEvent::Deleted(res) => (EventType::Deleted, res),
            WatchEvent::Modified(res) => (get_update_event_type(&res), res),
            WatchEvent::Error(err) => {
                log::warn!(
                    "Got apiError for watch on : {:?}, err: {:?}",
                    self.k8s_type,
                    err
                );
                return Err(err.into());
            }
        };
        let resource = K8sResource::from_value(object)?;
        let resource_version = resource.resource_version().to_owned();

        let resource_id = resource.get_object_id().to_owned();
        let resource_type = self.k8s_type;
        let mut cache_and_index = self.cache_and_index.lock().await;
        let index_key = cache_and_index.index.get_key(&resource).map(String::from);

        match event_type {
            EventType::Deleted => {
                cache_and_index.remove(&resource_id, &resource);
            }
            _ => {
                cache_and_index.add(resource);
            }
        }

        self.metrics
            .set_resource_count(cache_and_index.resource_count());
        let to_send = ResourceMessage {
            event_type,
            resource_type,
            resource_id,
            index_key,
        };
        self.sender.send(to_send).await?;
        Ok(resource_version)
    }

    async fn seed_cache(&mut self) -> Result<String, MonitorBackendErr> {
        log::info!(
            "Seeding resources of type: {:?} with selector: {:?}",
            self.k8s_type,
            self.label_selector
        );
        // lock the cache now and hold it until we're done, so that consumers don't get an inconsistent view of it
        let mut cache_and_index = self.cache_and_index.lock().await;
        cache_and_index.is_initialized = false;
        cache_and_index.clear_all();

        self.metrics.request_started();
        let list = self
            .client
            .list_all(
                &*self.k8s_type,
                self.namespace.as_ref().map(String::as_str),
                self.label_selector.as_ref().map(String::as_str),
            )
            .await?;
        // safe unwrap since RawApi can only fail when setting the request body, but it's hard coded to an empty veec
        let ObjectList { metadata, items } = list;
        let resource_version = metadata
            .resource_version
            .ok_or_else(|| InvalidResourceError {
                message: "list result from api server is missing metadata.resourceVersion",
                value: Value::Null,
            })?;

        for mut object in items {
            self.add_metadata_to_list_object(&mut object)?;
            let resource = K8sResource::from_value(object)?;
            let index_key = cache_and_index.index.get_key(&resource).map(String::from);
            let event_type = get_update_event_type(resource.as_ref());
            let resource_type = self.k8s_type;
            let resource_id = resource.get_object_id().to_owned();
            let message = ResourceMessage {
                event_type,
                resource_type,
                resource_id,
                index_key,
            };

            cache_and_index.add(resource);
            self.sender.send(message).await?;
        }
        self.metrics
            .set_resource_count(cache_and_index.resource_count());
        // set the initialization flag, which will allow the frontend to read from the cache
        cache_and_index.is_initialized = true;
        // drop the cache_and_index lock when we exit this function, which allows consumers to read from it
        Ok(resource_version)
    }

    /// For some reason, it seems that apiVersion and kind are missing from the individual response items in the list response
    fn add_metadata_to_list_object(
        &self,
        list_object: &mut Value,
    ) -> Result<(), InvalidResourceError> {
        match list_object.as_object_mut() {
            Some(obj) => {
                obj.insert(
                    "apiVersion".to_owned(),
                    self.k8s_type.api_version.to_string().into(),
                );
                obj.insert("kind".to_owned(), self.k8s_type.kind.to_string().into());
                Ok(())
            }
            None => Err(InvalidResourceError::new(
                "list item must be an object",
                list_object.clone(),
            )),
        }
    }
}

fn get_update_event_type(resource: &Value) -> EventType {
    if is_finalizing(resource) {
        EventType::Finalizing
    } else {
        EventType::Updated
    }
}

fn is_finalizing(resource: &Value) -> bool {
    resource.pointer("/metadata/deletionTimestamp").is_some()
}
