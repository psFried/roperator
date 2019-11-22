use crate::resource::{K8sResource, ObjectId, ObjectIdRef, InvalidResourceError};
use crate::config::{K8sType};
use crate::runner::client::{Client, Error as ClientError, WatchEvent, ApiError, ObjectList};

use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};
use tokio::sync::mpsc::{Sender, error::SendError};
use failure::Error;

use std::sync::{Arc};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::fmt::{self, Debug};


#[derive(Debug)]
struct ResourceCache(HashMap<ObjectId, K8sResource>);

impl ResourceCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, res: K8sResource) -> Option<K8sResource> {
        let id = res.get_object_id();
        self.0.insert(id.into_owned(), res)
    }

    pub fn remove(&mut self, id: &ObjectId) {
        self.0.remove(id);
    }

    pub fn get_copy(&self, id: &ObjectIdRef) -> Option<K8sResource> {
        self.0.get(id).cloned()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}


#[derive(Debug)]
pub struct LabelToIdIndex {
    label_name: String,
    entries: HashMap<String, HashSet<ObjectId>>,
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
    type Value = HashSet<ObjectId>;

    fn get_key<'a, 'b>(&'a self, res: &'b K8sResource) -> Option<&'b str> {
        res.get_label_value(self.label_name.as_str())
    }

    fn insert(&mut self, key: &str, res: &K8sResource) {
        let set = self.entries.entry(key.to_owned()).or_insert(HashSet::new());
        let id = res.get_object_id();
        if !set.contains(&id) {
            set.insert(id.into_owned());
        }
    }

    fn remove_one(&mut self, key: &str, id: &ObjectId) {
        if let Some(set) = self.entries.get_mut(key) {
            set.remove(id);
        }
    }

    fn remove_all(&mut self, key: &str) -> Option<HashSet<ObjectId>> {
        self.entries.remove(key)
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn lookup<'a, 'b>(&'a self, key: &'b str) -> Option<&'a HashSet<ObjectId>> {
        self.entries.get(key)
    }

}

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
        self.0.insert(key.to_owned(), value.get_object_id().into_owned());
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
    cache: ResourceCache,
    index: I,
    error: Option<failure::Error>,
    is_initialized: bool,
}

impl <I: ReverseIndex> CacheAndIndex<I> {

    fn new(index: I) -> Self {
        CacheAndIndex {
            error: Some(failure::format_err!("Not in sync yet")),
            cache: ResourceCache::new(),
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
        self.cache.remove(&id);
    }

    fn clear_all(&mut self) {
        self.index.clear();
        self.cache.clear();
        if let Some(err) = self.error.take() {
            log::info!("Clearing previous_error: {}", err);
        }
    }
}

impl CacheAndIndex<UidToIdIndex> {
    fn get_by_uid(&self, uid: &str) -> Option<K8sResource> {
        let CacheAndIndex {ref index, ref cache, .. } = *self;
        index.lookup(uid).and_then(|id| cache.get_copy(id))
    }
}

impl CacheAndIndex<LabelToIdIndex> {
    pub fn get_all_resources_by_index_key(&self, key: &str) -> Vec<K8sResource> {
        let mut results = Vec::new();
        if let Some(ids) = self.index.lookup(key) {
            for id in ids {
                let obj = self.cache.get_copy(id).expect("cache and index are inconsistent");
                results.push(obj);
            }
        }
        results
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Created,
    Updated,
    Finalizing,
    Deleted,
    UpdateOperationComplete,
}

#[derive(Clone, PartialEq)]
pub struct ResourceMessage {
    pub event_type: EventType,
    pub resource_type: Arc<K8sType>,
    pub resource_id: ObjectId,
    pub index_key: Option<String>,
}

impl Debug for ResourceMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({:?}, {}, {}, {:?})", std::any::type_name::<ResourceMessage>(),
                self.event_type, self.resource_type, self.resource_id, self.index_key)
    }
}


pub struct ResourceState<'a, I: ReverseIndex>(MutexGuard<'a, CacheAndIndex<I>>);

impl <'a> ResourceState<'a, UidToIdIndex> {
    pub fn get_by_uid(&self, uid: &str) -> Option<K8sResource> {
        self.0.get_by_uid(uid)
    }
}

impl <'a> ResourceState<'a, LabelToIdIndex> {
    pub fn get_all_resources_by_index_key(&self, key: &str) -> Vec<K8sResource> {
        self.0.get_all_resources_by_index_key(key)
    }
}

#[derive(Debug, Clone)]
pub struct ResourceMonitor<I: ReverseIndex> {
    cache_and_index: Arc<Mutex<CacheAndIndex<I>>>,
}


impl <I: ReverseIndex> ResourceMonitor<I> {

    pub async fn lock_state(&self) -> Result<ResourceState<'_, I>, Error> {
        let mut lock = self.cache_and_index.lock().await;
        if let Some(err) = lock.error.take() {
            Err(err)
        } else if !lock.is_initialized {
            Err(failure::format_err!("Resource state is uninitialized"))
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
}

impl MonitorBackendErr {
    fn into_failure_error(self) -> Error {
        match self {
            MonitorBackendErr::SendErr => failure::format_err!("Sender channel closed"),
            MonitorBackendErr::ClientErr(e) => e.into(),
            MonitorBackendErr::ResourceVersionExpired => failure::format_err!("ResourceVersion is expired"),
            MonitorBackendErr::InvalidResource(e) => failure::format_err!("Invalid resource: {}", e),
            MonitorBackendErr::Api(e) => e.into(),
        }
    }

    fn is_resource_version_expired(&self) -> bool {
        match self {
            &MonitorBackendErr::ResourceVersionExpired => true,
            _ => false,
        }
    }

    fn is_send_err(&self) -> bool {
        match self {
            &MonitorBackendErr::SendErr => true,
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

impl From<SendError> for MonitorBackendErr {
    fn from(_: SendError) -> MonitorBackendErr {
        MonitorBackendErr::SendErr
    }
}


pub async fn start_child_monitor(label_name: String, namespace: Option<String>, k8s_type: Arc<K8sType>, client: Client, sender: Sender<ResourceMessage>) -> ResourceMonitor<LabelToIdIndex> {
    let index = LabelToIdIndex::new(label_name.clone());
    start_monitor(index, k8s_type, namespace, Some(label_name), client, sender).await
}

pub async fn start_parent_monitor(namespace: Option<String>, k8s_type: Arc<K8sType>, client: Client, sender: Sender<ResourceMessage>) -> ResourceMonitor<UidToIdIndex> {
    start_monitor(UidToIdIndex::new(), k8s_type, namespace, None, client, sender).await
}

async fn start_monitor<I: ReverseIndex>(index: I, k8s_type: Arc<K8sType>, namespace: Option<String>, label_selector: Option<String>, client: Client, sender: Sender<ResourceMessage>) -> ResourceMonitor<I> {

    let cache_and_index = Arc::new(Mutex::new(CacheAndIndex::new(index)));
    let frontend = ResourceMonitor { cache_and_index: cache_and_index.clone() };

    let backend = ResourceMonitorBackend {
        cache_and_index,
        client,
        k8s_type,
        sender,
        label_selector,
        namespace,
    };
    tokio::spawn(async move {
        backend.run().await;
    });
    frontend
}

struct ResourceMonitorBackend<I: ReverseIndex> {
    cache_and_index: Arc<Mutex<CacheAndIndex<I>>>,
    client: Client,
    k8s_type: Arc<K8sType>,
    sender: Sender<ResourceMessage>,
    label_selector: Option<String>,
    namespace: Option<String>,
}

impl <I: ReverseIndex> ResourceMonitorBackend<I> {

    async fn run(mut self) {
        log::debug!("Starting monitoring resources of type: {:?} with selector: {:?}", self.k8s_type, self.label_selector);

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
                    log::error!("Error seeding cache for type: {:?}: {:?}", self.k8s_type, err);
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
        log::error!("Error in monitor for type: {:?}, err: {:?}", self.k8s_type, error);
        let mut lock = self.cache_and_index.lock().await;
        lock.error = Some(error.into_failure_error());
        lock.is_initialized = false;

        if !is_http_410 {
            let when = tokio::clock::now() + std::time::Duration::from_secs(10);
            tokio::timer::delay(when).await;
        }
        // if it's a send error, then we'll return false so that we can stop the loop
        !is_send_err
    }

    async fn run_inner(&mut self, mut resource_version: String) -> Result<(), MonitorBackendErr> {
        loop {
            let result = self.do_watch(&resource_version).await;
            log::debug!("Watch of {:?} ended with result: {:?}", self.k8s_type, result);

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

    async fn do_watch(&mut self, resource_version: &str) -> Result<Option<String>, MonitorBackendErr> {
        log::debug!("Starting watch of: {:?} with resourceVersion: {:?}", self.k8s_type, resource_version);

        let mut lines = self.client.watch(&*self.k8s_type,
                self.namespace.as_ref().map(String::as_str),
                Some(resource_version),
                self.label_selector.as_ref().map(String::as_str)).await?;

        let mut new_version: Option<String> = None;
        loop {
            let maybe_next = lines.next().await;
            if let Some(result) = maybe_next {
                let event = result?;
                let event_version = self.handle_event(event).await?;
                new_version = Some(event_version);
            } else {
                break;
            }
        }
        Ok(new_version)
    }

    async fn handle_event(&mut self, event: WatchEvent) -> Result<String,  MonitorBackendErr> {
        let (event_type, object) = match event {
            WatchEvent::Added(res) => (EventType::Created, res),
            WatchEvent::Deleted(res) => (EventType::Deleted, res),
            WatchEvent::Modified(res) => (get_update_event_type(&res), res),
            WatchEvent::Error(err) => {
                log::warn!("Got apiError for watch on : {:?}, err: {:?}", self.k8s_type, err);
                return Err(err.into());
            }
        };
        let resource = K8sResource::from_value(object)?;
        let resource_version = resource.get_resource_version().to_owned();

        let resource_id = resource.get_object_id().into_owned();
        let resource_type = self.k8s_type.clone();
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

        let to_send = ResourceMessage { event_type, resource_type, resource_id, index_key };
        self.sender.send(to_send).await?;
        Ok(resource_version)
    }

    async fn seed_cache(&mut self) -> Result<String, MonitorBackendErr> {
        log::info!("Seeding resources of type: {:?} with selector: {:?}", self.k8s_type, self.label_selector);
        // lock the cache now and hold it until we're done, so that consumers don't get an inconsistent view of it
        let mut cache_and_index = self.cache_and_index.lock().await;
        cache_and_index.is_initialized = false;
        cache_and_index.clear_all();


        let list = self.client.list_all(&*self.k8s_type, self.namespace.as_ref().map(String::as_str), self.label_selector.as_ref().map(String::as_str)).await?;
        // safe unwrap since RawApi can only fail when setting the request body, but it's hard coded to an empty veec
        let ObjectList {metadata, items} = list;
        let resource_version = metadata.resource_version.ok_or_else(|| {
            InvalidResourceError {
                message: "list result from api server is missing metadata.resourceVersion",
                value: Value::Null,
            }
        })?;

        for mut object in items {
            self.add_metadata_to_list_object(&mut object)?;
            let resource = K8sResource::from_value(object)?;
            let index_key = cache_and_index.index.get_key(&resource).map(String::from);
            let event_type = get_update_event_type(resource.as_ref());
            let resource_type = self.k8s_type.clone();
            let resource_id = resource.get_object_id().into_owned();
            let message = ResourceMessage { event_type, resource_type, resource_id, index_key };

            cache_and_index.add(resource);
            self.sender.send(message).await?;
        }

        // set the initialization flag, which will allow the frontend to read from the cache
        cache_and_index.is_initialized = true;
        // drop the cache_and_index lock when we exit this function, which allows consumers to read from it
        Ok(resource_version)
    }

    /// For some reason, it seems that apiVersion and kind are missing from the individual response items in the list response
    fn add_metadata_to_list_object(&self, list_object: &mut Value) -> Result<(), InvalidResourceError> {
        match list_object.as_object_mut() {
            Some(obj) => {
                obj.insert("apiVersion".to_owned(), self.k8s_type.format_api_version().into());
                obj.insert("kind".to_owned(), self.k8s_type.kind.clone().into());
                Ok(())
            }
            None => {
                Err(InvalidResourceError::new("list item must be an object", list_object.clone()))
            }
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
