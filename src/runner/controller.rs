use crate::resource::{K8sResource, ObjectId, ObjectIdRef, InvalidResourceError, K8sTypeRef};
use crate::config::{K8sType, ChildConfig};
use crate::handler::Handler;
use crate::runner::client::{Client, LineDeserializer, Error as ClientError};

use kube::api::{Informer, Object, TypeMeta, ListParams, ObjectList, RawApi};

use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};
use tokio::sync::mpsc::{Sender, Receiver, channel, error::SendError};
use failure::Error;
use http::Request;
use hyper::Body;

use std::borrow::{Borrow, Cow};
use std::sync::{Arc};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

// type K8sResource = Object<Value, Value>;



// impl <'a> std::borrow::Borrow<ObjectIdRef<'a>> for ObjectId {
//     fn borrow(&self) -> &ObjectIdRef<'a> {
//         &(*self)
//     }
// }


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

    pub fn get_copy(&self, id: &ObjectIdRef) -> Option<K8sResource> {
        self.0.get(id).cloned()
    }

    pub fn get_all(&self) -> Vec<K8sResource> {
        self.0.values().cloned().collect()
    }

    pub fn get<'a, 'b>(&'a self, id: &'b ObjectId) -> Option<&'a K8sResource> {
        self.0.get(id)
    }

    pub fn remove(&mut self, id: &ObjectId) -> Option<K8sResource> {
        self.0.remove(id)
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
        let mut set = self.entries.entry(key.to_owned()).or_insert(HashSet::new());
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

    fn get_all(&self) -> Vec<K8sResource> {
        self.cache.get_all()
    }

    pub fn get_resource<'a, 'b>(&'a self, id: &'b ObjectId) -> Option<&'a K8sResource> {
        self.cache.get(id)
    }

    fn add(&mut self, resource: K8sResource) {
        if let Some(key) = self.index.get_key(&resource) {
            self.index.insert(key, &resource);
        }
        self.cache.insert(resource);
    }

    fn remove_one(&mut self, index_key: &str, id: &ObjectId) {
        self.index.remove_one(index_key, id);
        self.cache.remove(id);
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
    fn remove_all(&mut self, index_key: &str)  {
        if let Some(ids) = self.index.remove_all(index_key) {
            for id in ids {
                self.cache.remove(&id);
            }

        }
    }

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
    Deleted,
    UpdateOperationComplete,
    CacheRefreshed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceMessage {
    pub event_type: EventType,
    pub resource_type: Arc<K8sType>,
    pub resource_id: ObjectId,
    pub index_key: Option<String>,
}


pub struct ResourceState<'a, I: ReverseIndex>(MutexGuard<'a, CacheAndIndex<I>>);
impl <'a, I: ReverseIndex> ResourceState<'a, I> {

    pub fn get_all(&self) -> Vec<K8sResource> {
        self.0.get_all()
    }

    pub fn get_by_id(&'a self, id: &ObjectId) -> Option<&'a K8sResource> {
        self.0.get_resource(id)
    }
}

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
    let list_params = make_child_list_params(&label_name);
    let index = LabelToIdIndex::new(label_name);
    start_monitor(index, list_params, k8s_type, namespace, client, sender).await
}

pub async fn start_parent_monitor(namespace: Option<String>, k8s_type: Arc<K8sType>, client: Client, sender: Sender<ResourceMessage>) -> ResourceMonitor<UidToIdIndex> {
    start_monitor(UidToIdIndex::new(), ListParams::default(), k8s_type, namespace, client, sender).await
}

async fn start_monitor<I: ReverseIndex>(index: I, list_params: ListParams, k8s_type: Arc<K8sType>, namespace: Option<String>, client: Client, sender: Sender<ResourceMessage>) -> ResourceMonitor<I> {

    let cache_and_index = Arc::new(Mutex::new(CacheAndIndex::new(index)));
    let frontend = ResourceMonitor { cache_and_index: cache_and_index.clone() };
    let mut raw_api = to_raw_api(&*k8s_type);
    raw_api.namespace = namespace;

    let mut backend = ResourceMonitorBackend {
        raw_api,
        cache_and_index,
        client,
        k8s_type,
        sender,
        list_params,
    };
    hyper::rt::spawn(async move {
        backend.run();
    });
    frontend
}

fn make_child_list_params(label_selector: &str) -> ListParams {
    ListParams {
        label_selector: Some(urlencoding::encode(label_selector)),
        timeout: Some(30),
        field_selector: None,
        include_uninitialized: false,
    }
}

struct ResourceMonitorBackend<I: ReverseIndex> {
    cache_and_index: Arc<Mutex<CacheAndIndex<I>>>,
    client: Client,
    k8s_type: Arc<K8sType>,
    raw_api: RawApi,
    sender: Sender<ResourceMessage>,
    list_params: ListParams,
}

impl <I: ReverseIndex> ResourceMonitorBackend<I> {

    async fn run(mut self) {
        log::debug!("Starting monitoring resources of type: {:?} with selector: {:?}", self.k8s_type, self.list_params.label_selector);

        loop {
            let result = self.seed_cache().await;
            match result {
                Ok(resource_version) => {
                    let result = self.run_inner(resource_version).await;
                    log::info!("Watch ended with result: {:?}", result);
                    if let Err(err) = result {
                        self.handle_error(err).await || break;
                    }
                }
                Err(err) => {
                    log::error!("Error seeding cache for type: {:?}: {:?}", self.k8s_type, err);
                    self.handle_error(err).await || break;
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
        let req = self.raw_api.watch(&self.list_params, resource_version).unwrap();

        let mut lines = self.client.get_response_lines_deserialized::<WatchEvent>(convert_request(req)).await?;

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
            WatchEvent::Modified(res) => (EventType::Updated, res),
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
        cache_and_index.add(resource);

        let to_send = ResourceMessage { event_type, resource_type, resource_id, index_key };
        self.sender.send(to_send).await?;
        Ok(resource_version)
    }

    async fn seed_cache(&mut self) -> Result<String, MonitorBackendErr> {
        log::info!("Seeding resources of type: {:?} with selector: {:?}", self.k8s_type, self.list_params.label_selector);
        // lock the cache now and hold it until we're done, so that consumers don't get an inconsistent view of it
        let mut cache_and_index = self.cache_and_index.lock().await;
        cache_and_index.is_initialized = false;
        cache_and_index.clear_all();

        // safe unwrap since RawApi can only fail when setting the request body, but it's hard coded to an empty veec
        let req = self.raw_api.list(&self.list_params).unwrap();
        let list: ObjectList<Value> = self.client.get_response_body(convert_request(req)).await?;
        let ObjectList {metadata, items} = list;
        let resource_version = metadata.resourceVersion.ok_or_else(|| {
            InvalidResourceError {
                message: "list result from api server is missing metadata.resourceVersion",
                value: Value::Null,
            }
        })?;

        for object in items {
            let resource = K8sResource::from_value(object)?;
            cache_and_index.add(resource);
        }

        // set the initialization flag, which will allow the frontend to read from the cache
        cache_and_index.is_initialized = true;
        // drop the cache_and_index lock when we exit this function, which allows consumers to read from it
        Ok(resource_version)
    }

}


#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "type", content = "object", rename_all = "UPPERCASE")]
enum WatchEvent {
    Added(Value),
    Modified(Value),
    Deleted(Value),
    Error(ApiError),
}


#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ApiError {
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub reason: String,
    pub code: u16,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Api Error: status: '{}', code: {}, reason: '{}', message: '{}'", self.status, self.code, self.reason, self.message)
    }
}
impl std::error::Error for ApiError { }

pub(crate) fn convert_request(req: Request<Vec<u8>>) -> Request<Body> {
    let (parts, bod) = req.into_parts();
    Request::from_parts(parts, Body::from(bod))
}

pub(crate) fn to_raw_api(k8s_type: &K8sType) -> kube::api::RawApi {
    let prefix = if k8s_type.group.len() > 0 {
        "apis"
    } else {
        "api"
    };
    kube::api::RawApi {
        resource: k8s_type.plural_kind.clone(),
        group: k8s_type.group.clone(),
        namespace: None,
        version: k8s_type.version.clone(),
        prefix: prefix.to_owned(),
    }
}
