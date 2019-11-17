use crate::resource::{K8sResource, ObjectIdRef, K8sTypeRef};

use serde_json::{Value};
use serde::de::DeserializeOwned;
use serde::Serialize;


#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncRequest {
    pub parent: K8sResource,
    pub children: Vec<K8sResource>,
}

impl SyncRequest {
    pub fn deserialize_parent<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.parent.clone().into_value())
    }

    pub fn children<'a, 'b: 'a, T: DeserializeOwned>(&'a self, api_version: &'b str, kind: &'b str) -> TypedView<'a, T> {
        TypedView::new(self, api_version, kind)
    }

    pub fn iter_children_with_type<'a, 'b: 'a>(&'a self, api_version: &'b str, kind: &'b str) -> impl Iterator<Item=&'a K8sResource> {
        let type_ref = K8sTypeRef::new(api_version, kind);
        self.children.iter().filter(move |child| child.get_type_ref() == type_ref)
    }

    pub fn find_child<'a, 'b>(&'a self, api_version: &'b str, kind: &'b str, namespace: &'b str, name: &'b str) -> Option<&'a K8sResource> {
        let id = ObjectIdRef::new(namespace, name);
        let type_ref = K8sTypeRef::new(api_version, kind);
        self.children.iter().find(move |child| child.get_type_ref() == type_ref && child.get_object_id() == id)
    }

    pub fn has_child(&self, api_version: &str, kind: &str, namespace: &str, name: &str) -> bool {
        self.find_child(api_version, kind, namespace, name).is_some()
    }

    pub fn deserialize_child<T: DeserializeOwned>(&self, api_version: &str, kind: &str, namespace: &str, name: &str) -> Option<Result<T, serde_json::Error>> {
        self.find_child(api_version, kind, namespace, name).map(|child| {
            serde_json::from_value(child.clone().into_value())
        })
    }
}

pub struct TypedView<'a, T: DeserializeOwned> {
    api_version: &'a str,
    kind: &'a str,
    req: &'a SyncRequest,
    _phantom: std::marker::PhantomData<T>,
}

impl <'a, T: DeserializeOwned> TypedView<'a, T> {
    fn new(req: &'a SyncRequest, api_version: &'a str, kind: &'a str) -> Self {
        TypedView {
            api_version,
            kind,
            req,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<Result<T, serde_json::Error>> {
        self.req.deserialize_child(self.api_version, self.kind, namespace, name)
    }

    pub fn get_all(&self) -> Result<Vec<T>, serde_json::Error> {
        self.req.iter_children_with_type(self.api_version, self.kind).map(|child| {
            serde_json::from_value(child.clone().into_value())
        }).collect()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncResponse {
    pub status: Value,
    pub children: Vec<Value>,
}

impl SyncResponse {
    pub fn new(status: Value) -> SyncResponse {
        SyncResponse {
            status,
            children: Vec::new(),
        }
    }

    pub fn from_status<S: Serialize>(status: S) -> Result<SyncResponse, serde_json::Error> {
        serde_json::to_value(status).map(SyncResponse::new)
    }

    pub fn add_child<C: Serialize>(&mut self, child: C) -> Result<(), serde_json::Error> {
        serde_json::to_value(child).map(|c| {
            self.children.push(c);
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct FinalizeResponse {
    pub status: Value,
    pub finalized: bool,
}


pub trait Handler: Send + Sync + 'static {
    fn sync(&self, request: &SyncRequest) -> SyncResponse;
    fn finalize(&self, request: &SyncRequest) -> FinalizeResponse;
}

impl <F> Handler for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> SyncResponse {
    fn sync(&self, request: &SyncRequest) -> SyncResponse {
        self(request)
    }
    fn finalize(&self, req: &SyncRequest) -> FinalizeResponse {
        log::info!("Ignoring finalize request for parent: {} since no finalize behavior was defined",
                req.parent.get_object_id());

        FinalizeResponse {
            status: Value::Null,
            finalized: true,
        }
    }
}
