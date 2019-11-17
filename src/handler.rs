use crate::resource::{K8sResource, ObjectIdRef, K8sTypeRef};

use serde_json::{Value};


#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncRequst {
    pub parent: K8sResource,
    pub children: Vec<K8sResource>,
}

impl SyncRequst {
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
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncResponse {
    pub status: Value,
    pub children: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct FinalizeResponse {
    pub status: Value,
    pub finalized: bool,
}


pub trait Handler: Send + Sync + 'static {
    fn sync(&self, request: &SyncRequst) -> SyncResponse;
    fn finalize(&self, request: &SyncRequst) -> FinalizeResponse;
}


