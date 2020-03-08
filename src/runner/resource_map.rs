use crate::resource::object_id::{ObjectId, ObjectIdRef};
use crate::resource::K8sResource;

use std::collections::HashMap;

#[derive(Debug)]
pub struct IdMap<T>(HashMap<String, HashMap<String, T>>);

pub type ResourceMap = IdMap<K8sResource>;
pub type IdSet = IdMap<()>;

impl<T> IdMap<T> {
    pub fn new() -> IdMap<T> {
        IdMap(HashMap::new())
    }

    pub fn contains<'a>(&self, id: impl Into<ObjectIdRef<'a>>) -> bool {
        let id = id.into();

        self.0.get(id.namespace).map(|by_name| {
            by_name.contains_key(id.name)
        }).unwrap_or(false)
    }

    pub fn remove<'a>(&mut self, id: impl Into<ObjectIdRef<'a>>) {
        let id = id.into();
        if let Some(by_name) = self.0.get_mut(id.namespace) {
            by_name.remove(id.name);
        }
    }

    pub fn clear(&mut self) {
        for by_name in self.0.values_mut() {
            by_name.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.0.values().map(HashMap::len).sum()
    }
}

impl IdMap<K8sResource> {

    pub fn insert(&mut self, resource: K8sResource) -> Option<K8sResource> {
        let ObjectId { namespace, name } = resource.get_object_id().to_owned();
        let by_name = self.0.entry(namespace).or_default();
        by_name.insert(name, resource)
    }

    pub fn get<'a, 'b>(&'a self, id: impl Into<ObjectIdRef<'b>>) -> Option<&'a K8sResource> {
        let id = id.into();
        if let Some(by_name) = self.0.get(id.namespace) {
            by_name.get(id.name)
        } else {
            None
        }
    }

    pub fn get_copy<'a>(&self, id: impl Into<ObjectIdRef<'a>>) -> Option<K8sResource> {
        self.get(id).cloned()
    }
}

impl IdMap<()> {
    pub fn insert(&mut self, id: ObjectId) -> bool {
        let ObjectId { namespace, name } = id;
        let by_name = self.0.entry(namespace).or_default();
        by_name.insert(name, ()).is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = ObjectIdRef> {
        self.0.iter().flat_map(|(namespace, by_name)| {
            by_name.keys().map(move |name| {
                ObjectIdRef {
                    namespace,
                    name,
                }
            })
        })
    }
}


