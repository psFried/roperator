use crate::resource::{K8sResource, K8sTypeRef, ObjectIdRef};

use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::fmt::{self, Debug};


/// The type passed to the Handler that provides a snapshot view of the parent Custom Resource and all of the children
/// as they exist in the Kubernetes cluster. The handler will be passed an immutable reference to this struct.
#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncRequest {
    /// The parent custom resource instance
    pub parent: K8sResource,
    /// The entire set of children related to this parent instance, as they exist in the cluster at the time.
    /// In the happy path, this will include all of the children that have been returned in a previous `SyncResponse`
    pub children: Vec<K8sResource>,
}

impl Debug for SyncRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("SyncRequst: ")?;
        let as_string = if f.alternate() {
            serde_json::to_string_pretty(self)
        } else {
            serde_json::to_string(self)
        }
        .map_err(|_| fmt::Error)?;

        f.write_str(as_string.as_str())
    }
}

impl SyncRequest {
    /// Deserialize the parent resource as the given type. It's common to have a struct representation of your CRD, so you
    /// don't have to work with the json directly. This function allows you to easily do just that.
    pub fn deserialize_parent<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.parent.clone().into_value())
    }

    /// Returns a view of just the children of this request, which is useful for passing to a function that determines the current
    /// status. The returned view has a variety of functions for accessing individual children and groups of children.
    pub fn children(&self) -> RequestChildren {
        RequestChildren(self)
    }

    /// Returns an iterator over the child resources that have the given apiVersion and kind
    pub fn iter_children_with_type<'a, 'b: 'a>(
        &'a self,
        api_version: &'b str,
        kind: &'b str,
    ) -> impl Iterator<Item = &'a K8sResource> {
        let type_ref = K8sTypeRef::new(api_version, kind);
        self.children
            .iter()
            .filter(move |child| child.get_type_ref() == type_ref)
    }

    /// Finds a child resource with the given type and id, and returns a reference to the raw json.
    /// The namespace can be an empty str for resources that are not namespaced
    pub fn raw_child<'a, 'b>(
        &'a self,
        api_version: &'b str,
        kind: &'b str,
        namespace: &'b str,
        name: &'b str,
    ) -> Option<&'a K8sResource> {
        let id = ObjectIdRef::new(namespace, name);
        let type_ref = K8sTypeRef::new(api_version, kind);
        self.children
            .iter()
            .find(move |child| child.get_type_ref() == type_ref && child.get_object_id() == id)
    }

    /// returns true if the request contains a child with the given type and id.
    /// The namespace can be an empty str for resources that are not namespaced
    pub fn has_child(&self, api_version: &str, kind: &str, namespace: &str, name: &str) -> bool {
        self.raw_child(api_version, kind, namespace, name).is_some()
    }

    /// Finds a child resource with the given type and id, and attempts to deserialize it.
    /// The namespace can be an empty str for resources that are not namespaced
    pub fn deserialize_child<T: DeserializeOwned>(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Option<Result<T, serde_json::Error>> {
        self.raw_child(api_version, kind, namespace, name)
            .map(|child| serde_json::from_value(child.clone().into_value()))
    }
}

/// A view of a subset of child resouces that share a given apiVersion and kind. This view has accessors
/// for retrieving deserialized child resources.
#[derive(Debug)]
pub struct TypedView<'a, T: DeserializeOwned> {
    api_version: &'a str,
    kind: &'a str,
    req: &'a SyncRequest,
    _phantom: PhantomData<T>,
}

impl<'a, T: DeserializeOwned> TypedView<'a, T> {
    fn new(req: &'a SyncRequest, api_version: &'a str, kind: &'a str) -> Self {
        TypedView {
            api_version,
            kind,
            req,
            _phantom: PhantomData,
        }
    }

    /// Returns true if the given child exists in this `SyncRequest`, otherwise false. Namespace
    /// can be an empty str for resources that are not namespaced.
    pub fn exists(&self, namespace: &str, name: &str) -> bool {
        self.req
            .has_child(self.api_version, self.kind, namespace, name)
    }

    /// Attempts to deserialize the resource with the given namespace and name
    pub fn get(&self, namespace: &str, name: &str) -> Option<Result<T, serde_json::Error>> {
        self.req
            .deserialize_child(self.api_version, self.kind, namespace, name)
    }

    pub fn first(&self) -> Option<Result<T, serde_json::Error>> {
        let mut iter = self.iter();
        iter.next()
    }

    /// Returns an iterator over the raw `K8sResource` children that match the apiVersion and kind
    pub fn iter_raw(&self) -> impl Iterator<Item = &K8sResource> {
        self.req
            .iter_children_with_type(self.api_version, self.kind)
    }

    /// Returns an iterator over the child resources that deserializes each of the child resources as the given type
    pub fn iter(&self) -> TypedIter<'a, T> {
        TypedIter::new(self.req)
    }
}

/// An iterator over child resources ov a given type that deserializes each item
#[derive(Debug)]
pub struct TypedIter<'a, T: DeserializeOwned> {
    req: &'a SyncRequest,
    index: usize,
    _phantom: PhantomData<T>,
}

impl<'a, T: DeserializeOwned> TypedIter<'a, T> {
    fn new(req: &'a SyncRequest) -> TypedIter<'a, T> {
        TypedIter {
            req,
            index: 0,
            _phantom: PhantomData,
        }
    }
}
impl<'a, T: DeserializeOwned> Iterator for TypedIter<'a, T> {
    type Item = Result<T, serde_json::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self
            .req
            .children
            .get(self.index)
            .map(|c| serde_json::from_value(c.clone().into_value()));
        self.index += 1;
        next
    }
}

/// A view of raw `K8sResource` children that share the same apiVersion and kind, which provides
/// convenient accessor functions
#[derive(Debug)]
pub struct RawView<'a> {
    api_version: &'a str,
    kind: &'a str,
    req: &'a SyncRequest,
}

impl<'a> RawView<'a> {
    /// Returns true if the given child exists in this `SyncRequest`, otherwise false. Namespace
    /// can be an empty str for resources that are not namespaced.
    pub fn exists(&self, namespace: &str, name: &str) -> bool {
        self.req
            .has_child(self.api_version, self.kind, namespace, name)
    }

    /// Returns a reference to the raw `K8sResource` of this type if it exists.
    /// Namespace can be an empty str for resources that are not namespaced
    pub fn get(&self, namespace: &str, name: &str) -> Option<&K8sResource> {
        self.req
            .raw_child(self.api_version, self.kind, namespace, name)
    }

    /// Returns an iterator over all of the resources of this type
    pub fn iter(&self) -> impl Iterator<Item = &K8sResource> {
        self.req
            .iter_children_with_type(self.api_version, self.kind)
    }

    pub fn first(&self) -> Option<&K8sResource> {
        let mut iter = self.iter();
        iter.next()
    }
}

/// A view of all of the children from a `SyncRequest`, which has convenient accessors for getting (and optionally deserializing)
/// child resources. This view represents a snapshot of the known state of all the children that are related to a specific parent.
/// The parent status should be computed from this view, NOT from the _desired_ children returned from your handler.
#[derive(Debug)]
pub struct RequestChildren<'a>(&'a SyncRequest);
impl<'a> RequestChildren<'a> {
    /// Provides a raw view of all the children with the given apiVersion/kind. This is useful if you don't want to
    /// deserialize the resources.
    pub fn of_type_raw<'b: 'a>(&'a self, api_version: &'b str, kind: &'b str) -> RawView<'a> {
        RawView {
            req: self.0,
            api_version,
            kind,
        }
    }

    /// Provides a typed view of all the children with the given apiVersion/kind. This view provides typed accessor functions
    /// to return deserialized children
    pub fn of_type<'b: 'a, T: DeserializeOwned>(
        &'a self,
        api_version: &'b str,
        kind: &'b str,
    ) -> TypedView<'a, T> {
        TypedView::new(&self.0, api_version, kind)
    }

    /// Returns true if the given child exists in this `SyncRequest`, otherwise false. Namespace
    /// can be an empty str for resources that are not namespaced.
    pub fn exists(&self, api_version: &str, kind: &str, namespace: &str, name: &str) -> bool {
        self.0.has_child(api_version, kind, namespace, name)
    }
}





