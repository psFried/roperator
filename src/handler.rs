//! The `handler` module defines the `Handler` trait. Operators must implement their own `Handler`,
//! which is where the majority of your code will live.
//!
//! The `Handler` functions you implement will both be passed a `SyncRequest` that represents a
//! snapshot of the state of a given parent resource, along with any children that currently exist for it.
//! This request struct has lots of functions on it for accessing and deserializing child resources.
use crate::error::Error;
use crate::resource::{K8sResource, K8sTypeRef, ObjectIdRef};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use std::fmt::{self, Debug};
use std::marker::PhantomData;

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

/// The return value from your handler function, which has the status to set for the parent, as well as any
/// desired child resources. Any existing child resources that are **not** included in this response **will be deleted**.
#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncResponse {
    /// The desired status of your parent resource. If there's any difference between this status and the current status,
    /// the operator will update the status. This status should be computed only from the current view of children in the
    /// `SyncRequest`, not from the _desired_ children that are included in this response.
    pub status: Value,

    /// The entire set of desired child resources that will correspond to the parent from the `SyncRequest`. This response
    /// should only set the fields that your operator actually cares about, since it's possible that other fields may be
    /// set or modified by other controllers or the apiServer. If there's any difference between this _desired_ state and
    /// the _actual_ state described in the `SyncRequest`, then those child resources will be updated based on the `UpdateStrategy`
    /// that was configured for that child type.
    pub children: Vec<Value>,
}

impl Debug for SyncResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let as_string = if f.alternate() {
            serde_json::to_string_pretty(self)
        } else {
            serde_json::to_string(self)
        }
        .map_err(|_| fmt::Error)?;
        write!(f, "SyncResponse: {}", as_string)
    }
}

impl SyncResponse {
    /// Constructs a new empty `SyncResponse` with the given parent status
    pub fn new(status: Value) -> SyncResponse {
        SyncResponse {
            status,
            children: Vec::new(),
        }
    }

    /// Attempts to construct a `SyncResponse` by serializing the given object to use for the parent status.
    pub fn from_status<S: Serialize>(status: S) -> Result<SyncResponse, serde_json::Error> {
        serde_json::to_value(status).map(SyncResponse::new)
    }

    /// Attempts to add a child to the response by serializing the given object
    pub fn add_child<C: Serialize>(&mut self, child: C) -> Result<(), serde_json::Error> {
        serde_json::to_value(child).map(|c| {
            self.children.push(c);
        })
    }
}

/// The response returned from a finalize function. Finalize functions may not return any children, but they may
/// modify the parent status. Note that the status of the parent will only be updated if `finalized` is `false`,
/// since if it's `true` then it typically indicates that the actual deletion of the resource is likely imminent.
#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct FinalizeResponse {
    /// The desired status of the parent. This will be ignored if `finalized` is `true`, but if `finalized` is false,
    /// then the parent status will be updated to this value.
    pub status: Value,
    /// Whether or not it's OK to proceed with deletion of the parent resource. If this is `true`, then roperator will
    /// remove itself from the list of finalizers for the parent, which will allow deletion to proceed. If this is `false`,
    /// then roperator will update the parent status and re-try your finalize function later.
    pub finalized: bool,
}

impl Debug for FinalizeResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let as_string = if f.alternate() {
            serde_json::to_string_pretty(self)
        } else {
            serde_json::to_string(self)
        }
        .map_err(|_| fmt::Error)?;
        write!(f, "FinalizeResponse: {}", as_string)
    }
}

/// The main trait that's used to implement your operator. Most operators will only need to implement
/// the `sync` function.
pub trait Handler: Send + Sync + 'static {
    /// The main logic of an operator is implemented in this function. This function is passed a `&SyncRequest`, which
    /// provides a snapshot of the state of your parent resource and any children that already exist. The `Handler` then
    /// returns a `SyncResponse` that contains the desired child resources and the status to be set on the parent.
    /// Roperator will ensure that the parent status and any child resources all match the values provided by this response.
    ///
    /// If this function returns an `Error`, then neither the status or any child resources will be updated!
    ///
    /// This function is allowed to have side effects, such as calling out to external systems and such, but it's important
    /// that any such operations are idempotent. An example would be an operator that calls out to a service to create a database
    /// schema for each parent custom resource instance. It is a good idea to generate deterministic identifiers for such things
    /// based on some immutable metadata from the parent resource (name, namespace, uid).
    fn sync(&self, request: &SyncRequest) -> Result<SyncResponse, Error>;

    /// Finalize is invoked whenever the parent resource starts being deleted. Roperator makes every reasonable attempt to
    /// ensure that this function gets invoked _at least once_ for each parent as it's being deleted. We cannot make any
    /// guarantees, though, since it's possible for Kuberentes resources to be force deleted without waiting for finalizers.
    ///
    /// The `FinalizeResponse` can return a new parent status, as well as a boolean indicating whether the finalization is
    /// complete. If the boolean is true, then Roperator will remove itself from the list of parent finalizers, allowing
    /// kubernetes to proceed with the deletion. If the boolean is false, then Roperator will re-try finalizing again after
    /// a backoff period.
    ///
    /// The default implementation of this function simply returns `true` and does not modify the status.
    fn finalize(&self, request: &SyncRequest) -> Result<FinalizeResponse, Error> {
        Ok(FinalizeResponse {
            status: request.parent.status().cloned().unwrap_or(Value::Null),
            finalized: true,
        })
    }
}

impl<F> Handler for F
where
    F: Fn(&SyncRequest) -> Result<SyncResponse, Error> + Send + Sync + 'static,
{
    fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
        self(req)
    }
}
