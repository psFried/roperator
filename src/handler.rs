use crate::resource::{K8sResource, ObjectIdRef, K8sTypeRef};

use serde_json::{Value};
use serde::de::DeserializeOwned;
use serde::Serialize;

use std::marker::PhantomData;

/// The type passed to the Handler that provides a snapshot view of the parent Custom Resource and all of the children
/// as they exist in the Kubernetes cluster. The handler will be passed an immutable reference to this struct.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncRequest {
    /// The parent custom resource instance
    pub parent: K8sResource,
    /// The entire set of children related to this parent instance, as they exist in the cluster at the time.
    /// In the happy path, this will include all of the children that have been returned in a previous `SyncResponse`
    pub children: Vec<K8sResource>,
}

impl SyncRequest {
    /// Deserialize the parent resource as the given type. It's common to have a struct representation of your CRD, so you
    /// don't have to work with the json directly. This function allows you to easily do just that.
    pub fn deserialize_parent<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.parent.clone().into_value())
    }

    /// Returns a view of just the children of this request, which is useful for passing to a function that determines the current
    /// status. The returned view has a variety of functions for accessing individual children and groups of children.
    pub fn children<'a>(&'a self) -> RequestChildren<'a> {
        RequestChildren(self)
    }

    /// Returns an iterator over the child resources that have the given apiVersion and kind
    pub fn iter_children_with_type<'a, 'b: 'a>(&'a self, api_version: &'b str, kind: &'b str) -> impl Iterator<Item=&'a K8sResource> {
        let type_ref = K8sTypeRef::new(api_version, kind);
        self.children.iter().filter(move |child| child.get_type_ref() == type_ref)
    }

    /// Finds a child resource with the given type and id, and returns a reference to the raw json.
    /// The namespace can be an empty str for resources that are not namespaced
    pub fn raw_child<'a, 'b>(&'a self, api_version: &'b str, kind: &'b str, namespace: &'b str, name: &'b str) -> Option<&'a K8sResource> {
        let id = ObjectIdRef::new(namespace, name);
        let type_ref = K8sTypeRef::new(api_version, kind);
        self.children.iter().find(move |child| child.get_type_ref() == type_ref && child.get_object_id() == id)
    }

    /// returns true if the request contains a child with the given type and id.
    /// The namespace can be an empty str for resources that are not namespaced
    pub fn has_child(&self, api_version: &str, kind: &str, namespace: &str, name: &str) -> bool {
        self.raw_child(api_version, kind, namespace, name).is_some()
    }

    /// Finds a child resource with the given type and id, and attempts to deserialize it.
    /// The namespace can be an empty str for resources that are not namespaced
    pub fn deserialize_child<T: DeserializeOwned>(&self, api_version: &str, kind: &str, namespace: &str, name: &str) -> Option<Result<T, serde_json::Error>> {
        self.raw_child(api_version, kind, namespace, name).map(|child| {
            serde_json::from_value(child.clone().into_value())
        })
    }
}

/// A view of a subset of child resouces that share a given apiVersion and kind. This view has accessors
/// for retrieving deserialized child resources.
pub struct TypedView<'a, T: DeserializeOwned> {
    api_version: &'a str,
    kind: &'a str,
    req: &'a SyncRequest,
    _phantom: PhantomData<T>,
}

impl <'a, T: DeserializeOwned> TypedView<'a, T> {
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
        self.req.has_child(self.api_version, self.kind, namespace, name)
    }

    /// Attempts to deserialize the resource with the given namespace and name
    pub fn get(&self, namespace: &str, name: &str) -> Option<Result<T, serde_json::Error>> {
        self.req.deserialize_child(self.api_version, self.kind, namespace, name)
    }

    pub fn first(&self) -> Option<Result<T, serde_json::Error>> {
        let mut iter = self.iter();
        iter.next()
    }

    /// Returns an iterator over the raw `K8sResource` children that match the apiVersion and kind
    pub fn iter_raw(&self) -> impl Iterator<Item=&K8sResource> {
        self.req.iter_children_with_type(self.api_version, self.kind)
    }

    /// Returns an iterator over the child resources that deserializes each of the child resources as the given type
    pub fn iter(&self) -> TypedIter<'a, T> {
        TypedIter::new(self.req)
    }
}

/// An iterator over child resources ov a given type that deserializes each item
pub struct TypedIter<'a, T: DeserializeOwned> {
    req: &'a SyncRequest,
    index: usize,
    _phantom: PhantomData<T>,
}

impl <'a, T: DeserializeOwned> TypedIter<'a, T> {
    fn new(req: &'a SyncRequest) -> TypedIter<'a, T> {
        TypedIter {
            req,
            index: 0,
            _phantom: PhantomData,
        }
    }
}
impl <'a, T: DeserializeOwned> Iterator for TypedIter<'a, T> {
    type Item = Result<T, serde_json::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.req.children.get(self.index).map(|c| serde_json::from_value(c.clone().into_value()));
            self.index += 1;
            match next {
                Some(res) => {
                    return Some(res);
                }
                None => {
                    return None;
                }
            }
        }
    }
}

/// A view of raw `K8sResource` children that share the same apiVersion and kind, which provides
/// convenient accessor functions
pub struct RawView<'a>{
    req: &'a SyncRequest,
    api_version: &'a str,
    kind: &'a str,
}

impl <'a> RawView<'a> {
    /// Returns true if the given child exists in this `SyncRequest`, otherwise false. Namespace
    /// can be an empty str for resources that are not namespaced.
    pub fn exists(&self, namespace: &str, name: &str) -> bool {
        self.req.has_child(self.api_version, self.kind, namespace, name)
    }

    /// Returns a reference to the raw `K8sResource` of this type if it exists.
    /// Namespace can be an empty str for resources that are not namespaced
    pub fn get(&self, namespace: &str, name: &str) -> Option<&K8sResource> {
        self.req.raw_child(self.api_version, self.kind, namespace, name)
    }

    /// Returns an iterator over all of the resources of this type
    pub fn iter(&self) -> impl Iterator<Item=&K8sResource> {
        self.req.iter_children_with_type(self.api_version, self.kind)
    }

    pub fn first(&self) -> Option<&K8sResource> {
        let mut iter = self.iter();
        iter.next()
    }
}

/// A view of all of the children from a `SyncRequest`, which has convenient accessors for getting (and optionally deserializing)
/// child resources. This view represents a snapshot of the known state of all the children that are related to a specific parent.
/// The parent status should be computed from this view, NOT from the _desired_ children returned from your handler.
pub struct RequestChildren<'a>(&'a SyncRequest);
impl <'a> RequestChildren<'a> {

    /// Provides a raw view of all the children with the given apiVersion/kind. This is useful if you don't want to
    /// deserialize the resources.
    pub fn of_type_raw<'b: 'a>(&'a self, api_version: &'b str, kind: &'b str) -> RawView<'a> {
        RawView {
            req: self.0,
            api_version,
            kind
        }
    }

    /// Provides a typed view of all the children with the given apiVersion/kind. This view provides typed accessor functions
    /// to return deserialized children
    pub fn of_type<'b: 'a, T: DeserializeOwned>(&'a self, api_version: &'b str, kind: &'b str) -> TypedView<'a, T> {
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
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
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
/// modify the parent status.
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


#[derive(Debug, Default)]
pub struct ResponseChildren {
    children: Vec<Value>,
    error: Option<failure::Error>,
}

impl <E: Into<failure::Error>> From<Result<Vec<Value>, E>> for ResponseChildren {
     fn from(result: Result<Vec<Value>, E>) -> ResponseChildren {
        match result {
            Ok(children) => {
                ResponseChildren {
                    children,
                    error: None
                }
            }
            Err(err) => {
                ResponseChildren {
                    children: Vec::new(),
                    error: Some(err.into())
                }
            }
        }
     }
}

impl From<Vec<Value>> for ResponseChildren {
    fn from(children: Vec<Value>) -> ResponseChildren {
        ResponseChildren {
            children,
            error: None,
        }
    }
}

pub trait DetermineChildren: 'static + Send + Sync {
    fn determine_children(&self, request: &SyncRequest) -> ResponseChildren;
}

impl <F, R> DetermineChildren for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> R, R: Into<ResponseChildren> {
    fn determine_children(&self, request: &SyncRequest) -> ResponseChildren {
        self(request).into()
    }
}

pub trait DetermineStatus: 'static + Send + Sync {
    fn determine_status(&self, req: &SyncRequest) -> Result<Value, failure::Error>;
}

impl <T, F> DetermineStatus for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> Result<T, failure::Error>, T: Serialize {
    fn determine_status(&self, req: &SyncRequest) -> Result<Value, failure::Error> {
        self(req).and_then(|status| serde_json::to_value(status).map_err(Into::into))
    }
}

pub trait HandleFinalize: 'static + Send + Sync {
    fn finalize(&self, req: &SyncRequest) -> Result<bool, failure::Error>;
}

impl <F, R> HandleFinalize for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> R, Result<bool, failure::Error>: From<R> {
    fn finalize(&self, req: &SyncRequest) -> Result<bool, failure::Error> {
        self(req).into()
    }
}

pub struct HandlerImpl {
    operator_error_status_field: String,
    determine_status: Box<dyn DetermineStatus>,
    determine_children: Box<dyn DetermineChildren>,
    handle_finalize: Box<dyn HandleFinalize>,
}

impl Default for HandlerImpl {
    fn default() -> HandlerImpl {
        HandlerImpl {
            operator_error_status_field: "operatorError".to_owned(),
            determine_status: Box::new(default_status_function),
            determine_children: Box::new(default_determine_children_function),
            handle_finalize: Box::new(default_finalize_function),
        }
    }
}

impl HandlerImpl {
    pub fn determine_status(mut self, determine_status: impl DetermineStatus) -> Self {
        self.determine_status = Box::new(determine_status);
        self
    }

    pub fn determine_children(mut self, determine_children: impl DetermineChildren) -> Self {
        self.determine_children = Box::new(determine_children);
        self
    }

    pub fn handle_finalize(mut self, handle_finalize: impl HandleFinalize) -> Self {
        self.handle_finalize = Box::new(handle_finalize);
        self
    }

    fn make_status_value(&self, req: &SyncRequest) -> Value {
        let status = self.determine_status.determine_status(req).unwrap_or_else(|err| {
            log::error!("Failed to determine status for parent: {}, error: {:?}", req.parent.get_object_id(), err);

            serde_json::json!({
                self.operator_error_status_field.as_str() : format!("Failed to determine status: {}", err),
            })
        });
        ensure_status_is_object_or_null(status)
    }
}

impl Handler for HandlerImpl {
    fn sync(&self, req: &SyncRequest) -> SyncResponse {
        let mut status = self.make_status_value(req);
        let ResponseChildren {children, error} = self.determine_children.determine_children(req);
        if let Some(err) = error {
            log::error!("Failed to determine children for parent: {}, error: {:?}", req.parent.get_object_id(), err);
            add_error_to_status(&mut status, self.operator_error_status_field.as_str(), "Failed to determine children ", err);
        }

        SyncResponse {
            children,
            status
        }
    }

    fn finalize(&self, req: &SyncRequest) -> FinalizeResponse {
        let mut status = self.make_status_value(req);
        match self.handle_finalize.finalize(req) {
            Ok(is_finalized) => {
                FinalizeResponse {
                    status,
                    finalized: is_finalized,
                }
            }
            Err(err) => {
                log::error!("Failed to finalize parent: {}, error: {:?}", req.parent.get_object_id(), err);
                add_error_to_status(&mut status, self.operator_error_status_field.as_str(), "Finalization function failed", err);
                FinalizeResponse {
                    status,
                    finalized: false,
                }
            }
        }
    }
}

fn ensure_status_is_object_or_null(status: Value) -> Value {
    if status.is_object() || status.is_null() {
        status
    } else {
        serde_json::json!({
            "status": status
        })
    }
}

fn add_error_to_status(status: &mut Value, key: &str, context: &str, error: failure::Error) {
    if status.is_null() {
        *status = serde_json::json!({
            key: format!("\"{}: {}\"", context, error)
        })
    } else {
        // status must be an object because we've already ensured that
        let obj = status.as_object_mut().unwrap();
        if let Some(value) = obj.get_mut(key) {
            let new_value = Value::from(format!("Multiple errors: \"{}: {}\", {}", context, error, value));
            *value = new_value;
        } else {
            obj.insert(key.to_owned(), format!("\"{}: {}\"", context, error).into());
        }
    }
}


pub fn default_finalize_function(req: &SyncRequest) -> Result<bool, failure::Error> {
    log::info!("Finalizing parent: {}", req.parent.get_object_id());
    Ok(true)
}

pub fn default_status_function(_req: &SyncRequest) -> Result<Value, failure::Error> {
    Ok(Value::Null)
}

pub fn default_determine_children_function(_req: &SyncRequest) -> ResponseChildren {
    ResponseChildren::default()
}

