use crate::resource::{K8sResource, ObjectIdRef, K8sTypeRef};

use serde_json::{Value};
use serde::de::DeserializeOwned;
use serde::Serialize;

use std::fmt::Debug;
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

/// Trait for errors that can be returned from a Handler function. This just enforces all of the
/// bounds we need in order to convert the errors into trait objects and to send them between threads.
pub trait HandlerError: std::error::Error + Send + 'static {}
impl <T> HandlerError for T where T: std::error::Error + Send + 'static {}

/// An error that can be returned from a `Handler` function. We use trait objects
/// for handler errors to keep the interfaces clean and simple. Most common error
/// types can be easily converted into `Box<dyn HandlerError>` without any trouble.
pub type Error = Box<dyn HandlerError>;

impl <T> From<T> for Error where T: HandlerError {
    fn from(e: T) -> Error {
        Box::new(e)
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

// impl <F> Handler for F where F: Fn(&SyncRequest) -> SyncResponse + Send + Sync + 'static {
//     fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
//         Ok(self(req))
//     }
// }

impl <F> Handler for F where F: Fn(&SyncRequest) -> Result<SyncResponse, Error> + Send + Sync + 'static {
    fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
        self(req)
    }
}


// #[derive(Debug)]
// pub struct ImpossibleError;
// impl Display for ImpossibleError {
//     fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
//         unimplemented!()
//     }
// }
// impl std::error::Error for ImpossibleError {}

// pub trait DetermineChildren: 'static + Send + Sync {
//     type Error: std::error::Error;

//     fn determine_children(&self, request: &SyncRequest) -> Result<Vec<Value>, Self::Error>;
// }

// impl <F> DetermineChildren for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> Vec<Value> {
//     type Error = ImpossibleError;
//     fn determine_children(&self, request: &SyncRequest) -> Result<Vec<Value>, Self::Error> {
//         Ok(self(request))
//     }
// }

// impl <F, E> DetermineChildren for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> Result<Vec<Value>, E>, E: std::error::Error {
//     type Error = E;

//     fn determine_children(&self, request: &SyncRequest) -> Result<Vec<Value>, Self::Error> {
//         self(request)
//     }
// }

// pub trait DetermineStatus: 'static + Send + Sync {
//     type Error: std::error::Error;

//     fn determine_status(&self, req: &SyncRequest) -> Result<Value, Self::Error>;
// }

// impl <T, F, E> DetermineStatus for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> Result<Value, E>, T: Serialize, E: std::error::Error {
//     type Error = E;

//     fn determine_status(&self, req: &SyncRequest) -> Result<Value, Self::Error> {
//         self(req)
//     }
// }

// pub trait HandleFinalize: 'static + Send + Sync {
//     fn finalize(&self, req: &SyncRequest) -> Result<bool, failure::Error>;
// }

// impl <F, R> HandleFinalize for F where F: 'static + Send + Sync + Fn(&SyncRequest) -> R, Result<bool, failure::Error>: From<R> {
//     fn finalize(&self, req: &SyncRequest) -> Result<bool, failure::Error> {
//         self(req).into()
//     }
// }

// /// A convenient `Handler` implementation that breaks down the sync handling into smaller parts,
// /// and also does some error handling.
// pub struct HandlerImpl {
//     pub operator_error_status_field: String,
//     pub determine_status: Box<dyn DetermineStatus>,
//     pub determine_children: Box<dyn DetermineChildren>,
//     pub handle_finalize: Box<dyn HandleFinalize>,
// }

// impl Default for HandlerImpl {
//     fn default() -> HandlerImpl {
//         HandlerImpl {
//             operator_error_status_field: "operatorError".to_owned(),
//             determine_status: Box::new(default_status_function),
//             determine_children: Box::new(default_determine_children_function),
//             handle_finalize: Box::new(default_finalize_function),
//         }
//     }
// }

// impl HandlerImpl {

//     pub fn error_status_field(mut self, field_name: impl Into<String>) -> Self {
//         self.operator_error_status_field = field_name.into();
//         self
//     }

//     pub fn determine_status(mut self, determine_status: impl DetermineStatus) -> Self {
//         self.determine_status = Box::new(determine_status);
//         self
//     }

//     pub fn determine_children(mut self, determine_children: impl DetermineChildren) -> Self {
//         self.determine_children = Box::new(determine_children);
//         self
//     }

//     pub fn handle_finalize(mut self, handle_finalize: impl HandleFinalize) -> Self {
//         self.handle_finalize = Box::new(handle_finalize);
//         self
//     }

//     fn make_status_value(&self, req: &SyncRequest) -> Value {
//         let status = self.determine_status.determine_status(req).unwrap_or_else(|err| {
//             log::error!("Failed to determine status for parent: {}, error: {:?}", req.parent.get_object_id(), err);

//             serde_json::json!({
//                 self.operator_error_status_field.as_str() : format!("Failed to determine status: {}", err),
//             })
//         });
//         ensure_status_is_object_or_null(status)
//     }
// }

// impl <E: std::error::Error> Handler for HandlerImpl {
//     type Error = E;

//     fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, E> {
//         let mut status = self.make_status_value(req);
//         let ResponseChildren {children, error} = self.determine_children.determine_children(req);
//         if let Some(err) = error {
//             log::error!("Failed to determine children for parent: {}, error: {:?}", req.parent.get_object_id(), err);
//             add_error_to_status(&mut status, self.operator_error_status_field.as_str(), "Failed to determine children ", err);
//         }

//         Ok(SyncResponse {
//             children,
//             status
//         })
//     }

//     fn finalize(&self, req: &SyncRequest) -> FinalizeResponse {
//         let mut status = self.make_status_value(req);
//         match self.handle_finalize.finalize(req) {
//             Ok(is_finalized) => {
//                 FinalizeResponse {
//                     status,
//                     finalized: is_finalized,
//                 }
//             }
//             Err(err) => {
//                 log::error!("Failed to finalize parent: {}, error: {:?}", req.parent.get_object_id(), err);
//                 add_error_to_status(&mut status, self.operator_error_status_field.as_str(), "Finalization function failed", err);
//                 FinalizeResponse {
//                     status,
//                     finalized: false,
//                 }
//             }
//         }
//     }
// }

// fn ensure_status_is_object_or_null(status: Value) -> Value {
//     if status.is_object() || status.is_null() {
//         status
//     } else {
//         serde_json::json!({
//             "status": status
//         })
//     }
// }

// fn add_error_to_status(status: &mut Value, key: &str, context: &str, error: failure::Error) {
//     if status.is_null() {
//         *status = serde_json::json!({
//             key: format!("\"{}: {}\"", context, error)
//         })
//     } else {
//         // status must be an object because we've already ensured that
//         let obj = status.as_object_mut().unwrap();
//         if let Some(value) = obj.get_mut(key) {
//             let new_value = Value::from(format!("Multiple errors: \"{}: {}\", {}", context, error, value));
//             *value = new_value;
//         } else {
//             obj.insert(key.to_owned(), format!("\"{}: {}\"", context, error).into());
//         }
//     }
// }
