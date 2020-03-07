//! The `handler` module defines the `Handler` trait. Operators must implement their own `Handler`,
//! which is where the majority of your code will live.
//!
//! The `Handler` functions you implement will both be passed a `SyncRequest` that represents a
//! snapshot of the state of a given parent resource, along with any children that currently exist for it.
//! This request struct has lots of functions on it for accessing and deserializing child resources.

mod request;

use crate::error::Error;
use serde::Serialize;
use serde_json::Value;
use std::fmt::{self, Debug};

pub use self::request::{RawView, RequestChildren, SyncRequest, TypedIter, TypedView};

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
