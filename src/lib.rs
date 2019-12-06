//! Roperator lets you easily write Kubernetes Operators that manage a potentially complex set of
//! _child_ Kuberntes resources for each instance of a _parent_ Custom Resource.
//!
//! To get started, all you need is an `OperatorConfig` and a `Handler` implementation.
//!
//! Busybox operator example:
//! ```no_run
//! use roperator::prelude::*;
//! use roperator::serde_json::json;
//!
//! /// Name of our operator, which is automatically added as a label value in all of the child resources we create
//! const OPERATOR_NAME: &str = "echoserver-example";
//!
//! // a `K8sType` with basic info about our parent CRD
//! static PARENT_TYPE: &K8sType = &K8sType {
//!    api_version: "example.roperator.com/v1alpha1",
//!    kind: "BusyBox",
//!    plural_kind: "busyboxes",
//! };
//!
//! // In your main function create the OperatorConfig, which tells Roperator about
//! // the types of your parent and child resources, and about how to deal with
//! // each type of child resource when it needs updated
//! let operator_config = OperatorConfig::new(OPERATOR_NAME, PARENT_TYPE)
//!     .with_child(k8s_types::core::v1::Pod, ChildConfig::recreate());
//!
//! // this function will block the current thread indifinitely while the operator runs
//! run_operator(operator_config, handle_sync);
//!
//! fn handle_sync(request: &SyncRequest) -> Result<SyncResponse, Error> {
//!     // for this tiny example, we'll only create a single Pod. You can also use any of the types
//!     // defined in the k8s_openapi crate, which has serializable structs for all the usual resources
//!     let pod = json!({
//!         "metadata": {
//!             "namespace": request.parent.namespace(),
//!             "name": format!("{}-busybox", request.parent.name()),
//!         },
//!         "spec": {
//!             "containers": [
//!                 {
//!                     "name": "busybox",
//!                     "image": "busybox:latest",
//!                     "command": [
//!                         "bash",
//!                         "-c",
//!                         format!("while true; do; echo 'Hello from {}'; sleep 10; done;", request.parent.name()),
//!                     ]
//!                 }
//!             ]
//!         }
//!     });
//!     let status = json!({
//!         // normally, we'd derive the status by taking a look at the existing `children` in the request
//!         "message": "everything looks good here!",
//!     });
//!     Ok(SyncResponse {
//!         status,
//!         children: vec![pod],
//!     })
//! }
//! ```
//!
//! The main function in most operators just needs to call `roperator::runner::run_operator` or
//! `roperator::runner::run_operator_with_client_config`, passing the `OperatorConfig` and your `Handler` implementation.
//!

#[macro_use]
extern crate serde_derive;

pub mod config;
pub mod error;
pub mod handler;
pub mod k8s_types;
pub mod resource;
pub mod runner;

pub use serde;
pub use serde_json;
pub use serde_yaml;

pub mod prelude {
    pub use crate::config::{ChildConfig, ClientConfig, OperatorConfig, UpdateStrategy};
    pub use crate::error::Error;
    pub use crate::handler::{FinalizeResponse, Handler, SyncRequest, SyncResponse};
    pub use crate::k8s_types::{self, K8sType};
    pub use crate::resource::K8sResource;
    pub use crate::runner::run_operator;
    pub use serde::{Deserialize, Serialize};
}
