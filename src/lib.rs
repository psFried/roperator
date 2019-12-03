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
