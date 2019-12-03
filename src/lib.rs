#[macro_use] extern crate serde_derive;

pub mod k8s_types;
pub mod runner;
pub mod resource;
pub mod config;
pub mod handler;
pub mod error;

pub use serde_yaml;
pub use serde_json;
pub use serde;

pub mod prelude {
    pub use crate::error::Error;
    pub use crate::runner::run_operator;
    pub use crate::handler::{SyncRequest, SyncResponse, FinalizeResponse, Handler};
    pub use crate::k8s_types::{self, K8sType};
    pub use crate::config::{OperatorConfig, ClientConfig, UpdateStrategy, ChildConfig};
    pub use crate::resource::K8sResource;
    pub use serde::{Serialize, Deserialize};
}

