#[macro_use] extern crate serde_derive;

pub mod runner;
pub mod resource;
pub mod config;
pub mod handler;

pub use serde_yaml;
pub use serde_json;
pub use serde;


pub mod prelude {
    pub use crate::runner::run_operator;
    pub use crate::handler::{SyncRequest, SyncResponse, FinalizeResponse, Handler};
    pub use crate::config::{K8sType, OperatorConfig, ClientConfig, UpdateStrategy, ChildConfig};
    pub use serde::{Serialize, Deserialize};
}

