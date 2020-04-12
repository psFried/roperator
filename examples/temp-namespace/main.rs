//! Example of using roperator to create an operator for an `TempNamespace` example Custom Resource.
//! When an instance of the TempNamespace CRD is created, the operator will create an actual k8s
//! namespace in response. It will automaticaly delete the namespace after the provided duration.
//!
//! This example uses the `DefaultFailableHandler` to wrap a `FailableHandler` impl. This
//! provides a somewhat more opinionated and simpler interface that makes it easy to have proper
//! error handling and visibility. This requires the "failable" feature to be enabled.
#[macro_use]
extern crate serde_derive;

use chrono::offset::Utc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use roperator::config::{ClientConfig, Credentials, KubeConfig};
use roperator::handler::failable::{DefaultFailableHandler, FailableHandler};
use roperator::prelude::{k8s_types, ChildConfig, K8sType, OperatorConfig, SyncRequest};
use roperator::serde_json::{json, Value};
use serde::de::{self, Deserialize, Deserializer};
use std::time::Duration;

/// Name of our operator, which is automatically added as a label value in all of the child resources we create
const OPERATOR_NAME: &str = "temp-namespace-example";

/// a `K8sType` with basic info about our parent CRD
static PARENT_TYPE: &K8sType = &K8sType {
    api_version: "example.roperator.com/v1alpha1",
    kind: "TempNamespace",
    plural_kind: "tempnamespaces",
};

/// Represents an instance of the CRD that is in the kubernetes cluster.
/// Note that this struct does not need to implement Serialize because the
/// operator will only ever update the `status` subresource
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct TempNamespace {
    pub metadata: ObjectMeta,
    pub spec: TempNamespaceSpec,
    pub status: Option<TempNamespaceStatus>,
}

impl TempNamespace {
    fn get_time_remaining(&self) -> Option<Duration> {
        let created_at = self
            .status
            .as_ref()
            .and_then(|status| status.created_at.as_ref().map(|time| time.0))
            .unwrap_or(Utc::now());

        let elapsed = Utc::now()
            .signed_duration_since(created_at)
            .to_std()
            .unwrap_or(Duration::from_secs(0));
        if self.spec.delete_after > elapsed {
            Some(self.spec.delete_after - elapsed)
        } else {
            None
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct TempNamespaceSpec {
    #[serde(rename = "deleteAfter", deserialize_with = "deserialize_duration")]
    delete_after: Duration,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TempNamespaceStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<Time>,

    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at: Option<Time>,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// Our handler implements the `FailableHandler` trait instead of implementing Handler directly.
// This allows us to express the sync logic a little more easily, and use the
// `DefaultFailableHandler::wrap` function to give us some reasonable defaults for error handling
// and regular re-syncs.
struct TempNsHandler;
impl FailableHandler for TempNsHandler {
    type Error = serde_json::Error;
    type Status = TempNamespaceStatus;

    fn sync_children(&self, request: &SyncRequest) -> Result<Vec<Value>, Self::Error> {
        let temp_ns = request.deserialize_parent::<TempNamespace>()?;
        let time_remaining = temp_ns.get_time_remaining();
        log::info!(
            "Namespace: '{}' has {:?} time remaining",
            request.parent.name(),
            time_remaining
        );
        if time_remaining.is_some() {
            Ok(vec![namespace(&temp_ns)])
        } else {
            Ok(Vec::new())
        }
    }

    fn determine_status(
        &self,
        request: &SyncRequest,
        sync_error: Option<Self::Error>,
    ) -> Self::Status {
        let parent = request.deserialize_parent::<TempNamespace>().ok();
        let time_remaining = parent.as_ref().and_then(|p| p.get_time_remaining());
        let prev_status = parent.and_then(|p| p.status).unwrap_or_default();

        let existing_namespace = request
            .children()
            .of_type(k8s_types::core::v1::Namespace)
            .first();
        let created_at = prev_status
            .created_at
            .clone()
            .or_else(|| existing_namespace.as_ref().map(|_| Time(Utc::now())));
        let deleted_at = prev_status.deleted_at.clone().or_else(|| {
            if time_remaining.is_none() {
                Some(Time(Utc::now()))
            } else {
                None
            }
        });
        let error = sync_error.map(|e| format!("invalid TempNamepsace resource: {}", e));
        TempNamespaceStatus {
            created_at,
            deleted_at,
            error,
        }
    }
}

fn main() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "roperator=info,temp_namespace=debug,warn");
    }
    env_logger::init();

    // create our operator config, which contains the type information of the parent resource and
    // all the child resources we might create.
    let operator_config = OperatorConfig::new(OPERATOR_NAME, PARENT_TYPE)
        .with_child(k8s_types::core::v1::Namespace, ChildConfig::on_delete());

    // This section is not necessary unless you want to run locally against a GKE cluster. This is only
    // provided to make that easier, since it may be a common environment to test against.
    // For most other clusters, you can just use `roperator::runner::run_operator` without providing a ClientConfig
    let client_config_result = if let Ok(token) = std::env::var("ROPERATOR_AUTH_TOKEN") {
        let credentials = Credentials::base64_bearer_token(token);
        let (kubeconfig, kubeconfig_path) = KubeConfig::load().expect("failed to load kubeconfig");
        let kubeconfig_parent_path = kubeconfig_path.parent().unwrap();
        kubeconfig.create_client_config_with_credentials(
            OPERATOR_NAME.to_string(),
            kubeconfig_parent_path,
            credentials,
        )
    } else {
        ClientConfig::from_kubeconfig(OPERATOR_NAME.to_string())
    };
    let client_config =
        client_config_result.expect("failed to resolve cluster data from kubeconfig");

    // the call to `with_regular_resync` will ensure that our handler is invoked every minute,
    // regardless of whether there's been a change detected. We do this so that we can check
    // regularly to see if the namespace needs to be deleted
    let handler =
        DefaultFailableHandler::wrap(TempNsHandler).with_regular_resync(Duration::from_secs(30));
    // now we run the operator, passing in our handler functions
    let err =
        roperator::runner::run_operator_with_client_config(operator_config, client_config, handler);

    // `run_operator_with_client_config` will never return under normal circumstances, so we only need to handle the sad path here
    log::error!("Error running operator: {}", err);
    std::process::exit(1);
}

// returns the desired namespace resource. Nothing fancy here, just the basics
fn namespace(parent: &TempNamespace) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": parent.metadata.name.as_ref().expect("parent name is missing"),
        }
    })
}

// helper to deserialize a std::time::Duration from a string representation
fn deserialize_duration<'de, D>(deserailizer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let as_str = String::deserialize(deserailizer)?;

    if as_str.len() < 2 {
        return Err(de::Error::custom(format!(
            "invalid duration: '{:?}'",
            as_str
        )));
    }
    let (digits, unit) = as_str.split_at(as_str.len() - 1);
    let quantity = digits
        .parse::<u64>()
        .map_err(|e| de::Error::custom(format!("invalid number: '{}'", e)))?;
    let multiplier = match unit.to_ascii_uppercase().as_str() {
        "S" => 1,
        "M" => 60,
        "H" => 60 * 60,
        "D" => 60 * 60 * 24,
        other => return Err(de::Error::custom(format!("invalid unit: '{:?}'", other))),
    };

    Ok(Duration::from_secs(quantity * multiplier))
}
