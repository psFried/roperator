//! Example of using roperator to create an operator for an `EchoServer` example Custom Resource
#[macro_use]
extern crate serde_derive;

use roperator::config::{ClientConfig, Credentials, KubeConfig};
use roperator::prelude::{
    k8s_types, ChildConfig, Error, K8sType, OperatorConfig, SyncRequest, SyncResponse,
};
use roperator::serde_json::{json, Value};
use std::time::Duration;

/// Name of our operator, which is automatically added as a label value in all of the child resources we create
const OPERATOR_NAME: &str = "echoserver-example";

/// a `K8sType` with basic info about our parent CRD
static PARENT_TYPE: &K8sType = &K8sType {
    api_version: "example.roperator.com/v1alpha1",
    kind: "EchoServer",
    plural_kind: "echoservers",
};

/// Represents an instance of the CRD that is in the kubernetes cluster.
/// Note that this struct does not need to implement Serialize because the
/// operator will only ever update the `status` subresource
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct EchoServer {
    pub metadata: Metadata,
    pub spec: EchoServerSpec,
    pub status: Option<EchoServerStatus>,
}

/// defines only the fields we care about from the metadata. We could also just use the `ObjectMeta` struct from the `k8s_openapi` crate.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct Metadata {
    pub namespace: String,
    pub name: String,
}

/// The spec of our CRD
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EchoServerSpec {
    pub service_name: String,
}

/// Represents the status of a parent EchoServer instance
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct EchoServerStatus {
    pub message: String,
}

fn main() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "roperator=info,warn");
    }
    env_logger::init();

    // first create an `OperatorConfig` struct that holds the types of the parent and child resources we'll be dealing with.
    // The `OperatorConfig` also has a few other fields that allow customizations of the behavior of the operator.
    // For each child type, we'll also specify the `UpdateStrategy`, which determines the behavior after a change is detected
    // between the desired and actual state of a resource of that type. For example, Pods cannot generally be updated in-place,
    // but must be deleted and re-created.
    let operator_config = OperatorConfig::new(OPERATOR_NAME, PARENT_TYPE)
        .with_child(k8s_types::core::v1::Pod, ChildConfig::recreate())
        .with_child(k8s_types::core::v1::Service, ChildConfig::replace());

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

    // now we run the operator, passing in our handler functions
    let err = roperator::runner::run_operator_with_client_config(
        operator_config,
        client_config,
        (handle_sync, handle_error),
    );

    // `run_operator_with_client_config` will never return under normal circumstances, so we only need to handle the sad path here
    log::error!("Error running operator: {}", err);
    std::process::exit(1);
}

/// This function will invoked by the operator any time there's a change to any parent or child resources.
/// This just needs to return the desired parent status as well as the desired state for any children.
fn handle_sync(request: &SyncRequest) -> Result<SyncResponse, Error> {
    log::info!("Got sync request: {:?}", request);
    let status = json!({
        "message": get_current_status_message(request),
        "phase": "Running",
    });
    let children = get_desired_children(request)?;
    Ok(SyncResponse {
        status,
        children,
        resync: None,
    })
}

/// This function gets called by the operator whenever the sync handler responds with an error.
/// It needs to respond with the appropriate status for the given request and error and the minimum length of
/// time to wait before calling `handle_sync` again.
fn handle_error(request: &SyncRequest, err: Error) -> (Value, Option<Duration>) {
    log::error!("Failed to process request: {:?}\nCause: {:?}", request, err);

    let status = json!({
        "message": err.to_string(),
        "phase": "Error",
    });

    (status, None)
}

/// Returns the json value that should be set on the parent EchoServer
fn get_current_status_message(request: &SyncRequest) -> String {
    let pod = request.children().of_type(("v1", "Pod")).first();
    pod.and_then(|p| p.pointer("/status/message").and_then(Value::as_str))
        .unwrap_or("Waiting for Pod to be initialized")
        .to_owned()
}

/// Returns the children that we want for the given parent
fn get_desired_children(request: &SyncRequest) -> Result<Vec<Value>, Error> {
    let custom_resource: EchoServer = request.deserialize_parent()?;
    let service_name = custom_resource.spec.service_name.as_str();

    let echo_server_name = custom_resource.metadata.name.as_str();
    let echo_server_namespace = custom_resource.metadata.namespace.as_str();

    // For this example, we'll just write JSON directly, but you could also feel free to use the structs
    // from the `k8s_openapi` crate, which defines types for pretty much all the Kuberentes resource types.
    // You can also use anything that implements `serde::Serialize`, but writing JSON directly is also fine.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": echo_server_namespace,
            "name": echo_server_name,
            "labels": {
                "app.kubernetes.io/name": service_name,
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "echo-server",
                    "image": "kennship/http-echo:latest",
                    "env": [
                        {
                            "name": "SERVICE_NAME",
                            "value": service_name,
                        }
                    ],
                    "resources": {
                        "requests": {
                            "cpu": "100m",
                            "memory": "50Mi"
                        }
                    }
                }
            ]
        }
    });

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "namespace": echo_server_namespace,
            "name": echo_server_name,
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {
                "app.kubernetes.io/name": service_name,
            },
            "ports": [
                {
                    "port": 80,
                    "targetPort": 3000,
                }
            ]
        }
    });

    Ok(vec![pod, service])
}
