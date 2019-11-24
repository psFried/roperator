//! Example of using roperator to create an operator for an `EchoServer` example Custom Resource
#[macro_use] extern crate serde_derive;

use roperator::prelude::{
    run_operator,
    OperatorConfig, ChildConfig, ClientConfig,
    K8sType, K8sResource, HandlerImpl, SyncRequest, SyncResponse,
};
use roperator::serde_json::{json, Value};
use roperator::failure::Error;

use std::sync::Arc;

/// Name of our operator, which is automatically added as a label value in all of the child resources we create
const OPERATOR_NAME: &str = "echoserver-example";

/// module describing our EchoServer CRD
mod echo_server {
    use roperator::config::K8sType;

    const VERSION: &str = "v1alpha1";
    const GROUP: &str = "example.roperator.com";
    const KIND: &str = "EchoServer";
    const PLURAL_KIND: &str = "echoservers";

    pub fn k8s_type() -> K8sType {
        K8sType::new(GROUP, VERSION, KIND, PLURAL_KIND)
    }

    /// Represents an instance of the CRD that is in the kubernetes cluster.
    /// Note that this struct does not need to implement Serialize because the
    /// operator will only ever update the `status` subresource
    #[derive(Deserialize, Clone, Debug, PartialEq)]
    pub struct EchoServer {
        pub metadata: Metadata,
        pub spec: EchoServerSpec,
        pub status: Option<EchoServerStatus>,
    }

    /// defines only the fields we care about from the metadata
    #[derive(Deserialize, Clone, Debug, PartialEq)]
    pub struct Metadata {
        pub namespace: String,
        pub name: String,
    }

    /// The spec of our CRD
    #[derive(Deserialize, Clone, Debug, PartialEq)]
    pub struct EchoServerSpec {
        #[serde(rename = "serviceName")]
        pub service_name: String,
    }

    /// Represents the status of a CRD instance. Unlike the other structs we've defined here,
    /// this one _does_ need to implement `serde::Serialize` so that we can return it from
    /// our handler function.
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    pub struct EchoServerStatus {
        pub message: String,
    }
}

fn main() {
    env_logger::init();

    // first create an `OperatorConfig` struct that holds the types of the parent and child resources we'll be dealing with.
    // The `OperatorConfig` also has a few other fields that allow customizations of the behavior of the operator.
    // For each child type, we'll also specify the `UpdateStrategy`, which determines the behavior after a change is detected
    // between the desired and actual state of a resource of that type. For example, Pods cannot generally be updated in-place,
    // but must be deleted and re-created.
    let parent_type = echo_server::k8s_type();
    let operator_config = OperatorConfig::new(OPERATOR_NAME, parent_type)
            .with_child(K8sType::pod(), ChildConfig::recreate())
            .with_child(K8sType::service(), ChildConfig::replace());

    // Since you'll likely be running this example on a dev machine, we'll attempt to load the client configuration from your ~/.kube/config file.
    // You'll most likely want to use `from_service_account` function in production.
    let client_config = ClientConfig::from_kubeconfig(OPERATOR_NAME).expect("Failed to load client configuration from kubeconfig");

    let handler = HandlerImpl::default()
            .determine_children(|req: &SyncRequest| {
                // This function should return the desired children based on the given parent in the request
                get_desired_children(req)
            })
            .determine_status(|req: &SyncRequest| {
                // This function just returns a json object that will be set as the status of the parent resource
                Ok(json!({
                    "message": get_current_status_message(req),
                }))
            });

    // `run_operator` will never return under normal circumstances, so we only need to handle the sad path here
    let err = run_operator(operator_config, client_config, handler);
    log::error!("Error running operator: {:?}", err);
    std::process::exit(1);
}

/// Returns the json value that should be set on the parent EchoServer
fn get_current_status_message(request: &SyncRequest) -> String {
    let children = request.children();
    let pods = children.of_type_raw("v1", "Pod");
    let pod: Option<&K8sResource> = pods.first();
    pod.and_then(|p| p.pointer("/status/message").and_then(Value::as_str).map(String::from))
            .unwrap_or("Waiting for Pod to be initialized".to_owned())
}

fn get_desired_children(request: &SyncRequest) -> Result<Vec<Value>, Error> {
    log::info!("Got sync request: {:?}", request);
    let custom_resource: echo_server::EchoServer = request.deserialize_parent()?;
    let service_name = custom_resource.spec.service_name.as_str();

    let echo_server_name = custom_resource.metadata.name.as_str();
    let echo_server_namespace = custom_resource.metadata.namespace.as_str();

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
            "name": service_name,
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
