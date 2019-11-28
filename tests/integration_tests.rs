use roperator::prelude::*;
use roperator::runner::testkit::{TestKit};
use roperator::resource::ObjectIdRef;

use roperator::serde_json::{json, Value};

use std::time::Duration;


fn make_client_config(operator_name: &str) -> ClientConfig {
    if let Some(conf) = ClientConfig::from_service_account(operator_name).ok() {
        conf
    } else {
        ClientConfig::from_kubeconfig(operator_name).expect("Failed to create client configuration")
    }
}

fn parent_type() -> K8sType {
    K8sType::new("roperator.com", "v1alpha1", "TestParent", "testparents")
}

fn setup(name: &str, handler: impl Handler) -> TestKit {
    std::env::set_var("RUST_LOG", "roperator=trace");
    let _ = env_logger::try_init();
    let operator_config = OperatorConfig::new(name, parent_type())
            .within_namespace(name)
            .with_child(K8sType::pod(), ChildConfig::recreate())
            .expose_health(false)
            .expose_metrics(false);
    let client_config = make_client_config(name);

    TestKit::with_test_namespace(name, operator_config, client_config, handler)
            .expect("Failed to create test kit")
}

fn parent(namespace: &str, name: &str) -> Value {
    let k8s_type = parent_type();
    json!({
        "apiVersion": k8s_type.format_api_version(),
        "kind": k8s_type.kind.as_str(),
        "metadata": {
            "namespace": namespace,
            "name": name,
        },
        "spec": {}
    })
}

#[test]
fn operator_settles_on_a_stable_state() {
    let namespace = "stable-state";
    let parent_type = parent_type();
    let mut testkit = setup(namespace, success_handler);

    let parent = parent(namespace, "parent-one");
    testkit.create_resource(&parent_type, &parent).expect("failed to create parent resource");

    testkit.reconcile_and_assert_success(Duration::from_secs(20));

    let expected_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": namespace,
            "name": "parent-one",
            "labels": {
                "app.kubernetes.io/name": "parent-one"
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "example",
                    "image": "busybox:latest",
                    "command": ["tail", "-f", "/dev/null"],
                    "resources": {
                        "requests": {
                            "cpu": "50m",
                            "memory": "20Mi"
                        }
                    }
                }
            ]
        }
    });

    testkit.assert_child_resource_eq(&K8sType::pod(), &ObjectIdRef::new(namespace, "parent-one"), expected_pod);
}


fn success_handler(req: &SyncRequest) -> Result<SyncResponse, Error> {
    let namespace = req.parent.namespace();
    let name = req.parent.name();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "labels": {
                "app.kubernetes.io/name": name,
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "example",
                    "image": "busybox:latest",
                    "command": ["tail", "-f", "/dev/null"],
                    "resources": {
                        "requests": {
                            "cpu": "50m",
                            "memory": "20Mi"
                        }
                    }
                }
            ]
        }
    });

    let children = req.children();
    let pods = children.of_type_raw("v1", "Pod");
    let child = pods.first();
    let message = child.and_then(|p| {
        p.pointer("/status/message").and_then(Value::as_str)
    }).unwrap_or("Waiting on pod to start");

    Ok(SyncResponse {
        status: json!({
            "message": message,
        }),
        children: vec![pod]
    })
}
