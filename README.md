# Roperator

Roperator lets you easily write [Kubernetes Operators](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) in Rust. Roperator handles all the mechanics and plumbing of watching and updating your resources. All you have to do is to write a function that returns the desired state of your resources. Roperator was heavily inspired by the excellent [Metacontroller](https://github.com/GoogleCloudPlatform/metacontroller) project, which it seems is no longer maintained.

## Declarative Operator

The goal of this project is to make it really easy to write operators for the vast majority of common cases. Roperator focuses on the case where you have a _parent_ Custom Resource Definition, and for each parent you'll want a set of corresponding _child_ resources. For example, you might have a CRD that describes your application, and for each instance of that Custom Resource, you'll want to create a potentially complex set of child resources.

The core of your operator is a _sync_ function that gets passed a snapshot view of the _parent_ custom resource and any existing _children_ in a `SyncRequest` struct. This function will simply return the _desired_ set of children for the given parent, as well as a JSON object that should be set as the `status` of the parent. Your sync function does not need to directly communicate with the Kubernetes API server in any way. Roperator will handle watching all of the resources, invoking your sync function whenever it's needed, and updating the child resources and the parent status.

The [EchoServer example](examples/echo-server/README.md) is a good first example to look at to see how everythign fits together end to end.

## Getting Started

First add the dependency:

```toml
# Cargo.toml
[dependencies]
roperator = "*"
```

Then, in your `fn main()`:

```rust
use roperator::prelude::*;
use roperator::serde_json::json;


fn main() {
    // We'll create a configuration object that tells roperator about the type of your parent CRD, and about the types
    // of all the child resources we might create. For each child type, we also specify the behavior for when there's a
    // difference between the desired and actual state for a child of that type.
    let parent_type = K8sType::new("example.roperator.com", "v1alpha1", "MyCrdKind", "mycrdkinds");
    let operator_config = OperatorConfig::new("my-operator", parent_type)
            .with_child(K8sType::pod(), ChildConfig::recreate()) // recreate means to first delete the resource, then create a new one
            .with_child(K8sType::service(), ChildConfig::replace()); // replace means to use a PUT request to update the resource in place

    // next we create a Handler that holds a function for determining the current status for the parent resource, as well as
    // a function for returning the desired child resources
    let handler = HandlerImpl::default()
            .determine_children(|req: &SyncRequest| {
                // This function should return the desired children based on the given parent in the request.
                // We could also return any types from the `k8s_openapi` crate (e.g. Pod or Service structs), but writing json directly is fine, too
                Ok(json!([
                    {
                        "apiVersion": "v1",
                        "kind": "Service",
                        "metadata": {
                            "name": req.parent.name(),
                            "namespace": req.parent.namespace(),
                        },
                        "spec": {
                            "type": "ExternalName",
                            "externalName": "some.other.host.test",
                        }
                    }
                ]))
            })
            .determine_status(|req: &SyncRequest| {
                // This function just returns a json object that will be set as the status of the parent resource.
                // Normally, you'd want to determine this status by looking at the current state of your child resources.
                let message = if req.has_child("v1", "Service", req.parent.namespace(), req.parent.name()) {
                    "Everything looks just great"
                } else {
                    "Creating a Service, hold your horses"
                };
                Ok(json!({
                    "message": message
                }))
            });

    // finally, we run the operator, passing it the configuration and the handler. This function
    // will block the current thread indefinitely, unless there's a fatal error.
    let err = run_operator(operator_config, handler);
    eprintln!("Err running operator: {}", err);
}
```

