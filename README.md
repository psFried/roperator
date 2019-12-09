# Roperator

[![roperator](https://docs.rs/roperator/badge.svg)](https://docs.rs/roperator)

Roperator lets you easily write [Kubernetes Operators](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) in Rust. Roperator handles all the mechanics and plumbing of watching and updating your resources. All you have to do is to write a function that returns the desired state of your resources. Roperator was heavily inspired by the excellent [Metacontroller](https://github.com/GoogleCloudPlatform/metacontroller) project, which it seems is no longer maintained.

## Declarative Operator

The goal of this project is to make it really easy to write operators for the vast majority of common cases. Roperator focuses on the case where you have a _parent_ Custom Resource Definition, and for each parent you'll want a set of corresponding _child_ resources. For example, you might have a CRD that describes your application, and for each instance of that Custom Resource, you'll want to create a potentially complex set of child resources.

The core of your operator is a _sync_ function that gets passed a snapshot view of the _parent_ custom resource and any existing _children_ in a `SyncRequest` struct. This function will simply return the _desired_ set of children for the given parent, as well as a JSON object that should be set as the `status` of the parent. Your sync function does not need to directly communicate with the Kubernetes API server in any way. Roperator will handle watching all of the resources, invoking your sync function whenever it's needed, and updating the child resources and the parent status.

The [EchoServer example](examples/echo-server/README.md) is a good first example to look at to see how everythign fits together end to end.

### Dependencies

- Rust 1.39 or later
- Openssl libraries and headers. Roperator uses the rust `openssl` crate. You can check out their docs on building and linking to openssl [here](https://docs.rs/openssl/0.10.26/openssl/index.html#building)

## Getting Started

First add the dependency:

```toml
# Cargo.toml
[dependencies]
roperator = "*"

[dev-dependencies]
roperator = { version = "*", features = ["testkit"] }
```

### Operator Configuration

The first thing you'll need is a configuration object that specifies the relationships between the CRD that will act as the _parent_ and the types of resources that may be created as _children_.

```rust
use roperator::prelude::{k8s_types, K8sType, OperatorConfiguration, ChildConfig, run_operator};
/// The type of our parent Custom Resource. This must match the fields provided in the CRD
pub static PARENT_TYPE: &K8sType = &K8sType {
    api_version: "example.com/v1alpha1",
    kind: "MyResource",
    plural_kind: "myresources",
};

fn main() {
    let operator_config = OperatorConfig::new("my-operator-name", PARENT_TYPE)
            .with_child(k8s_types::core::v1::Pod, ChildConfig::recreate())
            .with_child(k8s_types::core::v1::Service, ChildConfig::replace());
    //.... you can have as many child types as you want, but you need to specify all of the types that you may create ahead of time

    let my_handler = MyHandler; // we'll look at Handlers further down

    run_operator(operator_config, my_handler).expect("failed to run operator");
}
```

### Handler

The main logic of your operator is inside your `Handler` implementation. The main function you need to implement is `Handler::sync`, which accepts a `SyncRequest` and returns a `SyncResponse`. The `Hander` trait is implemented automatically for all `Fn(&SyncRequest) -> Result<SyncResponse, Error>`, but you can also implement it for your own type.

```rust
use roperator::prelude::{Handler, SyncRequest, SyncResponse, FinalizeResponse, Error};

struct MyHandler;
impl Handler for MyHandler {
    fn sync(&self, request: &SyncRequest) -> Result<SyncResponse, Error> {
        // some json object that describes the status of the parent. Typically, this will be determined just based on the
        // statuses of the child resources
        let parent_status = determine_parent_status(request);

        // returns all of the desired kubernetes resources that should correspond to this parent
        let children = determine_desired_child_resources(request);
        Ok(SyncResponse {
            status: parent_status,
            children
        })
    }
}
```

Handlers may optionally implement the `finalize` function. This function gets called when then parent resource is being deleted, and allows you to clean up any external resources that may have been created. Roperator does its best to ensure that `finalize` will get invoked _at least_ once per parent that is being deleted. It is not possible to guarantee that this will happen, though, since Kuberntes allows clients to force the deletion of resources without waiting for finalizers.

```rust

fn finalize(&self, request: &SyncRequest) -> Result<FinalizeResponse, Error> {
    // We return a boolean that says whether the finalization was successful or not. If we return false,
    // then roperator will continue to regularly re-try your finalize function until it succeeds.
    // As soon as we return `true`, roperator will remove itself from the finalizers list of your parent
    // resource and allow the deletion to proceed.
    let cleanup_succeeded: bool = do_cleanup(request);
    let parent_status = determine_parent_status(request);

    Ok(FinalizeResponse {
        status: parent_status,
        finalized: cleanup_succeeded,
    })
}
```

## The Path to 1.0

Roperator is still young, and not yet "production grade". The APIs will also be unstable until we reach a 1.0 milestone. It would really benefit from feedback from early adopters, so please file an issue or submit a Pull Request. The goal is to quickly identify the things that need to be addressed before the 1.0 release, and focus on producing a production grade 1.0 release as soon as is reasonable.

Some of the things that will definitely need addressed prior to the 1.0 release are:

- Build out a much more complete test suite
- Polish up the API for accessing child resources from a `SyncRequest`
- Settle on a more consistent and simple API for the `TestKit`
- Settle on a simpler API for running multiple operators in the same process

### License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Roperator by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
