# Operator Configuration

Now that you have a parent CRD and a type definition for it, you're ready to create an `OperatorConfig`. An `OperatorConfig` struct holds type information on your parent type, as well as the types of child resources that your operator might create. For each _type_ of child resource that your operator may create, you need to add a `ChildConfig`.

For example, say our operator will use `Foo` as its parent, and for each `Foo` it may create Services, Pods, and PodSecurityPolicies.

```rust
// in main.rs
use roperator::prelude::{K8sType, OperatorConfig, ChildConfig};

use roperator::k8s_types::{
    // These are all pre-defined `&'static K8sType`s for all the builtin resources that we'll be using
    core::v1::{
        Pod,
        Service
    },
    policy::v1beta1::PodSecurityPolicy,
};

// A static reference to the type of our parent resource
pub PARENT_TYPE: &K8sType = &K8sType {
    api_version: "example.com/v1",
    kind: "Foo",
    plural_kind: "foos",
};

fn main() {
    env_logger::init();

    // name is used as a label on all the child resources,
    //and also as the user-agent when communicating with the api server
    let operator_name = "foo-operator";

    // Create our configuration
    let operator_config = OperatorConfig::new(operator_name, PARENT_TYPE)
        .with_child(Service, ChildConfig::replace())
        .with_child(Pod, ChildConfig::recreate())
        .with_child(PodSecurityPolicy, ChildConfig::on_delete());

    // ...
}
```

Let's unpack some of that, starting with `OperatorConfig::new`. This function creates a new [`OperatorConfig`](https://docs.rs/roperator/~0.1/roperator/config/struct.OperatorConfig.html) for the given parent type. The next three calls to `.with_child` configure the three types of child resources that our operator will create. Each of these lines also passes a [`ChildConfig`](https://docs.rs/roperator/~0.1/roperator/config/struct.ChildConfig.html) that applies to all resources of that type. The `ChildConfig` contains an [`UpdateStrategy`](https://docs.rs/roperator/~0.1/roperator/config/enum.UpdateStrategy.html), which is what determines the behavior when roperator detects a difference between the desired and actual state of a resource of that type.

The possible values for `UpdateStrategy` are:
`UpdateStrategy::Replace`: When there's a difference between the actual and desired state of a resource, the existing resource will be updated in place using a PUT request. This strategy cannot be used for some resources (e.g. Pods), becuase their spec is immutable.
`UpdateStrategy::Recreate`: When there's a difference between the actual and desired state of a resource, roperator will first delete the existing resource and then recreate it with the new state.
`UpdateStratefy::OnDelete`: When there's a difference between the actual and desired state, roperator will never modify the existing resource. It will wait for the existing resource to be deleted by some other means, and only then will it re-create the new one with the new desired state.

For our example, we chose to `Replace` Services, to `Recreate` Pods, and to never modify PodSecurityPolicies. For your operator, you can choose whichever strategies make sense for your application and resource types.

## Optional Operator Configuration

The defaults provided by `OperatorConfig::new` are pretty reasonable for most use cases, but there are some other options that you may configure.

#### Namespaced Operator

The default behavior is for roperator to watch and act on resources in _all_ namespaces. If this is not what you want, then you can call `operator_config.within_namespace("my-namespace")` to isolate the operator to only that namespace. This is especially useful in testing, since it allows you to test multiple versions of your operator simultaneously in the same cluster.

#### Metrics

By default, roperator will gather and serve Prometheus metrics over HTTP at the `/metrics` endpoint. This is important because it makes it easy to monitor the operator, which may provide early warning signs for the applications that it manages. If you don't want metrics exposed, then you can call `operator_config.expose_metrics(false)` to disable this.

#### Health

Roperator will also expose a health check endpoint over HTTP at `/health`. This is enabled by default, but can be disabled by call

#### Server Port

If either metrics or health are enabled, then roperator will start an HTTP server that listens on port `8080` by default. You can set the server port using `operator_config.server_port(1234)`. If both metrics and health are disabled, then no HTTP server will be started.

## Authentication

Calling `OperatorConfig::new` will automatically use a Kubernetes [Service Account](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/) if there is a service account token mounted. If no service account is found, then a kubeconfig file will be used. The Kubeconfig file will be loaded from `~/.kube/config`, or from the path specified by the `KUBECONFIG` environment variable, if it is set. Using Service Accounts is the recommended approach in production scenarios, but kubeconfig files are much more convenient for local development and testing.

Kubeconfig files can include many different mechanisms for getting authentication credentials. Roperator tries to support the most comon and useful ones, but there is at least one notable exception: gcp-authentication and oauth not supported (though the plan is to add support in a future release). The list of _supported_ authentication methods is below.

If your authentication method isn't supported, then please see the [Advanced Client Configuration reference](../reference/advanced-client-configuration.md) for how to programatically configure the client.

### Supported Automatic Kubeconfig Authentication Methods

| Method             | Kubeconfig user fields                       | Notes                                                                                                                                                           |
|--------------------|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Basic              | `username`, `password`                       |                                                                                                                                                                 |
| Token              | `token`                                      |                                                                                                                                                                 |
| Inline Certificate | `client-certificate-data`, `client-key-data` | Certificate and key are expected to be base64 encoded                                                                                                           |
| Certificate        | `client-certificate`, `client-key`           |  Fields are expected to hold absolute paths to pem files                                                                                                        |
| Exec               | `exec.command`, `exec.args`, `exec.env`      | Executes a command that is expected to write a json response to stdout with the credentials. This method is used by the aws-iam-authenticator for EKS clusters. |

# Next

[Implementing your Handler](handler-sync.md)
