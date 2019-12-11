# Running the Operator

We're finally ready to put all the pieces together and get this operator running!

All the functions we need are defined in the [`roperator::runner` module](https://docs.rs/roperator/~0.1/roperator/runner/index.html).

The functions there all accept both an `OperatorConfig` and an `impl Handler`. The `run_operator` function accepts only those two arguments, and will block the current thread indefinitely. The body of a typical main function might look something like the following:

```rust,ignore
env_logger::init();

let config = create_operator_config();
let handler = MyHandler::new();

let error = roperator::runner::run_operator(config, handler);
log::error!("operator exited with error: {:?}", error);
std::process::exit(1);
```

The `run_operator` and `run_operator_with_client_config` functions are both meant to run the operator indefinitely, as you would in a production container. They do not ever return under normal circumstances, and thus they do not return a `Result`, since it would never return the `Ok` variant.

The `run_operator_with_client_config` function allows you to pass a custom `ClientConfig`. See the [advanced client configuration](../reference/advanced-client-configuration.md) section if you need to use that. The defaults used by `run_operator` should be fine for most use-cases, though.

## Deployment

Roperator is just a library, so you're responsible for building an image and deploying a container on your own. We do have a bit of useful advice, though.

- Use a `StatefulSet` with only a single replica. While `Deployment`s are great for a lot of things, they're not ideal for operators due to their behavior when you make changes to the deployment spec. Deployments will typically start the new version before they shutdown the old one, which can result in multiple instance of your operator running at the same time and trying to update the same resources. It's safest to use a `StatefulSet` instead, which ensures that at most a single instance is running as long as you set the `replicas` to 1.
- If `run_operator` returns an error, just exit with a non-zero status. Critical errors in your operator shoud be extremely rare, since most error conditions are handled internally. If `run_operator` returns an error, then it's probably something pretty serious and not recoverable. Let the container die and get re-created by the controller.
- Your container will need to have openssl installed. Most of the images out there will already have openssl, or allow it to be installed really easily. If your operator process fails to start, then a good first step would be to ensure that the openssl library is installed.
- You may want to add an option for your operator to be confined to a specific namespace. When you create your `OperatorConfig`, you can check your program arguments or an env variable and set a namespace on it if desired. This allows you the flexibility to deploy either cluster-wide or for only a specific namespace.

## RBAC for your Operator

Roperator of course needs authorization to update any of the resources it manages. In most production clusters, this means using [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/). For operators that will run within a single namespace, a regular `Role` and `RoleBinding` can be used. For cluster-scoped operators, you'll need a `ClusterRole` and `ClusterRoleBinding`.

For your parent resource (usually a CRD you've defined), you'll need to ensure that your `rules` include access to update the `status` subresource. Such a rule might look like the following:

```yaml
rules:
  - apiGroups: ["mygroup"]
    resources: ["mycsutomresources", "mycsutomresources/status"]
    verbs: ["get", "list", "watch", "update", "patch"]
```

For each type of child resource that's included in your `OperatorConfig`, you'll also need to allow all of the verbs: `["get", "list", "watch", "create", "update", "patch", "delete"]`

