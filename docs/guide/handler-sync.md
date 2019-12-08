# Handler (Sync)

The [`Handler` trait](https://docs.rs/roperator/~0.1/roperator/handler/trait.Handler.html) encapsulates the core logic of your operator. This trait defines two functions, `sync` and `finalize`. Every operator needs to implement a `sync` function, so that's what we'll focus on first. You only need to implement `finalize` if there is some special cleanup action that needs to be taken when a parent resource is deleted.

The `sync` function is defined as `fn sync(request: &SyncRequest) -> Result<SyncResponse, Error>`.

The `SyncRequest` represents a snapshot of the state of resources for a single parent in the cluster. It contains the raw JSON of the parent, as well as the raw JSON for all of the child resources related to that parent.

The `sync` function needs to determine two main things and return them as a `SyncResponse`:

## Parent Status

The `SyncResponse` struct has a `status` field, and this represents the _current_ status of the parent. Typically, this status should be determined by inspecting the state of all of the `children` from the `SyncRequest`. For example, if your operator creates `Pod` and `Service` resources, you could look at the status of the Pods and the endpoints of the Services in order to summarize the application status. You may also wish to perform some additional validation on the parent and set an error field in the status if it's invalid.

**observedGeneration**
Roperator will automatically add the `observedGeneration` field to your status, and set its value to the current `metadata.generation` of the parent. This makes it easy to tell whether changes to the parent `spec` have been observed yet.

**Null status**
`Value::Null` is a perfectly valid status for a parent. Returning null instructs Roperator not to set any status at all. If your parent resource does not have the status subresource enabled (as described [here](parent.md#Enable-the-status-subresource)), then you _must_ only return `Value::Null` as the `status`.

## Children

The `children` field represents the _desired_ state of all the child resources that corespond to the parent. Roperator will determine if there's any differences between the actual and desired states of each child resource, and it will update them based on the `ChildConfig` for each type. Any existing child resources that are _not_ included in the `SyncResonse` _will be deleted_.

Operators should only specify the fields that they care about in child resources, since these resources may have other controllers that set additional fields. Specifically, _don't_ just return the same JSON that came in the request, since that json will include all sorts of things that either cannot or should not be updated by your operator. It's also worth mentioning that child resources returned in the `SyncResponse` must never specify a `status` since that should only ever be determined by the controller of the resource.

## Returning Errors

When a `Handler` returns an `Err` result, roperator will not modify either the parent or any child resources. It will track the error counts on a per-parent basis, though, and expose them in the metrics if that feature is enabled. It will then re-try your sync function after a delay.

It is recommended that your handler should handle most errors itself by returning a `status` that includes information about the error. Whoever created the parent will then be able to check the status to see the error message.

## Sync Function Best Practices and Details

**Desired State:**
Sync functions always return a _desired_ state. They intentionally are not modeled in terms of _operations_ like create or update. So once your handler returns a particular child resource, you should continue to return the same resource for as long as you want that resource to exist. Most handlers should be implemented such that they simply always return the same _desired_ children for a given parent resource state. That is, they should be more or less pure functions that map a parent state to a desired set of children. For some scenarios, you may want to just keep a child resource in whatever state was given in the sync request. In that case, your handler could only return a minimal set of metadata for the child resource in the `SyncResponse` (`apiVersion`, `kind`, `metadata.namespace`, `metadata.name`, without any other fields). Doing so will ensure that roperator will not detect any differences between the desired and actual state of the child.

**Same Namespace**
For namespaced parents, all child resources _must_ be in the same namespace as the parent. It's possible that roperator may lift this requirement in the future if there are use cases that require it, but for now the best advice is to keep it simple anyway. If you need resources that span multiple namespaces, then you should use a non-namespaced (a.k.a. Cluster Scoped) parent CRD.

**Stable values:**
Although handlers are allowed to have side effects, it's strongly encouraged that your `SyncResponse` is the same across repeated function invocations. Be extra careful with values that are not stable. For an exaple, let's say that you set a field on some child resource to a current timestamp. Whenever your `sync` function is invoked, it would return a _different_ timestamp, and thus cause Roperator to update the resource again, which could potentially trigger yet another `sync` call, ans so on. If you do need to use a timestamp or any other random or non-stable value, then it's recommended that your sync function should read the existing value from the sync request, and only generate a new value if the resource or field is missing.

**Avoiding Name Conflicts**
It's best to ensure that your operator cannot generate multiple resources with the same name. For example, if your `sync` function always returns a child Pod with the name `"foo"`, then it will cause an error when someone creates two instance of the parent resource in the same namespace, because you can't have two resources with the same namespace and name. For namespaced parents, it's a good idea to include the name of the parent as a prefix or suffix on the child names.


### Child Resources

Roperator always represents Kubernetes resources as plain JSON objects, but that doen't mean your operator has to. `SyncRequest` has functions to simplify deserialization into typed structs, and `SyncResponse` has functions for adding types that implement `serde::Serialize`. This means that you can use the definitions in the [`k8s_openapi`](https://crates.io/crates/k8s-openapi) crate, or define your own structs.

See the [echo-server example](../../examples/echo-server) for how to use json directly using the `serde_json::json!` macro. Other examples will be added soon to show how to use types that implement `Serialize`.

### Comparison of desired and actual states

When comparing the actual and desired states of a child resource, roperator only considers the fields that were specified in the _desired_ state (from your `SyncResponse`). For example, if the request has a pod with 3 containers, but your response only includes a single container, then only differences in the single container in your response will be considered when determining whether to update hte child. This is important because there may be other processes or controllers that modify your child resources. For example, you may have an admission webhook that adds an initContainer to each Pod for injection of configuration.

## Handler impl

For simple handlers, there's a blanket impl for all `Fn(&SyncRequest) -> Result<SyncResponse, Error> + 'static`. This allows you to write a handler just as a normal function.

```rust
fn my_handler(req: &SyncRequest) -> Result<SyncResponse, Error> {
    let mut response = SyncResponse::from_status(determine_status(req))?;
    response.add_child(get_child_pod(req))?;
    response.add_child(get_child_service(req))?;
    // ... and so on
    Ok(response)
}
```

You may also declare any struct and just `impl Handler for` your struct. This allows you to access things like Http clients, database connection pools, and other state in your handler. Note that the `sync` function takes a `&self`, so you need to internally synchronize access to any mutable state.

```
use roperator::prelude::{Handler, SyncRequest, SyncResponse, Error};

struct MyHandler {
    // ...
}

impl Handler for MyHandler {
    fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
        let mut response = SyncResponse::from_status(determine_status(req))?;
        response.add_child(get_child_pod(req))?;
        response.add_child(get_child_service(req))?;
        // ... and so on
        Ok(response)
    }
}
```

# Next

Put it all together and [run your operator](running.md)!
