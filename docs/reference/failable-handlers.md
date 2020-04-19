# Failable Handlers

For many `Handler` impls, the only error conditions you need to handle are from
(de)serialization. For those, it's fine to just return the errors from your `sync` function. But
some handlers may perform custom validation of parent resources, or need to connect to external
systems like a database or web service. For these, more sophisticated error handling is
desirable, so that the operator can populate the status of the parent with details about the
error.

The optional `failable` feature is designed to help with implementing handlers that want to
recover from errors by adding error description to the status of the parent. This can be enabled
by declaring the roperator dependency like so in your `Cargo.toml`:

```toml
[dependencies]
roperator = { version = "^0.2", features = ["failable"] }
```

This enables this `roperator::handler::failable` module, which contains helpers for implementing
`Handler`s that also do some error handling. The main interface for your operator is and always
will be the `Handler` trait. The types exposed in the `failable` module just make it easier to
implement `Handler` in a way that allows most errors to be dealt with by describing the error in
the `status` of the parent resource.

## Implement the `FailableHandler` trait

The [`FailableHandler` trait](https://docs.rs/roperator/latest/roperator/handler/failable/trait.FailableHandler.html)
breaks down your sync and finalize functions into multiple steps, instead of building the whole
`SyncResponse` all at once. The `FailableHandler` trait defines three separate functions that go
into handling a sync request, `validate`, `sync_children`, and `determine_status`. These are each
called in sequence in order to build the `SyncResponse`. If either `validate` or `sync_children`
returns an error, then the error will be passed to the `determine_status` function, so that you
can desciribe the error in the status.

### Example:

```rust
use roperator::handler::failable::{FailableHandler, HandlerResult, DefaultFailableHandler};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

/// Struct that corresponds to your CRD type. The parent is deserailized as this type in the `validate` function
#[derive(Deserialize, Debug)]
struct MyCrdType {
    metadata: ObjectMeta,
    spec: MyCrdSpec,
    status: MyCrdStatus,
}

#[derive(Deserialize, Debug)]
struct MyCrdSpec {
    foo: String,
    some_number: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct MyCrdStatus {
    error: Option<String>,
    // plus whatever else you want to include in the status
}


struct MyHandler;
impl FailableHandler for MyHandler {

    type Validated = MyCrdType;

    // we'll just pretend that `MyError` exists and implements `From<serde_json::Error>` so
    // that we can use the `?` operator
    type Error = MyError;

    type Status = MyStatus;

    fn validate(&self, request: &SyncRequest) -> Result<Self::Validated, Self::Error> {
        // we could also do some additional custom validation if we want
        request.deserialize_parent().map_err(|e| MyError::SerdeError(e))
    }

    // This is only called if validation is successful, and will be passed a mutable reference to the validated type
    fn sync_children(&self, validated: &mut MyCrdType, request: &SyncRequest) -> Result<Vec<Value>, Self::Error> {
        let child1 = try_create_child1(validated)?;
        let child2 = try_create_child2(validated)?;
        Ok(vec![child1, child2])
    }

    fn determine_status(&self, request: &SyncRequest, result: HandlerResult<MyCrdType, MyError>) -> Self::Status {
        // if the result is an error variant, then we'll convert it to a string to add to the status.
        // Note that `HandlerResult` is not a std::result::Result, and has more variants
        let error_message = result.into_error().map(std::fmt::Display::to_string);

        // normally, we might also include some information derived from the status of the child resources, too
        MyStatus {
            error: error_message,
        }
    }
}
```

If `validate` returns an error, then `sync_children` will be skipped. This means that no new child resources will be created,
and any existing child resources will be deleted.


## Wrap `MyHandler` with `DefaultFailableHandler`

The `FailableHandler` trait does not do anything by itself. It must be adapted to the `Handler` trait by wrapping it using
`DefaultFailableHandler::wrap`. The `DefaultFailableHandler` drives the `FailableHandler` and also provides some optional
additional functionality.

### Example

```rust
use roperator::handler::failable::DefaultFailableHandler;

fn main() {
    // ... the usual setup to create an OperatorConfig
    // let config = ...

    // wrap the MyHandler from the previous example
    let handler = DefaultFailableHandler::wrap(MyHandler);
    roperator::runner::run_operator(config, handler);
}
```

### Error Backoffs

When either `validate` or `sync_children` return an error, `DefaultFailableHandler` will use it's `BackoffConfig` to determine when to re-try. This will use a backoff that grows by a multiplier with each subsequent error for the same parent. This backoff can be customized or disabled by supplying your own `BackoffConfig` to `DefaultFailableHandler::with_backoff`.

### Regular Re-Syncs

In addition to the normal behavior, `DefaultFailableHandler` can also re-sync at regular time intervals, even when nothing has changed. This is useful for operators that manage resources that are external to the k8s cluster. You can enable this by calling `with_regular_resync` and passing in a `std::time::Duration`.
