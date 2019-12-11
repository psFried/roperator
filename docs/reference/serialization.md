# (De)Serialization

Both the `SyncRequest` and `SyncResponse` types store resources as `serde_json::Value`s, but you'll probably want to deal with more typesafe structs in your operator. Roperator provides typed accessor functions on `SyncRequest` that allows you to deserialize any resource (or group of resources) as a typed struct. You can read objects from the sync request as any type that implements `serde::DeserializeOwned`. Take a look at the [SyncRequest docs](https://docs.rs/roperator/~0.1/roperator/handler/struct.SyncRequest.html) for details on the typed accessor functions.

Similarly, `SyncResponse` has functions that allow you to set the status or add child resources from any type that immplements `serde::Serialize`. The [SyncResponse docs](https://docs.rs/roperator/~0.1/roperator/handler/struct.SyncResponse.html) have the details on these functions.

The [k8s-openapi](https://crates.io/crates/k8s-openapi) crate provides type definitions for all of the builtin Kubernetes resource types, which all implement `Serialize` and `DeserializeOwned`. You're also free to provide your own struct definitions, and even mix the two approaches.
