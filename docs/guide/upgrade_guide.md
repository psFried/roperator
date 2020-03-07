# Upgrade Guide

Once Roperator version 1.0 is released, it will strictly adhere to semantic versioning. For pre-1.0 releases (e.g. 0.1, 0.2) there will be breaking changes with each minor version change (e.g. from 0.1.x to 0.2.x). This guide will attempt to enumerate all of these breaking changes.

## 0.1.x to 0.2.x

There were a number of breaking changes in the 0.2.0 release. Most of them were in the `roperator::request` module, and were made in order to provide a nicer API for retrieving child resources from the `SyncRequest`.

`SyncRequest`:

- Removed `iter_children_with_type` function. Instead you can use `request.children().with_type(api_version, kind;
- Removed `raw_child` function. Instead, use `request.children().of_type(k8s_type).get(namespace, name)`
- Removed `has_child` function. Instead use `request.children().of_type(k8s_type).exists(namespace, name)`
- Removed `deserialize_child` function. Instead use `request.children().with_type::<StructType>(k8s_type).get(namespace, name)`, where `StructType` is the type that you want to deserialize to

`RequestChildren`:

- Refactored the functions that return typed views
    - `of_type` returns a `RawView` and `with_type` returns a `TypedView`
    - The old `of_type` function has been renamed to `with_type`, to make it more clear that it adds type information
    - Renamed function `of_type_raw` to just `of_type`, which returns a `RawView`
    - both functions accept any value that implements `Into<K8sTypeRef>`, which includes `&K8sType` and `(&str, &str)`
- Removed the `exists` function. Use `of_type(k8s_type).exists(namespace, name)` instead

`TypedView`:

- Removed the `iter_raw` function. Use `as_raw().iter()` instead
- Changed the struct declaration to specify separate lifetimes for the inner `SyncRequest` and the `K8sTypeRef`. This should not impact most usages, but may if you're written out the full type on a variable

`RawView`:

- Changed the struct declaration to specify separate lifetimes for the inner `SyncRequest` and the `K8sTypeRef`. This should not impact most usages, but may if you're written out the full type on a variable

