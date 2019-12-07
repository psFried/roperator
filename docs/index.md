# Roperator

Roperator lets you easily write [Kubernetes Operators](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) in Rust. Roperator handles all the mechanics and plumbing of watching and updating your resources, and provides a simple yet powerful api for implementing your operator.

## [Introduction](intro.md)


These docs are in the early stages, so please [file an issue](https://github.com/psFried/roperator/issues/new) if you find something unclear or missing, or if you have questions that aren't answered here.


[API Documentation](https://docs.rs/roperator)

### Dependencies

- Rust 1.39 or later is required, since Roperator is built using async/await syntax.
- Roperator uses the `openssl` crate, which requires the openssl libraries and header files. You can check out their docs on building and linking to openssl [here](https://docs.rs/openssl/0.10.26/openssl/index.html#building)
