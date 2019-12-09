# Advanced Client Configuration

For most scenarios, you'll be able to just use the builtin `ClientConfig::from_kubeconfig` or `ClientConfig::from_service_account` functions. The `roperator::runner::run_operator` function will first try `from_service_account` and then fall back to `from_kubeconfig`, which allows it to "just work" in most scenarios. But there's always scenarios that require special handling, and that's why we allow you to supply your own `ClientConfig` struct.

The fields of `ClientConfig` are all public and are documented [here](https://docs.rs/roperator/~0.1/roperator/config/struct.ClientConfig.html).

The most common reason to create a custom client configuration is if roperator is not able to determine the proper credentials from your kubeconfig file or service account. If this is the case, then you'll need to determine the proper credentials on your own. The `roperator::config::Credentials` enum has two variants, one for certificate-based authentication, and the other for header-based authentication. Any value specified in the `Header` variant will simply be added to every request as the value of the `Authorization` header. This should include any formatting or encoding required for basic or bearer authentication.

Roperator also requires a user-agent string for the client configuration. When roperator creates the `ClientConfig` for you, it uses the value of `operator_name` from your `OperatorConfig` as the user agent. This makes it easier to identify calls made by the operator in the api server logs. It's recommended that you do the same thing when using a custom `ClientConfig`.
