# Advanced Client Configuration

The `ClientConfig` struct contains configuration that's used to connect to the Kubernetes API server. This includes the url, authentication configuration, TLS certificates, etc.

### User Agent

`ClientConfig` requires that you specify a user agent string. This doesn't necessarily affect the functionality, but it does help when looking though the logs of the api server, and is considered a good practice to set this to something descriptive.

## Authentication

Calling `roperator::prelude::run_operator` will automatically use a Kubernetes [Service Account](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/) if there is a service account token mounted. If no service account is found, then a kubeconfig file will be used. The Kubeconfig file will be loaded from `~/.kube/config`, or from the path specified by the `KUBECONFIG` environment variable, if it is set. Using Service Accounts is the recommended approach in production scenarios, but kubeconfig files are much more convenient for local development and testing.

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

For most scenarios, you'll be able to just use the builtin `ClientConfig::from_kubeconfig` or `ClientConfig::from_service_account` functions. The `roperator::runner::run_operator` function will first try `from_service_account` and then fall back to `from_kubeconfig`, which allows it to "just work" in most scenarios. But there's always scenarios that require special handling, and that's why we allow you to supply your own `ClientConfig` struct.

The fields of `ClientConfig` are all public and are documented [here](https://docs.rs/roperator/~0.1/roperator/config/struct.ClientConfig.html).

The most common reason to create a custom client configuration is if roperator is not able to determine the proper credentials from your kubeconfig file or service account. If this is the case, then you'll need to determine the proper credentials on your own. The `roperator::config::Credentials` enum has two variants, one for certificate-based authentication, and the other for header-based authentication. Any value specified in the `Header` variant will simply be added to every request as the value of the `Authorization` header. This should include any formatting or encoding required for basic or bearer authentication.

Roperator also requires a user-agent string for the client configuration. When roperator creates the `ClientConfig` for you, it uses the value of `operator_name` from your `OperatorConfig` as the user agent. This makes it easier to identify calls made by the operator in the api server logs. It's recommended that you do the same thing when using a custom `ClientConfig`.
