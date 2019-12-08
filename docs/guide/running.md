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

The `run_operator_with_client_config` function allows you to pass a custom `ClientConfig`. See the [advanced client configuration](advanced-client-configuration.md) section if you need to use that. The defaults used by `run_operator` should be fine for most use-cases, though.
