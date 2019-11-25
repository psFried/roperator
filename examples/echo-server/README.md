# EchoServer example Operator

For this example, we'll use the an `EchoServer` as our CRD. Each `EchoServer` instance (a resource with `kind: EchoServer`), the operator will create a Pod that runs an http echo server, as well as a Service. This serves as a good introduction because the application is so simple that it lets us focus more on how to get the operator running.

To run this example:

- Ensure that your kubeconfig file is updated and that the current context is pointed to a Kubernetes cluster that you have access to
- Create the CustomResourceDefinition in your cluster using `kubectl apply -f examples/echo-server/crd.yaml`
- Next, run the operator using `RUST_LOG=info cargo run --example examples/echo-server`
- Now use kubectl to create an instance of your EchoServer using `kubectl apply -f examples/echo-server/example.yaml`
- The operator will then ensure that both the Pod and the Service for the EchoServer get created. You can try deleting or modifying the resources and see how the operator responds
