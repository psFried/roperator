# EchoServer example Operator


To run this example:

- Ensure that your kubeconfig file is updated and that the current context is pointed to a Kubernetes cluster that you have access to
- Create the CustomResourceDefinition in your cluster using `kubectl apply -f examples/echo-server/crd.yaml`
- Next, run the operator using `RUST_LOG=info cargo run --example examples/echo-server`
- Now use kubectl to create an instance of your EchoServer using `kubectl apply -f examples/echo-server/example.yaml`
