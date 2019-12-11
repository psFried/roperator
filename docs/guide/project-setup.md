# Setting up a new Rust project

You'll first need to make sure you have just a couple of pre-requisites installed

- **Rust 1.39** or later is required. If you don't have it, you can head over to [the Rustup webpage](https://rustup.rs) and run the easy install command. Go ahead, we'll wait.
- **Openssl** libraries and headers are required
    - On OSX, you can just use homebrew to install them by running `brew install openssl@1.1`
    - On debian-based linux: `sudo apt-get install pkg-config libssl-dev`. The pkg-config package is optional, but generally makes things easier
    - On the RedHat linux flavors, `sudo dnf install pkg-config openssl-devel`
    - Check out the [docs here](https://docs.rs/openssl/0.10.26/openssl/index.html#building) if you run into any trouble building the openssl crate
- You'll need some sort of Kubernetes cluster to use for testing out your new operator. For just starting out, it's easiest to work with a test cluster that you have admin access to, like one of the single-node clusters that you can run locally
    - On OSX, [Docker for Mac](https://docs.docker.com/docker-for-mac/install/) has an option to run a single-node Kuberntes cluster inside the docker VM
    - On linux, [Kind](https://kind.sigs.k8s.io/) works well. This is actually what we use to run the Roperator continuous integration tests
    - Or provision a Kuberntes cluster from your provider of choice. There's lots of good options. Just ensure that you can run `kubectl` commands against that cluster to ensure it all works

## Create a new Rust project

Now you're ready to create a new Rust project. Run `cargo new --bin example-operator`, replacing `example-operator` with whatever name you want.

```
~/projects$ cargo new --bin example-operator
     Created binary (application) `example-operator` package
```

This should create the following files:

```
example-operator/
├── Cargo.toml
└── src
    └── main.rs

1 directory, 2 files
```

Change directory into your project and run `cargo run` to ensure that everything works correctly. You should see something like the following:

```
~/projects/example-operator$ cargo run
   Compiling example-operator v0.1.0 (/Users/pfried/projects/example-operator)
    Finished dev [unoptimized + debuginfo] target(s) in 1.54s
     Running `target/debug/example-operator`
Hello, world!
```

## Add the Roperator dependency

Edit `Cargo.toml` and add the following:

```toml
[dependencies]
roperator = "*"

# There's lots of "right" ways to setup logging, so feel free to use something else. For this example, we'll use the `env_logger` crate
log = "0.4"
env_logger = "0.7"

# This is only needed if you intend to use the builtin testkit for testing your operator, which is probably a good idea
[dev-dependencies]
roperator = { version = "*", features = "testkit" }
```

# Next

[Configure the parent Custom Resource Definition](parent.md)
