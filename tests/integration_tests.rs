use roperator::prelude::*;
use roperator::runner::testkit::{TestKit, HandlerErrors};
use roperator::resource::ObjectIdRef;

use roperator::serde_json::{json, Value};

use std::time::Duration;
use std::fmt::{self, Display};

fn make_client_config(operator_name: &str) -> ClientConfig {
    if let Some(conf) = ClientConfig::from_service_account(operator_name).ok() {
        conf
    } else {
        ClientConfig::from_kubeconfig(operator_name).expect("Failed to create client configuration")
    }
}

static PARENT_TYPE: &K8sType = &K8sType {
    api_version: "roperator.com/v1alpha1",
    kind: "TestParent",
    plural_kind: "testparents",
};

static CHILD_ONE_TYPE: &K8sType = &K8sType {
    api_version: "roperator.com/v1alpha1",
    kind: "TestChildOne",
    plural_kind: "testchildones",
};

fn setup(name: &str, handler: impl Handler) -> TestKit {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "roperator=INFO");
    }
    let _ = env_logger::try_init();
    let operator_config = OperatorConfig::new(name, PARENT_TYPE)
            .within_namespace(name)
            .with_child(CHILD_ONE_TYPE, ChildConfig::recreate())
            .expose_health(false)
            .expose_metrics(false);
    let client_config = make_client_config(name);

    TestKit::with_test_namespace(name, operator_config, client_config, handler)
            .expect("Failed to create test kit")
}

fn parent(namespace: &str, name: &str) -> Value {
    json!({
        "apiVersion": PARENT_TYPE.api_version,
        "kind": PARENT_TYPE.kind,
        "metadata": {
            "namespace": namespace,
            "name": name,
        },
        "spec": {
            "foo": "bar",
        }
    })
}

#[test]
fn operator_reconciles_a_parent_with_a_child() {
    let namespace = "stable-state";
    let mut testkit = setup(namespace, create_child_handler);

    let parent_name = "parent-one";
    let parent = parent(namespace, parent_name);
    testkit.create_resource(PARENT_TYPE, &parent).expect("failed to create parent resource");

    let expected_child = json!({
        "apiVersion": "roperator.com/v1alpha1",
        "kind": "TestChildOne",
        "metadata": {
            "namespace": namespace,
            "name": parent_name,
            "labels": {
                "app.kubernetes.io/name": parent_name
            }
        },
        "spec": {
            "parentSpec": {
                "foo": "bar",
            },
        }
    });

    let id = ObjectIdRef::new(namespace, parent_name);
    testkit.assert_resource_eq_eventually(CHILD_ONE_TYPE, &id, expected_child, Duration::from_secs(15));
    testkit.delete_parent(&id);
    testkit.assert_resource_deleted_eventually(CHILD_ONE_TYPE, &id, Duration::from_secs(30));
}

#[test]
fn operator_continuously_retries_sync_of_parent_when_handler_returns_error() {
    let namespace = "error-handler-test";
    let error_parent_name = "should-error";
    let normal_parent_name = "should-work";
    let error_id = ObjectIdRef::new(namespace, error_parent_name);
    let normal_id = ObjectIdRef::new(namespace, normal_parent_name);

    let mut testkit = setup(namespace, ReturnErrorHandler::new(create_child_handler, namespace, error_parent_name));
    let error_parent = parent(namespace, error_parent_name);
    testkit.create_resource(PARENT_TYPE, &error_parent).expect("failed to create parent resource");


    let err = testkit.reconcile(Duration::from_secs(4)).err().expect("reconciliation succeeded but should have returned error");
    println!("Got error: {:?}", err);
    let handler_errors = err.as_type::<HandlerErrors>().expect("Expected HandlerErrors but got another type");
    let error_count = handler_errors.get_sync_error_count_for_parent(&error_id);
    assert!(error_count > 0);
    let error_state = testkit.get_current_state_for_parent(&error_id).expect("failed to get children");
    assert!(error_state.children.is_empty());


    let normal_parent = parent(namespace, normal_parent_name);
    testkit.create_resource(PARENT_TYPE, &normal_parent).expect("failed to create parent resource");
    let _ = testkit.reconcile(Duration::from_secs(4));

    // ensure that the normal parent synced without error. This is kinda just as an extra check to ensure that the fixtures are working as expected
    let should_be_0 = handler_errors.get_sync_error_count_for_parent(&ObjectIdRef::new(namespace, normal_parent_name));
    assert_eq!(0, should_be_0);

    let expected_child = json!({
        "apiVersion": "roperator.com/v1alpha1",
        "kind": "TestChildOne",
        "metadata": {
            "namespace": namespace,
            "name": normal_parent_name,
            "labels": {
                "app.kubernetes.io/name": normal_parent_name
            }
        },
        "spec": {
            "parentSpec": {
                "foo": "bar",
            },
        }
    });

    testkit.assert_child_resource_eq(CHILD_ONE_TYPE, &normal_id, expected_child);
}

#[derive(Debug, PartialEq, Clone)]
struct MockHandlerError(u64);
impl Display for MockHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for MockHandlerError { }

struct ReturnErrorHandler<T: Handler> {
    counter: std::sync::atomic::AtomicU64,
    match_namespace: String,
    match_name: String,
    delegate: T,
}

impl <T: Handler> ReturnErrorHandler<T> {
    fn new(delegate: T, match_namespace: &str, match_name: &str) -> ReturnErrorHandler<T> {
        ReturnErrorHandler {
            counter: std::sync::atomic::AtomicU64::new(0),
            match_namespace: match_namespace.to_string(),
            match_name: match_name.to_string(),
            delegate,
        }
    }
    fn should_return_error(&self, req: &SyncRequest) -> bool {
        let ns = req.parent.namespace().unwrap_or("");
        let name = req.parent.name();
        self.match_namespace == ns && self.match_name == name
    }
}

impl<T: Handler> Handler for ReturnErrorHandler<T> {
    fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
        if self.should_return_error(req) {
            let index = self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(Box::new(MockHandlerError(index)))
        } else {
            self.delegate.sync(req)
        }
    }

    fn finalize(&self, req: &SyncRequest) -> Result<FinalizeResponse, Error> {
        if self.should_return_error(req) {
            let index = self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(Box::new(MockHandlerError(index)))
        } else {
            self.delegate.finalize(req)
        }
    }
}

fn create_child_handler(req: &SyncRequest) -> Result<SyncResponse, Error> {
    let namespace = req.parent.namespace();
    let name = req.parent.name();

    let child = json!({
        "apiVersion": "roperator.com/v1alpha1",
        "kind": "TestChildOne",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "labels": {
                "app.kubernetes.io/name": name,
            }
        },
        "spec": {
            "parentSpec": req.parent.pointer("/spec"),
        }
    });

    let children = req.children();
    let test_child_ones = children.of_type_raw(CHILD_ONE_TYPE.api_version, CHILD_ONE_TYPE.kind);
    let count = test_child_ones.iter().count();

    let message = if count == 0 {
        "Waiting for children to be created".to_string()
    } else {
        format!("Has {} children", count)
    };

    Ok(SyncResponse {
        status: json!({
            "message": message,
            "childCount": count,
        }),
        children: vec![child]
    })
}
