use crate::{
    error::Error,
    runner::{
        OperatorState, create_operator_state, HandlerRef,
        client::Client,
        reconcile::compare,
        metrics::Metrics,
    },
    k8s_types::K8sType,
    config::{OperatorConfig, ClientConfig},
    handler::{Handler, SyncRequest, SyncResponse, FinalizeResponse},
    resource::{K8sResource, ObjectIdRef, ObjectId}
};

use tokio::runtime::current_thread::{Runtime, TaskExecutor};
use serde_json::Value;
use serde::Serialize;

use std::fmt::{self, Debug, Display};
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::time::{Instant, Duration};
use std::collections::{HashSet, HashMap};

macro_rules! test_error {
    ($message:tt) => {{
        Box::new(TestKitError(format!($message))) as Error
    }};
}

pub struct TestKit {
    state: OperatorState,
    handler: HandlerRef,
    instrumented_handler: InstrumentedHandler,
    runtime: Runtime,
    client: Client,
    parents_needing_sync: HashSet<String>,
    delete_namespace_on_drop: bool,
    namespace: Option<String>,
}

impl Debug for TestKit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TestKit")
                .field("operator_state", &self.state)
                .field("instrumented_handler", &self.instrumented_handler)
                .field("client", &self.client)
                .field("parents_needing_sync", &self.parents_needing_sync)
                .finish()
    }
}

pub trait ToJson {
    fn to_json(&self) -> Value;
    fn into_json(self) -> Value;
}

impl <T: Serialize + Clone> ToJson for T {
    fn to_json(&self) -> Value {
        self.clone().into_json()
    }
    fn into_json(self) -> Value {
        serde_json::to_value(self).expect("failed to serialize value")
    }
}

impl Drop for TestKit {
    fn drop(&mut self) {
        if self.delete_namespace_on_drop {
            let TestKit {ref namespace, ref client, ref mut runtime, ..} = self;
            if let Some(ns) = namespace.as_ref() {
                log::info!("Deleting test namespace: '{}'", ns);
                let id = ObjectIdRef::new("", ns);
                let result = runtime.block_on(async {
                    client.delete_resource(crate::k8s_types::core::v1::Namespace, &id).await
                });
                if let Err(err) = result {
                    log::error!("Failed to delete test namespace: '{}': {:?}", ns, err);
                }
            }
        }
    }
}

impl TestKit {

    pub fn with_test_namespace(namespace: impl Into<String>, operator_config: OperatorConfig, client_config: ClientConfig, handler: impl Handler) -> Result<TestKit, Error> {
        let ns = namespace.into();
        let mut testkit = TestKit::create(operator_config.within_namespace(ns.as_str()), client_config, handler)?;

        let ns_type = crate::k8s_types::core::v1::Namespace;
        let namespace_id = ObjectIdRef::new("", ns.as_str());

        if testkit.get_resource_from_api_server(&ns_type, &namespace_id)?.is_none() {
            let namespace_json = serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": ns.as_str(),
                    "labels": {
                        "roperator.com/test-namespace": "true",
                    }
                }
            });
            testkit.create_resource(&ns_type, &namespace_json)?;
        }

        Ok(testkit.delete_namespace_on_drop())
    }

    pub fn delete_namespace_on_drop(mut self) -> Self {
        if self.namespace.is_none() {
            panic!("cannot delete namespace on drop because no namespace has been configured for testkit");
        }
        self.delete_namespace_on_drop = true;
        self
    }

    pub fn create(operator_config: OperatorConfig, client_config: ClientConfig, handler: impl Handler) -> Result<TestKit, Error> {
        let metrics = Metrics::new();
        let client = Client::new(client_config, metrics.client_metrics())?;
        let mut runtime = Runtime::new()?;
        let namespace = operator_config.namespace.clone();

        let state = runtime.block_on( async {
            let mut executor = TaskExecutor::current();
            create_operator_state(&mut executor, metrics, Arc::new(AtomicBool::new(true)), operator_config, client.clone()).await
        });

        let (instrumented_handler, handler) = InstrumentedHandler::wrap(handler);

        Ok(TestKit {
            state,
            handler,
            instrumented_handler,
            runtime,
            client,
            namespace,
            parents_needing_sync: HashSet::new(),
            delete_namespace_on_drop: false,
        })
    }

    pub fn create_parent(&mut self, resource: impl ToJson, reconciliation_timeout: Duration) {
        let parent_type = self.state.runtime_config.parent_type;
        let json = resource.to_json();
        self.create_resource(parent_type, &json).expect("Failed to create parent resource");
        self.reconcile_and_assert_success(reconciliation_timeout);
    }

    pub fn delete_parent(&mut self, id: &ObjectIdRef<'_>) {
        let parent_type = self.state.runtime_config.parent_type;
        self.delete_resource(parent_type, id).expect("failed to delete parent");
    }

    pub fn assert_resource_deleted_eventually(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, timeout: Duration) {
        let start_time = Instant::now();

        let mut result = self.get_resource_from_api_server(k8s_type, id).expect("Failed to get child resource");
        while result.is_some() {
            if start_time.elapsed() > timeout {
                panic!("Timed out waiting for child resource deletion to be observed, resouce: {:#}", result.as_ref().unwrap());
            } else {
                // we need to run the reconciliation so that the operator can remove itself from the paretn finalizer
                // list if needed
                let remaining = timeout.checked_sub(start_time.elapsed())
                        .expect("Timed out waiting for child resource deletion to be observed");
                self.reconcile_and_assert_success(remaining.min(Duration::from_millis(250)));

                result = self.get_resource_from_api_server(k8s_type, id).expect("Failed to get child resource");
            }
        }
        // if we break from the loop then the result must be None
    }

    pub fn assert_resource_exists_eventually(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, timeout: Duration) {
        let start_time = Instant::now();

        let mut result = self.get_resource_from_api_server(k8s_type, id).expect("Failed to get child resource");
        while result.is_none() {
            if start_time.elapsed() > timeout {
                panic!("Timed out waiting for child resource exist, type: {}, id: {}", k8s_type, id);
            } else {
                // we need to run the reconciliation so that the operator can remove itself from the paretn finalizer
                // list if needed
                let remaining = timeout.checked_sub(start_time.elapsed())
                        .expect("Timed out waiting for child resource deletion to be observed");
                self.reconcile_and_assert_success(remaining.min(Duration::from_millis(250)));

                result = self.get_resource_from_api_server(k8s_type, id).expect("Failed to get child resource");
            }
        }
        // result must be some
    }

    pub fn get_current_state_for_parent(&mut self, parent_id: &ObjectIdRef<'_>) -> Result<SyncRequest, Error> {
        let TestKit {ref state, ref mut runtime, ..} = self;

        let req = runtime.block_on( async {
            let parent = state.get_parent_by_id(parent_id).await?.ok_or_else(|| {
                MissingResource {
                    k8s_type: state.runtime_config.parent_type.clone(),
                    id: parent_id.to_owned(),
                }
            })?;

            state.create_sync_request(parent).await
        })?;
        Ok(req)
    }

    pub fn assert_resource_eq_eventually(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, expected: impl ToJson, timeout: Duration) {
        let start_time = Instant::now();
        let expected = expected.into_json();
        let TestKit {ref mut state, ref mut runtime, ref instrumented_handler, ref handler, ref mut parents_needing_sync, ..} = *self;

        let (maybe_diff, maybe_err) = runtime.block_on(async {
            let mut err: Option<Error> = None;
            let mut diff: Option<Diff> = None;
            while start_time.elapsed() < timeout {
                let result = compare_resources(state, k8s_type, id, &expected).await;
                match result {
                    Ok(None) => {
                        diff = None;
                        err = None;
                        break;
                    },
                    Ok(Some(d)) => {
                        let result = do_reconciliation_run(state, parents_needing_sync, handler, instrumented_handler, Duration::from_millis(250).min(timeout)).await;
                        instrumented_handler.reset();
                        diff = Some(d);
                        err = result.err();
                    }
                    Err(e) => {
                        let result = do_reconciliation_run(state, parents_needing_sync, handler, instrumented_handler, Duration::from_millis(250).min(timeout)).await;
                        instrumented_handler.reset();
                        diff = None;
                        err = result.err().or(Some(e));
                    }
                }
            }
            (diff, err)
        });

        if let Some(diff) = maybe_diff {
            panic!("Resource never reached desired state after {:?} : {}", start_time.elapsed(), diff);
        }
        if let Some(err) = maybe_err {
            panic!("Error waiting for resource to reach desired state: {:?}", err);
        }
    }

    pub fn assert_child_resource_eq(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, expected: impl ToJson) {
        let expected = expected.into_json();
        let TestKit {ref state, ref mut runtime, ..} = *self;

        let maybe_diff = runtime.block_on(async {
            compare_resources(state, k8s_type, id, &expected).await
        }).unwrap();
        if let Some(diff) = maybe_diff {
            panic!("Found diff in {}: {}\n{}", k8s_type, id, diff);
        }
    }

    pub fn get_resource_from_api_server(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<Option<Value>, Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        let maybe_resource = runtime.block_on(async {
            client.get_resource(k8s_type, id).await
        })?;
        Ok(maybe_resource)
    }

    pub fn replace_resource(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, new_resource: impl ToJson) -> Result<(), Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        runtime.block_on(async {
            client.replace_resource(k8s_type, id, &new_resource.into_json()).await
        })?;
        Ok(())
    }

    pub fn create_resource(&mut self, k8s_type: &K8sType, new_resource: &Value) -> Result<(), Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        runtime.block_on(async {
            client.create_resource(k8s_type, new_resource).await
        })?;
        Ok(())
    }

    pub fn delete_resource(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<(), Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        runtime.block_on(async {
            client.delete_resource(k8s_type, id).await
        })?;
        Ok(())
    }

    pub fn reconcile_and_assert_success(&mut self, max_timeout: Duration) {
        self.run_reconciliation(true, max_timeout).expect("Reconciliation failed");
    }

    pub fn reconcile(&mut self, max_timeout: Duration) -> Result<(), Error> {
        self.run_reconciliation(true, max_timeout)
    }

    pub fn run_reconciliation(&mut self, fail_on_handler_error: bool, max_timeout: Duration) -> Result<(), Error> {
        let TestKit {ref mut state, ref mut parents_needing_sync, ref handler, ref instrumented_handler, ref mut runtime, ..} = *self;
        let result = runtime.block_on(async {
            do_reconciliation_run(state, parents_needing_sync, handler, instrumented_handler, max_timeout).await
        });
        if !state.is_running() {
            return Err(test_error!("Operator has stopped due to an error"));
        }

        if fail_on_handler_error {
            // Return any handler errors first, before we return any timeout errors
            instrumented_handler.take_errors()?;
        } else {
            instrumented_handler.reset();
        }

        result
    }
}


#[derive(Debug, Default, Clone)]
struct SyncRecord {
    sync_count: usize,
    sync_errors: usize,
    finalize_count: usize,
    finalize_errors: usize,
    last_sync_request: Option<SyncRequest>,
    last_sync_response: Option<Result<SyncResponse, String>>,
    last_finalize_request: Option<SyncRequest>,
    last_finalize_response: Option<Result<FinalizeResponse, String>>,
}

impl SyncRecord {
    fn sync_started(&mut self, req: &SyncRequest) {
        self.sync_count += 1;
        self.last_sync_request = Some(req.clone());
    }

    fn sync_finished(&mut self, resp: &Result<SyncResponse, Error>) {
        match resp.as_ref() {
            Ok(response) => {
                self.last_sync_response = Some(Ok(response.clone()));
            }
            Err(e) => {
                self.sync_errors += 1;
                self.last_sync_response = Some(Err(format!("Handler error: {}", e)));
            }
        }
    }

    fn finalize_started(&mut self, req: &SyncRequest) {
        self.finalize_count += 1;
        self.last_finalize_request = Some(req.clone());
    }

    fn finalize_finished(&mut self, resp: &Result<FinalizeResponse, Error>) {
        match resp.as_ref() {
            Ok(response) => {
                self.last_finalize_response = Some(Ok(response.clone()));
            }
            Err(e) => {
                self.finalize_errors += 1;
                self.last_finalize_response = Some(Err(format!("Handler error: {}", e)));
            }
        }
    }

    fn has_error(&self) -> bool {
        self.sync_errors > 0 || self.finalize_errors > 0
    }
}

#[derive(Clone)]
struct InstrumentedHandler {
    wrapped: HandlerRef,
    records: Arc<RwLock<HashMap<ObjectId, SyncRecord>>>,
}

impl Debug for InstrumentedHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("InstrumentedHandler")
    }
}

impl InstrumentedHandler {
    fn wrap(wrapped: impl Handler) -> (InstrumentedHandler, HandlerRef) {
        let handler_ref = Arc::new(wrapped);
        let handler = InstrumentedHandler {
            wrapped: handler_ref.clone(),
            records: Arc::new(RwLock::new(HashMap::new())),
        };

        let dyn_handler = Arc::new(handler.clone());

        (handler, dyn_handler)
    }

    fn take_errors(&self) -> Result<(), HandlerErrors> {
        let mut records = self.take_records();
        records.retain(|_, record| record.has_error());
        if records.is_empty() {
            Ok(())
        } else {
            Err(HandlerErrors(records))
        }
    }

    fn take_records(&self) -> HashMap<ObjectId, SyncRecord> {
        let mut lock = self.records.write().unwrap();
        std::mem::replace(&mut *lock, HashMap::new())
    }

    fn reset(&self) {
        self.take_records();
    }

    fn total_invocation_count(&self) -> usize {
        let lock = self.records.read().unwrap();
        lock.values().map(|record| record.sync_count + record.finalize_count).sum()
    }
}

impl Handler for InstrumentedHandler {
    fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
        let InstrumentedHandler { ref wrapped, ref records } = self;

        let parent_id = req.parent.get_object_id().into_owned();
        let mut records_lock = records.write().unwrap();
        let record = records_lock.entry(parent_id).or_insert_with(|| SyncRecord::default());
        record.sync_started(req);

        let result = wrapped.sync(req);
        record.sync_finished(&result);
        result
    }

    fn finalize(&self, req: &SyncRequest) -> Result<FinalizeResponse, Error> {
        let InstrumentedHandler { ref wrapped, ref records } = self;

        let parent_id = req.parent.get_object_id().into_owned();
        let mut records_lock = records.write().unwrap();
        let record = records_lock.entry(parent_id).or_insert_with(|| SyncRecord::default());
        record.finalize_started(req);

        let result = wrapped.finalize(req);
        record.finalize_finished(&result);
        result
    }

}

    // TODO: add some sort of "required_quiet_period" parameter so that we can detect hot-loop scenarios
async fn do_reconciliation_run(state: &mut OperatorState, parents_needing_sync: &mut HashSet<String>, handler: &HandlerRef, instrumented_handler: &InstrumentedHandler, max_timeout: Duration) -> Result<(), Error> {
    let mut timeout = max_timeout.min(Duration::from_millis(50));
    let start = Instant::now();
    let starting_invocation_count = instrumented_handler.total_invocation_count();
    let mut last_invocation_count = starting_invocation_count;

    // We'll keep calling run_once until there are no more in-progress sync/finalize requests, but we also continue to call
    // run_once until it stops generating sync requests. We do that so that we can detect "hot loops" where the operator never
    // "settles" on a consistent state. Such states are caused either by a misbehaving Handler or by some other process that
    // keeps modifying a child resource in response to the operator updates.
    while start.elapsed() < max_timeout && state.is_running() {
        state.run_once(parents_needing_sync, handler, timeout).await;
        let current_invocation_count = instrumented_handler.total_invocation_count();
        if state.is_any_update_in_progress() || current_invocation_count > last_invocation_count {
            // either an update is in progress, or the handler has been invoked since the last time
            // so we'll keep going and try again
            log::debug!("Continuing reconciliation last_invocation_count: {}, current_invocation_count: {}", last_invocation_count, current_invocation_count);
            last_invocation_count = current_invocation_count;
            timeout = max_timeout.checked_sub(start.elapsed()).unwrap_or(Duration::from_secs(0));
        } else {
            return Ok(());
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct HandlerErrors(HashMap<ObjectId, SyncRecord>);

impl HandlerErrors {
    pub fn get_sync_error_count_for_parent(&self, id: &ObjectIdRef<'_>) -> usize {
        let id = id.to_owned();
        self.0.get(&id).map(|record| record.sync_errors).unwrap_or(0)
    }
    pub fn get_finalize_error_count_for_parent(&self, id: &ObjectIdRef<'_>) -> usize {
        let id = id.to_owned();
        self.0.get(&id).map(|record| record.finalize_errors).unwrap_or(0)
    }
}

impl std::error::Error for HandlerErrors {}

impl Display for HandlerErrors {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Handler Errors:")?;
        for (id, record) in self.0.iter() {
            write!(f, "\nParent id: {},\n SyncRecord:\n{:?}", id, record)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TestKitError(String);

impl Display for TestKitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Test Error: {}", self.0)
    }
}
impl std::error::Error for TestKitError {}



/// Error representing an operator that never "settled" after repeated attempts to sync or finalize. This condition would manifest
/// as a "hot loop" in a production environment, and is typically caused by repeatedly returning a different value in a SyncResponse.
/// For example, if you always return a new timestamp in one of the values, then the resource would be continuously updated.
#[derive(Debug)]
pub struct ReconciliationIncompleteError {
    records: HashMap<ObjectId, SyncRecord>,
    timeout: Duration,
}

impl std::error::Error for ReconciliationIncompleteError {}

impl Display for ReconciliationIncompleteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total_invocations = self.records.values().map(|record| record.sync_count + record.finalize_count).sum::<usize>();
        let duration_ms = crate::runner::duration_to_millis(self.timeout);
        write!(f, "Operator reconciliation loop never stabilized after {} handler invocations in {}ms", total_invocations, duration_ms)
    }
}


#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Diff {
    expected: Value,
    actual: Value,
    different_paths: Vec<String>,
}

impl Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Diff: \n\texpected:\t {},\n\tactual:\t{},\n\tdifferent_paths:\t{:?}", self.expected, self.actual, self.different_paths)
    }
}

#[derive(Debug)]
pub struct MissingResource {
    pub k8s_type: K8sType,
    pub id: ObjectId,
}

impl Display for MissingResource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Missing Resource: type: {}, id: {}", self.k8s_type, self.id)
    }
}
impl std::error::Error for MissingResource { }

async fn compare_resources(state: &OperatorState, k8s_type: &K8sType, id: &ObjectIdRef<'_>, expected: &Value) -> Result<Option<Diff>, Error> {
    let actual = get_object(state, k8s_type, id).await?;
    Ok(diff_resources(actual.into_value(), expected))
}

async fn get_object(state: &OperatorState, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<K8sResource, Error> {
    let maybe_resource = if k8s_type == state.runtime_config.parent_type {
        let parents = state.parents.lock_state().await?;
        parents.get_by_id(id)
    } else {
        let children = state.children.get(k8s_type).unwrap_or_else(|| {
            panic!("No configuration exists for the resource type: {}", k8s_type);
        });
        let lock = children.lock_state().await?;
        lock.get_by_id(id)
    };
    maybe_resource.ok_or_else(|| {
        MissingResource {
            k8s_type: k8s_type.clone(),
            id: id.to_owned(),
        }.into()
    })
}

fn diff_resources(actual: Value, expected: &Value) -> Option<Diff> {
    let diffs = compare::compare_values(&actual, expected).into_vec();
    let paths: Vec<String> = diffs.into_iter().map(|diff| diff.path).collect();
    if paths.is_empty() {
        None
    } else {
        Some(Diff {
            expected: expected.clone(),
            actual: actual,
            different_paths: paths,
        })
    }
}
