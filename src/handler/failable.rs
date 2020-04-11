//! Traits and helpers for creating `Handler` implementations that perform some error
//! handling. Most use cases are covered by implementing the `FailableHandler` trait
//! and wrapping your impl in a `DefaultFalableHandler`.
use crate::handler::{Error, FinalizeResponse, Handler, SyncRequest, SyncResponse};

use serde_json::Value;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Configuration that determines the behavior of an exponential backoff.
/// The `Default` impl will use an `initial_interval` of 500 milliseconds,
/// a `max_interval` of 10 minutes, a multiplier of 1.5, and it will never
/// "give up".
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// The starting backoff for the first error. For each subsequent error,
    /// this interval will be multiplied by the `multiplier` to determine the
    /// next backoff, before applying a pseudorandom jitter of +/- 20% of the
    /// duration.
    pub initial_interval: Duration,

    /// The maximum interval that will ever be returned by a backoff
    pub max_interval: Duration,

    /// If this is set to `Some`, then we will no discontinue any retries
    /// after the given duration has elapsed. This duration is measured from the
    /// time of the first error occurrence, and this will be reset after a single
    /// success. If this is None, then we will continue to retry indefinitely.
    pub give_up_after: Option<Duration>,

    /// The multiplier to apply to the backoff on each subsequent error. The
    /// backoff interval will continue to grow until it reaches the `max_interval`
    /// or there is at least one success.
    pub multiplier: f64,

    /// Applies a random jitter to each backoff duration, to vary it by at most the
    /// given multiplier in either direction. This is typically desirable because it
    /// can help spread out reties in cases where many parents all encounter errors
    /// at around the same time.
    pub randomization_factor: f64,
}

impl Default for BackoffConfig {
    fn default() -> BackoffConfig {
        BackoffConfig {
            initial_interval: Duration::from_millis(500),
            max_interval: Duration::from_secs(600),
            give_up_after: None,
            multiplier: 1.5,
            randomization_factor: 0.5,
        }
    }
}

impl BackoffConfig {
    /// Sets backoff to always be at a fixed interval that will never increase or
    /// decrease. Randomization will be disabled as well, so that the interval is
    /// always the same.
    ///
    /// ```rust
    /// use roperator::handler::{BackoffConfig, ErrorBackoff};
    /// use std::time::Duration;
    /// # let request = &roperator::handler::request::test_request();
    /// let interval = Duration::from_millis(500);
    /// let config = BackoffConfig::fixed_interval(interval);
    /// let error_backoff = ErrorBackoff::new(config);
    ///
    /// for _ in 0..10 {
    ///     let backoff_duration = error_backoff.next_error_backoff(request);
    ///     assert_eq!(Some(interval), backoff_duration);
    /// }
    /// ```
    pub fn fixed_interval(interval: Duration) -> BackoffConfig {
        BackoffConfig {
            initial_interval: interval,
            max_interval: interval,
            give_up_after: None,
            multiplier: 1.0,
            randomization_factor: 0.0,
        }
    }

    /// Disables retries entirely. Using this configuration will cause
    /// `ErrorBackoff::next_error_backoff` to always return `None`.
    ///
    /// ```rust
    /// use roperator::handler::{BackoffConfig, ErrorBackoff};
    /// use std::time::Duration;
    /// # let request = &roperator::handler::request::test_request();
    /// let interval = Duration::from_millis(500);
    /// let config = BackoffConfig::never_retry();
    /// let error_backoff = ErrorBackoff::new(config);
    ///
    /// for _ in 0..10 {
    ///     let backoff_duration = error_backoff.next_error_backoff(request);
    ///     assert!(backoff_duration.is_none());
    /// }
    /// ```
    pub fn never_retry() -> BackoffConfig {
        BackoffConfig {
            initial_interval: Duration::from_millis(500),
            give_up_after: Some(Duration::from_millis(0)),
            randomization_factor: 0.0,
            ..Default::default()
        }
    }

    /// Disables the randomization of backoff intervals. By default, backoff intervals will vary
    /// by a random amount in the range of +/-50%. This is typically what you want, since it can
    /// help space out retries if there's a case where errors occur for many resources at the same
    /// time. Disabling this behavior can be useful, though, especially for testing.
    pub fn disable_randomization(mut self) -> Self {
        self.randomization_factor = 0.0;
        self
    }

    fn new_backoff(&self) -> backoff::ExponentialBackoff {
        let start_time = if Some(Duration::from_millis(0)) == self.give_up_after {
            std::time::Instant::now() - Duration::from_millis(500)
        } else {
            std::time::Instant::now()
        };
        backoff::ExponentialBackoff {
            initial_interval: self.initial_interval,
            current_interval: self.initial_interval,
            max_interval: self.max_interval,
            multiplier: self.multiplier,
            max_elapsed_time: self.give_up_after,
            randomization_factor: self.randomization_factor,
            start_time,
            ..Default::default()
        }
    }
}

/// A helper to track the state of error backoffs for each parent resource. This can be used
/// by a handler in order to recover from errors and compute the retry backoff on a per-parent
/// basis. For most use cases, you can implemet `FailableHandler` and use `DefaultFailableHandler`,
/// which will apply the error backoff automatically. This struct is exposed so that you could also
/// use it in your own Handler impl for cases where you need more control over error handling and
/// recovery.
///
/// Example:
///
/// ```rust
/// use roperator::handler::{Handler, ErrorBackoff, SyncRequest, SyncResponse};
/// use roperator::error::Error;
/// use std::io;
///
/// # let request = &roperator::handler::request::test_request();
/// fn try_handle_sync(_req: &SyncRequest) -> Result<SyncResponse, Error> {
///     Err(Error::from(io::Error::new(io::ErrorKind::Other, "oh no, an error!")))
/// }
///
/// struct ErrorRecoveringHandler(ErrorBackoff);
///
/// impl Handler for ErrorRecoveringHandler {
///
///     fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, Error> {
///
///         // if `try_handle_sync` fails, we'll format the error message and put it in the
///         // status, and then retry the sync after a backoff period
///         match try_handle_sync(req) {
///             Ok(resp) => {
///                 self.0.reset_backoff(req);
///                 Ok(resp)
///             }
///             Err(err) => {
///                 let backoff = self.0.next_error_backoff(req);
///                 let status = serde_json::json!({
///                     "error": err.to_string(),
///                 });
///                 Ok(SyncResponse {
///                     status,
///                     resync: backoff,
///                     children: Vec::new(),
///                 })
///             }
///         }
///     }
/// }
///
/// let handler = ErrorRecoveringHandler(ErrorBackoff::default());
/// let response = handler.sync(request).expect("should always return Ok");
/// assert!(response.resync.is_some());
///
/// ```
#[derive(Debug, Default)]
pub struct ErrorBackoff {
    backoff_state: Arc<Mutex<HashMap<String, backoff::ExponentialBackoff>>>,
    backoff_config: BackoffConfig,
}

impl ErrorBackoff {
    /// Constructs a new `ErrorBackoff` from the given configuration.
    ///
    /// Example:
    ///
    /// ```rust
    /// use roperator::handler::{BackoffConfig, ErrorBackoff};
    /// use std::time::Duration;
    ///
    /// let config = BackoffConfig {
    ///     initial_interval: Duration::from_millis(50),
    ///     multiplier: 2.0,
    ///     ..Default::default()
    /// };
    /// let _backoff = ErrorBackoff::new(config);
    /// ```
    pub fn new(backoff_config: BackoffConfig) -> ErrorBackoff {
        ErrorBackoff {
            backoff_config,
            backoff_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the next backoff for the parent in the given request. The first time
    /// this is called, it will return a backoff based on the `initial_interval`. It
    /// will be increased by the multiplier on each subsequent call, until `reset_backoff`
    /// is called for the parent.
    ///
    /// ```rust
    /// use roperator::handler::{BackoffConfig, ErrorBackoff};
    /// # let request = &roperator::handler::request::test_request();
    /// // disable randomization to make backoff durations deterministic for tests
    /// let error_backoff = ErrorBackoff::new(BackoffConfig::default().disable_randomization());
    ///
    /// let mut duration = error_backoff.next_error_backoff(request).unwrap();
    /// for _ in 0..10 {
    ///     let next = error_backoff.next_error_backoff(request).unwrap();
    ///     assert!(next > duration);
    ///     duration = next;
    /// }
    /// ```
    pub fn next_error_backoff(&self, req: &SyncRequest) -> Option<Duration> {
        use backoff::backoff::Backoff;

        let ErrorBackoff {
            ref backoff_state,
            ref backoff_config,
        } = *self;
        let uid = req.parent.uid();
        let mut backoffs = backoff_state.lock().unwrap();
        if !backoffs.contains_key(uid) {
            let backoff = backoff_config.new_backoff();
            backoffs.insert(uid.to_owned(), backoff);
        }
        let bo = backoffs.get_mut(uid).unwrap();
        bo.next_backoff()
    }

    /// Resets the error backoff for the given parent. This should be called after _every_
    /// sucessful sync or finalize call to ensure that the error state gets cleared.
    /// After this is called, the next error duration will go back down to the `initial_interval`.
    ///
    /// ```rust
    /// use roperator::handler::{BackoffConfig, ErrorBackoff};
    /// # let request = &roperator::handler::request::test_request();
    /// let config = BackoffConfig::default().disable_randomization();
    /// let error_backoff = ErrorBackoff::new(config.clone());
    ///
    /// let mut duration = error_backoff.next_error_backoff(request).unwrap();
    /// for _ in 0..20 {
    ///     duration = error_backoff.next_error_backoff(request).unwrap();
    /// }
    /// assert!(duration > config.initial_interval);
    ///
    /// error_backoff.reset_backoff(request);
    /// duration = error_backoff.next_error_backoff(request).unwrap();
    /// assert_eq!(config.initial_interval, duration);
    /// ```
    pub fn reset_backoff(&self, req: &SyncRequest) {
        let uid = req.parent.uid();
        let mut backoffs = self.backoff_state.lock().unwrap();
        backoffs.remove(uid);
    }
}

/// An optional trait for creating handlers that want to recover from their own errors.
/// This provides an opinionated base that should work well for most operators.
/// Sync and finalize operations are broken down into separate steps, one for the action
/// itself, and another for determining the status. If `sync_children` returns an `Err`
/// then the error will be passed to `determine_status` so that it can be described in the
/// status of the parent. Same goes for `finalize`. Like the base `Handler` trait, this trait
/// provides a default implementation of `finalize` for common cases where no special finalization
/// logic is required.
///
/// Unlike the base `Handler` trait, `FailableHandler` impls may use any eror type they wish,
/// even those that are not `Send` or `'static`.
pub trait FailableHandler: Send + Sync + 'static {
    type Error: Debug;
    type Status: serde::Serialize + serde::de::DeserializeOwned;

    /// Returns the list of desired children for this parent. This function will be called
    /// in the same was as the base `sync` function, except that it only returns the list of
    /// children instead of the entire response.
    fn sync_children(&self, req: &SyncRequest) -> Result<Vec<Value>, Self::Error>;

    /// Finalize this parent. The default implementation simply allows the deletion to proceed
    /// as normal.
    fn finalize(&self, _req: &SyncRequest) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Determines the current status of the parent based on the current state of the child
    /// resources and whether an error was returned by `sync_children` or `finalize`. If an
    /// error was returned, then it will be passed to this function so that it can be described
    /// in the status.
    fn determine_status(&self, req: &SyncRequest, error: Option<Self::Error>) -> Self::Status;
}

impl<Syncf, Sf, Status, E> FailableHandler for (Syncf, Sf)
where
    E: Debug,
    Status: serde::Serialize + serde::de::DeserializeOwned,
    Syncf: Fn(&SyncRequest) -> Result<Vec<Value>, E> + Send + Sync + 'static,
    Sf: Fn(&SyncRequest, Option<E>) -> Status + Send + Sync + 'static,
{
    type Error = E;
    type Status = Status;

    fn sync_children(&self, req: &SyncRequest) -> Result<Vec<Value>, Self::Error> {
        (self.0)(req)
    }

    fn determine_status(&self, req: &SyncRequest, error: Option<Self::Error>) -> Self::Status {
        (self.1)(req, error)
    }
}

/// An opinionated and batteries-included helper for implementing Handlers that can recover from
/// errors by setting a status on the parent that describes the error. This struct wraps a
/// `FailableHandler` and adapts it to the `Handler` trait.
///
/// Example:
///
/// ```rust,no_run
/// use roperator::handler::{DefaultFailableHandler, SyncRequest};
/// use roperator::error::Error;
/// use serde::{Serialize, Deserialize};
/// use std::time::Duration;
/// use serde_json::{json, Value};
///
/// let sync_children: fn(&SyncRequest) -> Result<Vec<Value>, Error> = |req| {
///     Ok(vec![
///         json!({
///             "apiVersion": "v1",
///             "kind": "Pod",
///             // omitted for the sake of brevity
///         })
///     ])
/// };
/// let determine_status = |req: &SyncRequest, err: Option<Error>| {
///     let error_json = err.as_ref()
///         .map(|_| Value::from("omg there was an error"))
///         .unwrap_or(Value::Null);
///     json!({
///         "ok": err.is_none(),
///         "error": error_json,
///     })
/// };
/// let handler = DefaultFailableHandler::wrap((sync_children, determine_status))
///     .with_regular_resync(Duration::from_secs(300));
/// ```
pub struct DefaultFailableHandler<H: FailableHandler> {
    inner: H,
    error_backoff: ErrorBackoff,
    regular_resync: Option<Duration>,
}

impl<F: FailableHandler> DefaultFailableHandler<F> {
    /// Wraps a `FailableHandler` to provide a default implementation of `Handler`. The returned
    /// handler will use the default exponential backoff.
    pub fn wrap(failable: F) -> DefaultFailableHandler<F> {
        DefaultFailableHandler::new(failable, BackoffConfig::default(), None)
    }

    /// Complete constructor for creating a Handler that uses the given `backoff_config`.
    ///
    /// The `regular_resync` parameter allows the handler to resync on a regular repeating interval,
    /// even when no error occurs. This is useful for cases where the operator needs to sync with
    /// some external resource outside of the kuberentes cluster. If it is `Some`, then `sync` will
    /// be called regularly on the given interval, even when there is no change to the parent or
    /// children. If `None` then `sync` will only be called when there is a change to the parent
    /// or any child resource.
    pub fn new(
        failable: F,
        backoff_config: BackoffConfig,
        regular_resync: Option<Duration>,
    ) -> DefaultFailableHandler<F> {
        DefaultFailableHandler {
            inner: failable,
            error_backoff: ErrorBackoff::new(backoff_config),
            regular_resync,
        }
    }

    /// Sets this handler to re-sync at the given regular interval, even when there is no change
    /// to the parent or any child resources. This is useful for cases where the operator needs
    /// to sync with some external resource outside of the kuberentes cluster.
    pub fn with_regular_resync(mut self, resync_interval: Duration) -> Self {
        self.regular_resync = Some(resync_interval);
        self
    }

    /// Sets the backoff configuration to use for handling errors
    pub fn with_backoff(mut self, backoff_config: BackoffConfig) -> Self {
        self.error_backoff.backoff_config = backoff_config;
        self
    }
}

impl<H: FailableHandler> Handler for DefaultFailableHandler<H> {
    fn sync(&self, request: &SyncRequest) -> Result<SyncResponse, Error> {
        let (error, children, resync) = match self.inner.sync_children(request) {
            Ok(kids) => {
                self.error_backoff.reset_backoff(request);
                (None, kids, self.regular_resync)
            }
            Err(err) => {
                log::error!(
                    "sync_children for parent: {} returned error: {:?}",
                    request.parent.get_object_id(),
                    err
                );
                let duration = self.error_backoff.next_error_backoff(request);
                (Some(err), Vec::new(), duration)
            }
        };

        let status = self.inner.determine_status(request, error);

        // if there's a serialization error, then we'll rely on roperator's builtin backoff.
        // These error conditions are expected to be pretty rare.
        let status_json = serde_json::to_value(status).map_err(|err| {
            log::error!(
                "Failed to serialize status of parent: {}, err: {:?}",
                request.parent.get_object_id(),
                err
            );
            Error::from(err)
        })?;

        Ok(SyncResponse {
            resync,
            children,
            status: status_json,
        })
    }

    fn finalize(&self, request: &SyncRequest) -> Result<FinalizeResponse, Error> {
        let error = self.inner.finalize(request).err();

        let retry = if let Some(err) = error.as_ref() {
            log::error!(
                "Failed to finalize parent: {}, err: {:?}",
                request.parent.get_object_id(),
                err
            );
            self.error_backoff.next_error_backoff(request)
        } else {
            self.error_backoff.reset_backoff(request);
            None
        };
        let status_struct = self.inner.determine_status(request, error);
        let status = serde_json::to_value(status_struct).map_err(|err| {
            log::error!(
                "Failed to serialize status of parent: {}, err: {:?}",
                request.parent.get_object_id(),
                err
            );
            Error::from(err)
        })?;
        Ok(FinalizeResponse { status, retry })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::handler::request::{test_request, SyncRequest};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestStatus {
        error: Option<String>,
    }

    #[derive(Debug)]
    struct TestError;

    #[test]
    fn backoff_is_reset_after_successful_sync() {
        let return_error = Arc::new(AtomicBool::new(true));
        let error_control = return_error.clone();
        let status_error = return_error.clone();
        let sync_fun = move |_req: &SyncRequest| {
            if return_error.load(Ordering::SeqCst) {
                Err(TestError)
            } else {
                Ok(Vec::<Value>::new())
            }
        };
        let status_fun = move |_req: &SyncRequest, err: Option<TestError>| {
            let expected_error = status_error.load(Ordering::SeqCst);
            assert_eq!(expected_error, err.is_some());
            let error = err.map(|_| "omg there was an error".to_owned());
            TestStatus { error }
        };

        let backoff_config = BackoffConfig::default().disable_randomization();
        let handler = DefaultFailableHandler::wrap((sync_fun, status_fun))
            .with_backoff(backoff_config.clone());

        let request = &test_request();

        for i in 0..5 {
            let resp = handler.sync(request).expect("handler returned err");
            assert!(resp.resync.is_some());
            if i == 0 {
                assert_eq!(backoff_config.initial_interval, resp.resync.unwrap());
            } else {
                assert!(resp.resync.unwrap() > backoff_config.initial_interval);
            }
            assert!(resp.children.is_empty());
            let expected_status = serde_json::json!({
                "error": "omg there was an error"
            });
            assert_eq!(expected_status, resp.status);
        }

        error_control.store(false, Ordering::SeqCst);

        let resp = handler.sync(request).expect("handler returned an error");
        assert!(resp.resync.is_none());

        // now trigger another error and assert that the resync interval has gone back down
        error_control.store(true, Ordering::SeqCst);
        let resp = handler.sync(request).expect("handler returned an error");
        assert_eq!(Some(backoff_config.initial_interval), resp.resync);
    }
}
