use std::any::Any;

/// Trait for errors that maybe be used by roperator or handlers. This just sets up the
/// trait bounds that are required, since we'll typicaly only expose rather opaque boxed
/// error types, and they will need to be sent between threads.
pub trait RoperatorError: std::error::Error + Send + 'static + Any {
    fn as_any(&self) -> &dyn Any;
}
impl<T> RoperatorError for T
where
    T: std::error::Error + Send + 'static + Any,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Error = Box<dyn RoperatorError>;

impl dyn RoperatorError {
    /// convenience function for downcasting the error to a concrete type
    pub fn as_type<T: RoperatorError>(&self) -> Option<&T> {
        let as_any = self.as_any();
        as_any.downcast_ref::<T>()
    }

    /// convenience function for checking whether the error is of the given concrete type.
    /// If `is_type::<MyType>()` returns true, then calling `as_type::<MyType>()` will return
    /// `Some`.
    pub fn is_type<T: RoperatorError>(&self) -> bool {
        let as_any = self.as_any();
        as_any.is::<T>()
    }
}

impl<T> From<T> for Error
where
    T: RoperatorError,
{
    fn from(e: T) -> Error {
        Box::new(e)
    }
}
