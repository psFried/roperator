#[macro_use] extern crate serde_derive;

mod runner;

pub mod resource;
pub mod config;
pub mod handler;

pub use serde_json;
pub use serde;

pub use crate::runner::run_operator;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
