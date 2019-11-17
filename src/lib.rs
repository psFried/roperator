#![allow(dead_code)]
// TODO: remove dead code and allow directive

#[macro_use] extern crate serde_derive;

mod runner;

pub mod resource;
pub mod config;
pub mod handler;

pub use serde_json;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
