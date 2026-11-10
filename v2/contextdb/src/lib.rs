#![doc = include_str!("../README.md")]

mod database;
mod memory;
mod query;
mod read;
mod schema;
mod sql;
mod store;
mod system;
mod types;
mod write;

pub use database::{Database, DatabaseOpenOptions, Transaction};
pub use query::QueryResult;
pub use read::{ReadSession, ReadSessionOptions};
pub use store::{StorageBackend, WrapStorage};
pub use system::Clock;
pub use types::{ColumnType, Error, Result, TxId, Uuid, Value};

/// The JSON package whose `Value` a [`Value::Json`] holds, so a host needs no copy of its own.
pub use serde_json;

/// Writes the map of named values that [`Database::execute`] binds into a statement.
///
/// `params! {}` is the empty map. Each value is anything a [`Value`] can be made from.
#[macro_export]
macro_rules! params {
    () => {
        ::std::collections::HashMap::<::std::string::String, $crate::Value>::new()
    };
    ($($name:expr => $value:expr),+ $(,)?) => {{
        let mut map = ::std::collections::HashMap::<::std::string::String, $crate::Value>::new();
        $(map.insert(::std::string::String::from($name), $crate::Value::from($value));)+
        map
    }};
}
