//! Test-only row oracle that keeps the engine answer funded until its last comparison.
use contextdb_core::Value;
use contextdb_engine::QueryResult;
use std::ops::Deref;

#[derive(Debug)]
pub struct RetainedRows(QueryResult);
impl From<QueryResult> for RetainedRows {
    fn from(answer: QueryResult) -> Self {
        Self(answer)
    }
}
impl AsRef<[Vec<Value>]> for RetainedRows {
    fn as_ref(&self) -> &[Vec<Value>] {
        self.0.rows()
    }
}
impl Deref for RetainedRows {
    type Target = [Vec<Value>];
    fn deref(&self) -> &Self::Target {
        self.0.rows()
    }
}
impl<T: ?Sized + AsRef<[Vec<Value>]>> PartialEq<T> for RetainedRows {
    fn eq(&self, other: &T) -> bool {
        self.0.rows() == other.as_ref()
    }
}

impl serde::Serialize for RetainedRows {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(self.0.rows(), serializer)
    }
}
