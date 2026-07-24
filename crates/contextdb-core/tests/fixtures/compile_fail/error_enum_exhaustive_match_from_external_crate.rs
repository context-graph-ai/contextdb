// Lives at tests/fixtures/compile_fail/error_enum_exhaustive_match_from_external_crate.rs
// and is exercised by `trybuild::TestCases::new().compile_fail(...)` from
// `tests/error_non_exhaustive_tests.rs`. Trybuild compiles this file as its own
// crate against contextdb-core's public surface — the setting in which
// `#[non_exhaustive]` actually has an effect (it is a no-op for code inside
// contextdb-core itself).
//
// This match lists EVERY `contextdb_core::Error` variant that exists today,
// with no wildcard arm. Before `#[non_exhaustive]` is added to the enum, this
// file compiles cleanly (nothing left uncovered), so the outer trybuild
// assertion (which expects this file to fail to compile) itself fails today
// because compilation unexpectedly SUCCEEDS. Once `#[non_exhaustive]` lands,
// the identical file (unchanged) must fail to compile with "non-exhaustive
// patterns: `_` not covered", because a downstream crate can never treat
// today's variant list as closed — that is the entire product contract this pins.

use contextdb_core::Error;

fn classify(err: &Error) -> &'static str {
    match err {
        Error::TableNotFound(..) => "a",
        Error::ImmutableTable(..) => "a",
        Error::ImmutableColumn { .. } => "a",
        Error::InvalidStateTransition(..) => "a",
        Error::ConditionalUpdateConflict { .. } => "a",
        Error::PropagationAborted { .. } => "a",
        Error::BfsDepthExceeded(..) => "a",
        Error::BfsVisitedExceeded(..) => "a",
        Error::VectorIndexDimensionMismatch { .. } => "a",
        Error::UnknownVectorIndex { .. } => "a",
        Error::PersistedRowVectorRowMissing { .. } => "a",
        Error::PersistedRowVectorCellNull { .. } => "a",
        Error::RankPolicyColumnUnknown { .. } => "a",
        Error::RankPolicyColumnAmbiguous { .. } => "a",
        Error::RankPolicyColumnType { .. } => "a",
        Error::RankPolicyJoinTableUnknown { .. } => "a",
        Error::RankPolicyJoinColumnUnknown { .. } => "a",
        Error::RankPolicyJoinColumnUnindexed { .. } => "a",
        Error::RankPolicyNotFound { .. } => "a",
        Error::RankPolicyFormulaParse { .. } => "a",
        Error::UseRankRequiresVectorOrder => "a",
        Error::UseRankRequiresLimit => "a",
        Error::DropBlockedByRankPolicy { .. } => "a",
        Error::LegacyVectorStoreDetected { .. } => "a",
        Error::StoreCorrupted { .. } => "a",
        Error::NotFound(..) => "a",
        Error::TxNotFound(..) => "a",
        Error::UniqueViolation { .. } => "a",
        Error::ForeignKeyViolation { .. } => "a",
        Error::RecursiveCteNotSupported => "a",
        Error::WindowFunctionNotSupported => "a",
        Error::StoredProcNotSupported => "a",
        Error::UnboundedTraversal => "a",
        Error::UnboundedVectorSearch => "a",
        Error::SubqueryNotSupported => "a",
        Error::FullTextSearchNotSupported => "a",
        Error::OrderByExpressionNotSupported => "a",
        Error::ParseError(..) => "a",
        Error::PlanError(..) => "a",
        Error::SyncError(..) => "a",
        Error::NotSyncEligible(..) => "a",
        Error::CycleDetected { .. } => "a",
        Error::PluginRejected { .. } => "a",
        Error::MemoryBudgetExceeded { .. } => "a",
        Error::DiskBudgetExceeded { .. } => "a",
        Error::ColumnTypeMismatch { .. } => "a",
        Error::TxIdOutOfRange { .. } => "a",
        Error::TxIdOverflow { .. } => "a",
        Error::ColumnNotNullable { .. } => "a",
        Error::IndexNotFound { .. } => "a",
        Error::DuplicateIndex { .. } => "a",
        Error::ReservedIndexName { .. } => "a",
        Error::ColumnNotIndexable { .. } => "a",
        Error::ColumnInIndex { .. } => "a",
        Error::ColumnNotFound { .. } => "a",
        Error::DatabaseLocked { .. } => "a",
        Error::ContextScopeViolation { .. } => "a",
        Error::ScopeLabelViolation { .. } => "a",
        Error::PrincipalRequired { .. } => "a",
        Error::AclDenied { .. } => "a",
        Error::MissedTicksExceeded { .. } => "a",
        Error::EngineNotInitialized { .. } => "a",
        Error::TriggerCallbackMissing { .. } => "a",
        Error::TriggerNotDeclared { .. } => "a",
        Error::TriggerAlreadyRegistered { .. } => "a",
        Error::TriggerCascadeDepthExceeded { .. } => "a",
        Error::TriggerEventUnsupported { .. } => "a",
        Error::TriggerCallbackFailed { .. } => "a",
        Error::TriggerRequiresAdmin { .. } => "a",
        Error::SchemaInvalid { .. } => "a",
        Error::CallbackActiveCrossThread { .. } => "a",
        Error::CallbackReentry { .. } => "a",
        Error::ExportDestinationExists { .. } => "a",
        Error::ExportIo { .. } => "a",
        Error::InputRequiresBlobResolver { .. } => "a",
        Error::SyncPushUnconfirmed { .. } => "a",
        Error::Other(..) => "a",
    }
}

fn main() {
    let _ = classify;
}
