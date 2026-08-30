use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use contextdb_core::Error;
use contextdb_core::read_contract::{
    ChannelAddress, CursorExpiryKind, CursorPage, DatabaseIdentity, DeadlineClock, DeadlineWait,
    HeldByReadersDetail, LocalUserIdentity, MetadataItem, MetadataPage, MetadataPageVocabulary,
    OwnerLimitExceededDetail, OwnerReadCancellation, OwnerReadLimits, OwnerReadStatus,
    OwnerRequestHandler, OwnerRouteUnsupportedDetail, OwnerServiceTimeouts, OwnerServingReason,
    OwnerServingState, ProcessStartIdentity, ReadClientTimeouts, ReadContractViolation,
    ReadFailure, ReadFailureClass, ReadFailureConstructionError, ReadFailureDetail,
    ReadFailureKind, ReadFailureLimit, ReadLimitField, ReadLimits, ReadRoute, ReadTimeoutField,
    ReaderBreadcrumb, ReaderBreadcrumbLocation, ReaderProcessIdentity, RequiredBytesSetting,
    StatementRemedy, WriterRunNumber,
};
use std::any::TypeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

fn shipped_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4 * 1024 * 1024,
        work: 50_000,
        active_ms: 5_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 300_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn with_limit_field(mut limits: ReadLimits, field: ReadLimitField, value: u64) -> ReadLimits {
    match field {
        ReadLimitField::ResultRows => limits.result_rows = value,
        ReadLimitField::ResultBytes => limits.result_bytes = value,
        ReadLimitField::Work => limits.work = value,
        ReadLimitField::ActiveMs => limits.active_ms = value,
        ReadLimitField::Memory => limits.memory = value,
        ReadLimitField::CursorPageRows => limits.cursor_page_rows = value,
        ReadLimitField::CursorPageBytes => limits.cursor_page_bytes = value,
        ReadLimitField::CursorIdleMs => limits.cursor_idle_ms = value,
        ReadLimitField::CursorLifetimeMs => limits.cursor_lifetime_ms = value,
        ReadLimitField::Concurrency => panic!("concurrency is not a ReadLimits field"),
    }
    limits
}

#[test]
fn read_limits_ship_the_exact_read_policy() {
    assert_eq!(ReadLimits::default(), shipped_limits());
}

#[test]
fn owner_limits_and_timeouts_ship_the_exact_policy() {
    assert_eq!(
        OwnerReadLimits::default(),
        OwnerReadLimits {
            limits: shipped_limits(),
            concurrency: 4,
        }
    );
    assert_eq!(
        ReadClientTimeouts::default(),
        ReadClientTimeouts {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        }
    );
    assert_eq!(
        OwnerServiceTimeouts::default(),
        OwnerServiceTimeouts {
            request_ms: 10_000,
            shutdown_drain_ms: 10_000,
        }
    );
}

#[test]
fn read_limit_validation_rejects_zero_and_every_invalid_relationship() {
    let valid = shipped_limits();
    let valid_policies = [
        valid,
        ReadLimits {
            result_rows: 37,
            result_bytes: 4_099,
            work: 83,
            active_ms: 211,
            memory: 8_191,
            cursor_page_rows: 13,
            cursor_page_bytes: 1_021,
            cursor_idle_ms: 43,
            cursor_lifetime_ms: 127,
        },
        ReadLimits {
            result_rows: 999,
            result_bytes: 9_000_000,
            work: 72_000,
            active_ms: 8_500,
            memory: 24_000_000,
            cursor_page_rows: 333,
            cursor_page_bytes: 2_000_000,
            cursor_idle_ms: 120_000,
            cursor_lifetime_ms: 900_000,
        },
    ];
    for policy in valid_policies {
        assert_eq!(policy.validate(), Ok(()));
    }

    let zero_cases = [
        (
            ReadLimitField::ResultRows,
            ReadLimits {
                result_rows: 0,
                ..valid
            },
        ),
        (
            ReadLimitField::ResultBytes,
            ReadLimits {
                result_bytes: 0,
                ..valid
            },
        ),
        (ReadLimitField::Work, ReadLimits { work: 0, ..valid }),
        (
            ReadLimitField::ActiveMs,
            ReadLimits {
                active_ms: 0,
                ..valid
            },
        ),
        (ReadLimitField::Memory, ReadLimits { memory: 0, ..valid }),
        (
            ReadLimitField::CursorPageRows,
            ReadLimits {
                cursor_page_rows: 0,
                ..valid
            },
        ),
        (
            ReadLimitField::CursorPageBytes,
            ReadLimits {
                cursor_page_bytes: 0,
                ..valid
            },
        ),
        (
            ReadLimitField::CursorIdleMs,
            ReadLimits {
                cursor_idle_ms: 0,
                ..valid
            },
        ),
        (
            ReadLimitField::CursorLifetimeMs,
            ReadLimits {
                cursor_lifetime_ms: 0,
                ..valid
            },
        ),
    ];
    for (field, zero) in zero_cases {
        assert_eq!(
            zero.validate(),
            Err(ReadContractViolation::ZeroLimit { field })
        );
    }

    let mut page_rows = valid;
    page_rows.cursor_page_rows = page_rows.result_rows + 1;
    assert_eq!(
        page_rows.validate(),
        Err(ReadContractViolation::CursorPageRowsExceedResultRows)
    );

    let mut page_bytes = valid;
    page_bytes.cursor_page_bytes = page_bytes.result_bytes + 1;
    assert_eq!(
        page_bytes.validate(),
        Err(ReadContractViolation::CursorPageBytesExceedResultBytes)
    );

    let mut cursor_time = valid;
    cursor_time.cursor_idle_ms = cursor_time.cursor_lifetime_ms + 1;
    assert_eq!(
        cursor_time.validate(),
        Err(ReadContractViolation::CursorIdleExceedsLifetime)
    );
}

#[test]
fn owner_limits_validate_their_read_limits_and_positive_concurrency() {
    let valid = OwnerReadLimits {
        limits: shipped_limits(),
        concurrency: 4,
    };
    assert_eq!(valid.validate(), Ok(()));

    let zero_concurrency = OwnerReadLimits {
        concurrency: 0,
        ..valid
    };
    assert_eq!(
        zero_concurrency.validate(),
        Err(ReadContractViolation::ZeroLimit {
            field: ReadLimitField::Concurrency,
        })
    );

    let invalid_inner_limit = OwnerReadLimits {
        limits: ReadLimits {
            result_rows: 0,
            ..shipped_limits()
        },
        concurrency: 4,
    };
    assert_eq!(
        invalid_inner_limit.validate(),
        Err(ReadContractViolation::ZeroLimit {
            field: ReadLimitField::ResultRows,
        })
    );
}

#[test]
fn owner_route_uses_the_stricter_limit_field_by_field() {
    let requested = ReadLimits {
        result_rows: 900,
        result_bytes: 2 * 1024 * 1024,
        work: 60_000,
        active_ms: 4_000,
        memory: 20 * 1024 * 1024,
        cursor_page_rows: 80,
        cursor_page_bytes: 2 * 1024 * 1024,
        cursor_idle_ms: 400_000,
        cursor_lifetime_ms: 1_000_000,
    };
    let owner_maximum = shipped_limits();

    assert_eq!(
        ReadLimits::stricter_of(requested, owner_maximum),
        Ok(ReadLimits {
            result_rows: 500,
            result_bytes: 2 * 1024 * 1024,
            work: 50_000,
            active_ms: 4_000,
            memory: 16 * 1024 * 1024,
            cursor_page_rows: 80,
            cursor_page_bytes: 1024 * 1024,
            cursor_idle_ms: 300_000,
            cursor_lifetime_ms: 1_000_000,
        })
    );
}

#[test]
fn stricter_limits_choose_each_mutated_field_in_both_orders() {
    let roomy = ReadLimits {
        result_rows: 1_000,
        result_bytes: 10_000,
        work: 1_000,
        active_ms: 1_000,
        memory: 10_000,
        cursor_page_rows: 100,
        cursor_page_bytes: 1_000,
        cursor_idle_ms: 1_000,
        cursor_lifetime_ms: 10_000,
    };
    let lower_values = [
        (ReadLimitField::ResultRows, 900),
        (ReadLimitField::ResultBytes, 9_000),
        (ReadLimitField::Work, 900),
        (ReadLimitField::ActiveMs, 900),
        (ReadLimitField::Memory, 9_000),
        (ReadLimitField::CursorPageRows, 90),
        (ReadLimitField::CursorPageBytes, 900),
        (ReadLimitField::CursorIdleMs, 900),
        (ReadLimitField::CursorLifetimeMs, 9_000),
    ];

    for (field, lower_value) in lower_values {
        let one_field_lower = with_limit_field(roomy, field, lower_value);
        assert_eq!(
            ReadLimits::stricter_of(roomy, one_field_lower),
            Ok(one_field_lower),
            "owner maximum must win for {field:?}",
        );
        assert_eq!(
            ReadLimits::stricter_of(one_field_lower, roomy),
            Ok(one_field_lower),
            "caller request must win for {field:?}",
        );
    }
}

#[test]
fn stricter_limits_reject_invalid_requested_and_owner_policies() {
    let valid = ReadLimits {
        result_rows: 1_000,
        result_bytes: 10_000,
        work: 1_000,
        active_ms: 1_000,
        memory: 10_000,
        cursor_page_rows: 100,
        cursor_page_bytes: 1_000,
        cursor_idle_ms: 1_000,
        cursor_lifetime_ms: 10_000,
    };

    let invalid_requested = ReadLimits { work: 0, ..valid };
    assert_eq!(
        ReadLimits::stricter_of(invalid_requested, valid),
        Err(ReadContractViolation::ZeroLimit {
            field: ReadLimitField::Work,
        })
    );

    let invalid_owner = ReadLimits { memory: 0, ..valid };
    assert_eq!(
        ReadLimits::stricter_of(valid, invalid_owner),
        Err(ReadContractViolation::ZeroLimit {
            field: ReadLimitField::Memory,
        })
    );

    let invalid_requested_relationship = ReadLimits {
        cursor_page_bytes: valid.result_bytes + 1,
        ..valid
    };
    assert_eq!(
        ReadLimits::stricter_of(invalid_requested_relationship, valid),
        Err(ReadContractViolation::CursorPageBytesExceedResultBytes)
    );

    let invalid_owner_relationship = ReadLimits {
        cursor_idle_ms: valid.cursor_lifetime_ms + 1,
        ..valid
    };
    assert_eq!(
        ReadLimits::stricter_of(valid, invalid_owner_relationship),
        Err(ReadContractViolation::CursorIdleExceedsLifetime)
    );
}

#[test]
fn client_and_service_timeouts_are_positive_and_response_exceeds_request() {
    let client = ReadClientTimeouts {
        connect_ms: 1_000,
        routing_retry_ms: 1_000,
        response_ms: 11_000,
    };
    let owner = OwnerServiceTimeouts {
        request_ms: 10_000,
        shutdown_drain_ms: 10_000,
    };
    assert_eq!(client.validate(), Ok(()));
    assert_eq!(owner.validate(), Ok(()));
    assert_eq!(client.validate_with_owner(owner), Ok(()));

    let client_zero_cases = [
        (
            ReadTimeoutField::ConnectMs,
            ReadClientTimeouts {
                connect_ms: 0,
                ..client
            },
        ),
        (
            ReadTimeoutField::RoutingRetryMs,
            ReadClientTimeouts {
                routing_retry_ms: 0,
                ..client
            },
        ),
        (
            ReadTimeoutField::ResponseMs,
            ReadClientTimeouts {
                response_ms: 0,
                ..client
            },
        ),
    ];
    for (field, zero) in client_zero_cases {
        assert_eq!(
            zero.validate(),
            Err(ReadContractViolation::ZeroTimeout { field })
        );
    }

    let service_zero_cases = [
        (
            ReadTimeoutField::RequestMs,
            OwnerServiceTimeouts {
                request_ms: 0,
                ..owner
            },
        ),
        (
            ReadTimeoutField::ShutdownDrainMs,
            OwnerServiceTimeouts {
                shutdown_drain_ms: 0,
                ..owner
            },
        ),
    ];
    for (field, zero) in service_zero_cases {
        assert_eq!(
            zero.validate(),
            Err(ReadContractViolation::ZeroTimeout { field })
        );
    }

    let too_short = ReadClientTimeouts {
        response_ms: owner.request_ms,
        ..client
    };
    assert_eq!(
        too_short.validate_with_owner(owner),
        Err(ReadContractViolation::ResponseMustExceedRequest)
    );
    let shorter = ReadClientTimeouts {
        response_ms: owner.request_ms - 1,
        ..client
    };
    assert_eq!(
        shorter.validate_with_owner(owner),
        Err(ReadContractViolation::ResponseMustExceedRequest)
    );

    let overflow_cases = [
        (
            ReadTimeoutField::ConnectMs,
            ReadClientTimeouts {
                connect_ms: u64::MAX,
                ..client
            },
        ),
        (
            ReadTimeoutField::RoutingRetryMs,
            ReadClientTimeouts {
                routing_retry_ms: u64::MAX,
                ..client
            },
        ),
        (
            ReadTimeoutField::ResponseMs,
            ReadClientTimeouts {
                response_ms: u64::MAX,
                ..client
            },
        ),
    ];
    for (field, overflow) in overflow_cases {
        assert_eq!(
            overflow.validate(),
            Err(ReadContractViolation::TimeoutOverflow { field })
        );
    }

    let service_overflow_cases = [
        (
            ReadTimeoutField::RequestMs,
            OwnerServiceTimeouts {
                request_ms: u64::MAX,
                ..owner
            },
        ),
        (
            ReadTimeoutField::ShutdownDrainMs,
            OwnerServiceTimeouts {
                shutdown_drain_ms: u64::MAX,
                ..owner
            },
        ),
    ];
    for (field, overflow) in service_overflow_cases {
        assert_eq!(
            overflow.validate(),
            Err(ReadContractViolation::TimeoutOverflow { field })
        );
    }
}

#[test]
fn clock_aware_timeout_validation_checks_exact_deadline_boundaries() {
    let client = ReadClientTimeouts {
        connect_ms: 3,
        routing_retry_ms: 5,
        response_ms: 17,
    };
    let owner = OwnerServiceTimeouts {
        request_ms: 11,
        shutdown_drain_ms: 13,
    };

    for (field, now_ms, at_boundary, one_past_boundary) in [
        (
            ReadTimeoutField::ConnectMs,
            11,
            ReadClientTimeouts {
                connect_ms: u64::MAX - 11,
                ..client
            },
            ReadClientTimeouts {
                connect_ms: u64::MAX - 10,
                ..client
            },
        ),
        (
            ReadTimeoutField::RoutingRetryMs,
            37,
            ReadClientTimeouts {
                routing_retry_ms: u64::MAX - 37,
                ..client
            },
            ReadClientTimeouts {
                routing_retry_ms: u64::MAX - 36,
                ..client
            },
        ),
        (
            ReadTimeoutField::ResponseMs,
            1_001,
            ReadClientTimeouts {
                response_ms: u64::MAX - 1_001,
                ..client
            },
            ReadClientTimeouts {
                response_ms: u64::MAX - 1_000,
                ..client
            },
        ),
    ] {
        assert_eq!(at_boundary.validate_at(now_ms), Ok(()));
        assert_eq!(at_boundary.validate_with_owner_at(owner, now_ms), Ok(()));
        assert_eq!(
            one_past_boundary.validate_at(now_ms),
            Err(ReadContractViolation::TimeoutOverflow { field })
        );
        assert_eq!(
            one_past_boundary.validate_with_owner_at(owner, now_ms),
            Err(ReadContractViolation::TimeoutOverflow { field })
        );
    }

    for (field, now_ms, at_boundary, one_past_boundary) in [
        (
            ReadTimeoutField::RequestMs,
            7,
            OwnerServiceTimeouts {
                request_ms: u64::MAX - 7,
                ..owner
            },
            OwnerServiceTimeouts {
                request_ms: u64::MAX - 6,
                ..owner
            },
        ),
        (
            ReadTimeoutField::ShutdownDrainMs,
            113,
            OwnerServiceTimeouts {
                shutdown_drain_ms: u64::MAX - 113,
                ..owner
            },
            OwnerServiceTimeouts {
                shutdown_drain_ms: u64::MAX - 112,
                ..owner
            },
        ),
    ] {
        assert_eq!(at_boundary.validate_at(now_ms), Ok(()));
        assert_eq!(
            one_past_boundary.validate_at(now_ms),
            Err(ReadContractViolation::TimeoutOverflow { field })
        );
    }

    let shutdown_now_ms = 113;
    let shutdown_at_boundary = OwnerServiceTimeouts {
        shutdown_drain_ms: u64::MAX - shutdown_now_ms,
        ..owner
    };
    assert_eq!(
        client.validate_with_owner_at(shutdown_at_boundary, shutdown_now_ms),
        Ok(())
    );
    assert_eq!(
        client.validate_with_owner_at(
            OwnerServiceTimeouts {
                shutdown_drain_ms: u64::MAX - shutdown_now_ms + 1,
                ..owner
            },
            shutdown_now_ms,
        ),
        Err(ReadContractViolation::TimeoutOverflow {
            field: ReadTimeoutField::ShutdownDrainMs,
        })
    );

    let request_now_ms = 211;
    let response_at_boundary = u64::MAX - request_now_ms;
    assert_eq!(
        ReadClientTimeouts {
            response_ms: response_at_boundary,
            ..client
        }
        .validate_with_owner_at(
            OwnerServiceTimeouts {
                request_ms: response_at_boundary - 1,
                ..owner
            },
            request_now_ms,
        ),
        Ok(())
    );

    let request_at_boundary = u64::MAX - request_now_ms;
    assert_eq!(
        ReadClientTimeouts {
            response_ms: request_at_boundary + 1,
            ..client
        }
        .validate_with_owner_at(
            OwnerServiceTimeouts {
                request_ms: request_at_boundary,
                ..owner
            },
            request_now_ms,
        ),
        Err(ReadContractViolation::TimeoutOverflow {
            field: ReadTimeoutField::ResponseMs,
        })
    );
    assert_eq!(
        ReadClientTimeouts {
            response_ms: request_at_boundary + 2,
            ..client
        }
        .validate_with_owner_at(
            OwnerServiceTimeouts {
                request_ms: request_at_boundary + 1,
                ..owner
            },
            request_now_ms,
        ),
        Err(ReadContractViolation::TimeoutOverflow {
            field: ReadTimeoutField::RequestMs,
        })
    );

    let client_zero_cases = [
        (
            ReadTimeoutField::ConnectMs,
            ReadClientTimeouts {
                connect_ms: 0,
                ..client
            },
        ),
        (
            ReadTimeoutField::RoutingRetryMs,
            ReadClientTimeouts {
                routing_retry_ms: 0,
                ..client
            },
        ),
        (
            ReadTimeoutField::ResponseMs,
            ReadClientTimeouts {
                response_ms: 0,
                ..client
            },
        ),
    ];
    for (field, zero) in client_zero_cases {
        let expected = Err(ReadContractViolation::ZeroTimeout { field });
        assert_eq!(zero.validate_at(29), expected.clone());
        assert_eq!(zero.validate_with_owner_at(owner, 29), expected);
    }

    let owner_zero_cases = [
        (
            ReadTimeoutField::RequestMs,
            OwnerServiceTimeouts {
                request_ms: 0,
                ..owner
            },
        ),
        (
            ReadTimeoutField::ShutdownDrainMs,
            OwnerServiceTimeouts {
                shutdown_drain_ms: 0,
                ..owner
            },
        ),
    ];
    for (field, zero) in owner_zero_cases {
        let expected = Err(ReadContractViolation::ZeroTimeout { field });
        assert_eq!(zero.validate_at(29), expected.clone());
        assert_eq!(client.validate_with_owner_at(zero, 29), expected);
    }

    assert_eq!(
        ReadClientTimeouts {
            response_ms: owner.request_ms,
            ..client
        }
        .validate_with_owner_at(owner, 29),
        Err(ReadContractViolation::ResponseMustExceedRequest)
    );
    assert_eq!(
        ReadClientTimeouts {
            response_ms: owner.request_ms - 1,
            ..client
        }
        .validate_with_owner_at(owner, 29),
        Err(ReadContractViolation::ResponseMustExceedRequest)
    );
}

#[test]
fn core_bincode_dependency_serializes_contract_values_without_a_test_only_dependency() {
    let limits = shipped_limits();
    let bytes = encode_to_vec(limits, standard()).expect("serialize a declared limit policy");
    let (decoded, used): (ReadLimits, usize) =
        decode_from_slice(&bytes, standard()).expect("deserialize a declared limit policy");
    assert_eq!(used, bytes.len());
    assert_eq!(decoded, limits);
}

#[test]
fn local_owner_identity_values_are_distinct_strong_types() {
    assert_ne!(
        TypeId::of::<DatabaseIdentity>(),
        TypeId::of::<WriterRunNumber>()
    );
    assert_ne!(
        TypeId::of::<DatabaseIdentity>(),
        TypeId::of::<LocalUserIdentity>()
    );
    assert_ne!(
        TypeId::of::<DatabaseIdentity>(),
        TypeId::of::<ChannelAddress>()
    );
    assert_ne!(
        TypeId::of::<WriterRunNumber>(),
        TypeId::of::<LocalUserIdentity>()
    );
    assert_ne!(
        TypeId::of::<WriterRunNumber>(),
        TypeId::of::<ChannelAddress>()
    );
    assert_ne!(
        TypeId::of::<LocalUserIdentity>(),
        TypeId::of::<ChannelAddress>()
    );
}

#[test]
fn breadcrumb_is_addressed_by_the_database_hash_and_uses_process_start_identity() {
    let runtime_seed = rand::random::<[u8; 32]>();
    let addresses =
        [0x00, 0x3c, 0xa5, 0xff].map(|mask| ChannelAddress(runtime_seed.map(|byte| byte ^ mask)));
    let locations = addresses.map(ReaderBreadcrumbLocation::for_database_path_hash);
    for (address, location) in addresses.into_iter().zip(locations) {
        assert_eq!(location, ReaderBreadcrumbLocation(address));
        assert_eq!(location.0, address);
    }
    for (index, left) in locations.iter().enumerate() {
        for right in locations.iter().skip(index + 1) {
            assert_ne!(
                left, right,
                "distinct database-path hashes must not collapse"
            );
        }
    }
    let first = ReaderBreadcrumb {
        process_id: 41,
        process_name: "contextdb".to_owned(),
        process_start: ProcessStartIdentity(7),
    };
    let second = ReaderBreadcrumb {
        process_id: 73,
        process_name: "contextdb-server".to_owned(),
        process_start: ProcessStartIdentity(12),
    };
    assert!(first.is_live_for(&ReaderProcessIdentity {
        process_id: 41,
        process_start: ProcessStartIdentity(7),
    }));
    assert!(second.is_live_for(&ReaderProcessIdentity {
        process_id: 73,
        process_start: ProcessStartIdentity(12),
    }));
    assert!(!first.is_live_for(&ReaderProcessIdentity {
        process_id: 99,
        process_start: ProcessStartIdentity(7),
    }));
    assert!(!first.is_live_for(&ReaderProcessIdentity {
        process_id: 41,
        process_start: ProcessStartIdentity(8),
    }));
    assert!(!second.is_live_for(&ReaderProcessIdentity {
        process_id: 41,
        process_start: ProcessStartIdentity(7),
    }));
}

#[test]
fn serving_state_and_reader_refusal_remain_deliberately_distinct() {
    let status = OwnerReadStatus {
        state: OwnerServingState::NotServing,
        reason: Some(OwnerServingReason::StartupFailure(
            "runtime dir unavailable".to_owned(),
        )),
    };
    assert_eq!(status.state, OwnerServingState::NotServing);
    assert_ne!(
        ReadFailureKind::OwnerNotServing,
        ReadFailureKind::OwnerNotRunning,
        "a writer state is not the reader's refusal kind"
    );
}

#[test]
fn owner_read_status_accepts_only_coherent_state_reason_pairs() {
    let coherent = [
        OwnerReadStatus {
            state: OwnerServingState::Serving,
            reason: None,
        },
        OwnerReadStatus {
            state: OwnerServingState::ServingDisabled,
            reason: Some(OwnerServingReason::DisabledByConfiguration),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::StartupFailure(
                "runtime directory unavailable".to_owned(),
            )),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::ShutdownDraining),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: None,
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: Some(OwnerServingReason::PlatformUnsupported),
        },
    ];
    for status in coherent {
        assert_eq!(status.validate(), Ok(()));
    }

    let contradictory = [
        OwnerReadStatus {
            state: OwnerServingState::Serving,
            reason: Some(OwnerServingReason::DisabledByConfiguration),
        },
        OwnerReadStatus {
            state: OwnerServingState::Serving,
            reason: Some(OwnerServingReason::StartupFailure("failed".to_owned())),
        },
        OwnerReadStatus {
            state: OwnerServingState::Serving,
            reason: Some(OwnerServingReason::PlatformUnsupported),
        },
        OwnerReadStatus {
            state: OwnerServingState::ServingDisabled,
            reason: None,
        },
        OwnerReadStatus {
            state: OwnerServingState::ServingDisabled,
            reason: Some(OwnerServingReason::StartupFailure("failed".to_owned())),
        },
        OwnerReadStatus {
            state: OwnerServingState::ServingDisabled,
            reason: Some(OwnerServingReason::PlatformUnsupported),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: None,
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::DisabledByConfiguration),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::PlatformUnsupported),
        },
        OwnerReadStatus {
            state: OwnerServingState::ServingDisabled,
            reason: Some(OwnerServingReason::ShutdownDraining),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: Some(OwnerServingReason::ShutdownDraining),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: Some(OwnerServingReason::DisabledByConfiguration),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: Some(OwnerServingReason::StartupFailure("failed".to_owned())),
        },
    ];
    for status in contradictory {
        assert_eq!(
            status.validate(),
            Err(ReadContractViolation::OwnerReadStatusReasonMismatch)
        );
    }
}

#[derive(Clone, Default)]
struct ManualDeadlineClock {
    inner: Arc<Mutex<ManualDeadlineState>>,
}

#[derive(Default)]
struct ManualDeadlineState {
    now_ms: u64,
    next_waiter_id: u64,
    waiters: Vec<ManualWaiterRegistration>,
}

struct ManualWaiterRegistration {
    id: u64,
    deadline_ms: u64,
    waker: Waker,
}

struct ManualDeadlineWait {
    inner: Arc<Mutex<ManualDeadlineState>>,
    id: u64,
    deadline_ms: u64,
}

impl Future for ManualDeadlineWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.inner.lock().expect("manual deadline clock lock");
        if state.now_ms >= self.deadline_ms {
            state.waiters.retain(|waiter| waiter.id != self.id);
            return Poll::Ready(());
        }

        if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == self.id) {
            waiter.waker = context.waker().clone();
        } else {
            state.waiters.push(ManualWaiterRegistration {
                id: self.id,
                deadline_ms: self.deadline_ms,
                waker: context.waker().clone(),
            });
        }
        Poll::Pending
    }
}

impl DeadlineClock for ManualDeadlineClock {
    fn now_ms(&self) -> u64 {
        self.inner
            .lock()
            .expect("manual deadline clock lock")
            .now_ms
    }

    fn wait_until(&self, deadline_ms: u64) -> DeadlineWait<'_> {
        let id = {
            let mut state = self.inner.lock().expect("manual deadline clock lock");
            let id = state.next_waiter_id;
            state.next_waiter_id += 1;
            id
        };
        Box::pin(ManualDeadlineWait {
            inner: Arc::clone(&self.inner),
            id,
            deadline_ms,
        })
    }
}

impl ManualDeadlineClock {
    fn advance(&self, now_ms: u64) {
        let ready = {
            let mut state = self.inner.lock().expect("manual deadline clock lock");
            assert!(now_ms >= state.now_ms, "manual time must not move backward");
            state.now_ms = now_ms;

            let mut ready = Vec::new();
            let mut pending = Vec::new();
            for waiter in std::mem::take(&mut state.waiters) {
                if waiter.deadline_ms <= now_ms {
                    ready.push(waiter.waker);
                } else {
                    pending.push(waiter);
                }
            }
            state.waiters = pending;
            ready
        };
        for waker in ready {
            waker.wake();
        }
    }

    fn registered_waiters(&self) -> usize {
        self.inner
            .lock()
            .expect("manual deadline clock lock")
            .waiters
            .len()
    }
}

#[derive(Default)]
struct WakeCount {
    wakes: AtomicUsize,
}

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_once(future: &mut DeadlineWait<'_>, waker: &Waker) -> Poll<()> {
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}

struct EchoHandler;

impl OwnerRequestHandler for EchoHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancel: &OwnerReadCancellation,
    ) -> contextdb_core::Result<Vec<u8>> {
        if request == b"fail" {
            Err(Error::Other("custom owner handler failed".to_owned()))
        } else {
            Ok(request.to_vec())
        }
    }
}

fn accepts_cross_thread_deadline_clock<T: DeadlineClock + Send + Sync + 'static>(_: T) {}

#[test]
fn routes_deadline_clock_and_owner_handler_have_the_type_contract() {
    accepts_cross_thread_deadline_clock(ManualDeadlineClock::default());
    let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::default());
    assert_eq!(clock.now_ms(), 0);
    drop(clock.wait_until(0));
    assert_ne!(
        TypeId::of::<ManualDeadlineClock>(),
        TypeId::of::<contextdb_core::Wallclock>(),
        "deadline waiting must not reuse the persisted-data wall clock"
    );
    assert_ne!(ReadRoute::File, ReadRoute::Owner);

    let handler: Arc<dyn OwnerRequestHandler> = Arc::new(EchoHandler);
    let cancellation = OwnerReadCancellation::new();
    assert_eq!(
        handler
            .handle("diagnostics", b"request", &cancellation)
            .expect("custom handler success"),
        b"request".to_vec()
    );
    assert!(matches!(
        handler.handle("diagnostics", b"fail", &cancellation),
        Err(Error::Other(message)) if message == "custom owner handler failed"
    ));
}

#[test]
fn manual_deadline_clock_wakes_cross_thread_waiters_at_their_deadlines() {
    let clock = ManualDeadlineClock::default();
    let first_wake = Arc::new(WakeCount::default());
    let second_wake = Arc::new(WakeCount::default());
    let first_waker = Waker::from(Arc::clone(&first_wake));
    let second_waker = Waker::from(Arc::clone(&second_wake));
    let mut first = clock.wait_until(5);
    let mut second = clock.wait_until(9);

    assert_eq!(poll_once(&mut first, &first_waker), Poll::Pending);
    assert_eq!(poll_once(&mut second, &second_waker), Poll::Pending);
    assert_eq!(clock.registered_waiters(), 2);

    let advancing_clock = clock.clone();
    std::thread::spawn(move || advancing_clock.advance(5))
        .join()
        .expect("cross-thread manual-clock advance");
    assert_eq!(clock.now_ms(), 5);
    assert_eq!(first_wake.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(second_wake.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(poll_once(&mut first, &first_waker), Poll::Ready(()));
    assert_eq!(poll_once(&mut second, &second_waker), Poll::Pending);

    let advancing_clock = clock.clone();
    std::thread::spawn(move || advancing_clock.advance(9))
        .join()
        .expect("cross-thread manual-clock advance");
    assert_eq!(second_wake.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(poll_once(&mut second, &second_waker), Poll::Ready(()));
    assert_eq!(clock.registered_waiters(), 0);

    let already_reached_wake = Arc::new(WakeCount::default());
    let already_reached_waker = Waker::from(Arc::clone(&already_reached_wake));
    let mut already_reached = clock.wait_until(8);
    assert_eq!(
        poll_once(&mut already_reached, &already_reached_waker),
        Poll::Ready(())
    );
    assert_eq!(already_reached_wake.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(clock.registered_waiters(), 0);
}

#[test]
fn cancellation_is_shared_across_clones_isolated_across_tokens_and_idempotent() {
    let cancellation = OwnerReadCancellation::new();
    let clone = cancellation.clone();
    let independent = OwnerReadCancellation::new();
    assert!(!cancellation.is_cancelled());
    assert!(!clone.is_cancelled());
    assert!(!independent.is_cancelled());

    clone.cancel();
    assert!(cancellation.is_cancelled());
    assert!(clone.is_cancelled());
    assert!(!independent.is_cancelled());

    clone.cancel();
    assert!(cancellation.is_cancelled());
    independent.cancel();
    assert!(independent.is_cancelled());
}

fn assert_read_failure_round_trip(failure: ReadFailure, expected_class: ReadFailureClass) {
    let bytes = encode_to_vec(&failure, standard()).expect("encode stable read failure");
    let (decoded, used): (ReadFailure, usize) =
        decode_from_slice(&bytes, standard()).expect("decode stable read failure");
    assert_eq!(used, bytes.len());
    assert_eq!(decoded, failure);
    assert_eq!(decoded.class(), expected_class);
}

#[test]
fn stable_read_failure_vocabulary_round_trips_every_legal_shape() {
    let held_by_readers_details = [
        HeldByReadersDetail {
            observed_direct_readers: 3,
            verified_readers: vec![
                ReaderBreadcrumb {
                    process_id: 41,
                    process_name: "contextdb".to_owned(),
                    process_start: ProcessStartIdentity(7),
                },
                ReaderBreadcrumb {
                    process_id: 73,
                    process_name: "contextdb-server".to_owned(),
                    process_start: ProcessStartIdentity(12),
                },
            ],
        },
        HeldByReadersDetail {
            observed_direct_readers: 2,
            verified_readers: vec![],
        },
    ];
    let owner_limit_details = [
        OwnerLimitExceededDetail {
            limit: ReadFailureLimit::ResultRows,
            value: 500,
            required: None,
            statement: Some(StatementRemedy {
                statement: "SELECT * FROM observations".to_owned(),
                remedy_command: ".cursor open SELECT * FROM observations".to_owned(),
            }),
        },
        OwnerLimitExceededDetail {
            limit: ReadFailureLimit::ResultBytes,
            value: 4 * 1024 * 1024,
            required: Some(RequiredBytesSetting {
                required_bytes: 4 * 1024 * 1024 + 17,
                required_setting: "effective result_bytes >= 4194321".to_owned(),
            }),
            statement: None,
        },
        OwnerLimitExceededDetail {
            limit: ReadFailureLimit::ResultBytes,
            value: 4 * 1024 * 1024,
            required: None,
            statement: Some(StatementRemedy {
                statement: "SELECT payload FROM events".to_owned(),
                remedy_command: "raise the writer's --owner-read-result-bytes or use .cursor open SELECT payload FROM events".to_owned(),
            }),
        },
    ];
    let ordinary_detail_shapes = [
        ReadFailureDetail::None,
        ReadFailureDetail::Reason {
            reason: "runtime directory unavailable".to_owned(),
        },
    ];

    for kind in ReadFailureKind::ALL {
        if matches!(
            kind,
            ReadFailureKind::HeldByReaders
                | ReadFailureKind::OwnerLimitExceeded
                | ReadFailureKind::CursorExpired
                | ReadFailureKind::OwnerRouteUnsupported
        ) {
            continue;
        }
        for detail in &ordinary_detail_shapes {
            let failure = ReadFailure::new(kind, detail.clone())
                .expect("ordinary read failures accept every ordinary detail shape");
            assert_eq!(failure.kind(), kind);
            assert_read_failure_round_trip(failure, kind.class());
        }
    }

    for detail in &held_by_readers_details {
        assert_eq!(detail.validate(), Ok(()));
        let held_from_new = ReadFailure::new(
            ReadFailureKind::HeldByReaders,
            ReadFailureDetail::HeldByReaders(detail.clone()),
        )
        .expect("held-by-readers detail matches its refusal kind");
        let held_from_constructor = ReadFailure::held_by_readers(detail.clone())
            .expect("held-by-readers constructor validates its detail");
        assert_eq!(held_from_new, held_from_constructor);
        assert_read_failure_round_trip(held_from_new, ReadFailureClass::Io);
    }

    let held_by_writer = ReadFailure::new(
        ReadFailureKind::HeldByWriter,
        ReadFailureDetail::Reason {
            reason: "writer owns the store".to_owned(),
        },
    )
    .expect("held-by-writer keeps the ordinary failure detail vocabulary");
    assert_read_failure_round_trip(held_by_writer, ReadFailureClass::Io);

    let owner_route_unsupported = ReadFailure::new(
        ReadFailureKind::OwnerRouteUnsupported,
        ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
            inspection: "image_state".to_owned(),
        }),
    )
    .expect("the unsupported-inspection detail matches its refusal kind");
    assert_read_failure_round_trip(owner_route_unsupported, ReadFailureClass::Io);

    for detail in &owner_limit_details {
        let limit_from_new = ReadFailure::new(
            ReadFailureKind::OwnerLimitExceeded,
            ReadFailureDetail::OwnerLimitExceeded(detail.clone()),
        )
        .expect("owner-limit detail matches the owner-limit kind");
        let limit_from_constructor = ReadFailure::owner_limit_exceeded(detail.clone());
        assert_eq!(limit_from_new, limit_from_constructor);
        assert_read_failure_round_trip(limit_from_new, ReadFailureClass::Io);
    }

    for expiry in [CursorExpiryKind::Idle, CursorExpiryKind::Lifetime] {
        let expired_from_new = ReadFailure::new(
            ReadFailureKind::CursorExpired,
            ReadFailureDetail::CursorExpired { expiry },
        )
        .expect("cursor-expiry detail matches the cursor-expired kind");
        let expired_from_constructor = ReadFailure::cursor_expired(expiry);
        assert_eq!(expired_from_new, expired_from_constructor);
        assert_read_failure_round_trip(expired_from_new, ReadFailureClass::Io);
    }

    let contradictory = [
        (ReadFailureKind::OwnerLimitExceeded, ReadFailureDetail::None),
        (
            ReadFailureKind::OwnerLimitExceeded,
            ReadFailureDetail::CursorExpired {
                expiry: CursorExpiryKind::Idle,
            },
        ),
        (
            ReadFailureKind::CursorExpired,
            ReadFailureDetail::Reason {
                reason: "expired".to_owned(),
            },
        ),
        (
            ReadFailureKind::CursorExpired,
            ReadFailureDetail::OwnerLimitExceeded(owner_limit_details[0].clone()),
        ),
        (
            ReadFailureKind::OwnerTimeout,
            ReadFailureDetail::OwnerLimitExceeded(owner_limit_details[0].clone()),
        ),
        (
            ReadFailureKind::CursorNotFound,
            ReadFailureDetail::CursorExpired {
                expiry: CursorExpiryKind::Lifetime,
            },
        ),
        (ReadFailureKind::HeldByReaders, ReadFailureDetail::None),
        (
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByReaders(held_by_readers_details[0].clone()),
        ),
        (
            ReadFailureKind::OwnerNotServing,
            ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
                inspection: "image_state".to_owned(),
            }),
        ),
        // A route that refuses an inspection always knows which one it was
        // asked for -- there is no case, as there is for a writer whose
        // process identity was never published, where the answer is honestly
        // unknown. So this kind owes its named detail in both directions: a
        // refusal that arrives without one is not a thinner answer, it is the
        // bare "not implemented" this kind exists to replace.
        (
            ReadFailureKind::OwnerRouteUnsupported,
            ReadFailureDetail::None,
        ),
        (
            ReadFailureKind::OwnerRouteUnsupported,
            ReadFailureDetail::Reason {
                reason: "not implemented".to_owned(),
            },
        ),
    ];
    for (kind, detail) in contradictory {
        assert_eq!(
            ReadFailure::new(kind, detail.clone()),
            Err(ReadFailureConstructionError::KindDetailMismatch)
        );
        let bytes = encode_to_vec((kind, detail), standard())
            .expect("encode a contradictory failure envelope");
        let decoded: Result<(ReadFailure, usize), _> = decode_from_slice(&bytes, standard());
        assert!(decoded.is_err(), "deserialization must reject {kind:?}");
    }

    let invalid_held_by_readers_details = [
        (
            HeldByReadersDetail {
                observed_direct_readers: 0,
                verified_readers: vec![],
            },
            ReadContractViolation::HeldByReadersObservedCountZero,
        ),
        (
            HeldByReadersDetail {
                observed_direct_readers: 1,
                verified_readers: held_by_readers_details[0].verified_readers.clone(),
            },
            ReadContractViolation::VerifiedReadersExceedObserved,
        ),
        (
            HeldByReadersDetail {
                observed_direct_readers: 2,
                verified_readers: vec![
                    held_by_readers_details[0].verified_readers[0].clone(),
                    ReaderBreadcrumb {
                        process_name: "renamed-reader-process".to_owned(),
                        ..held_by_readers_details[0].verified_readers[0].clone()
                    },
                ],
            },
            ReadContractViolation::DuplicateVerifiedReader { reader: 1 },
        ),
    ];
    for (detail, violation) in invalid_held_by_readers_details {
        assert_eq!(detail.validate(), Err(violation.clone()));
        assert_eq!(
            ReadFailure::held_by_readers(detail.clone()),
            Err(ReadFailureConstructionError::InvalidHeldByReadersDetail(
                violation
            ))
        );
        let bytes = encode_to_vec(
            (
                ReadFailureKind::HeldByReaders,
                ReadFailureDetail::HeldByReaders(detail),
            ),
            standard(),
        )
        .expect("encode malformed held-by-readers envelope");
        let decoded: Result<(ReadFailure, usize), _> = decode_from_slice(&bytes, standard());
        assert!(
            decoded.is_err(),
            "deserialization must reject malformed held-by-readers detail"
        );
    }

    assert!(matches!(
        ReadFailure::owner_limit_exceeded(owner_limit_details[0].clone()).detail(),
        ReadFailureDetail::OwnerLimitExceeded(OwnerLimitExceededDetail {
            limit: ReadFailureLimit::ResultRows,
            value: 500,
            required: None,
            statement: Some(StatementRemedy { statement, remedy_command }),
        }) if statement == "SELECT * FROM observations"
            && remedy_command == ".cursor open SELECT * FROM observations"
    ));
    assert!(matches!(
        ReadFailure::owner_limit_exceeded(owner_limit_details[1].clone()).detail(),
        ReadFailureDetail::OwnerLimitExceeded(OwnerLimitExceededDetail {
            limit: ReadFailureLimit::ResultBytes,
            value: 4_194_304,
            required: Some(RequiredBytesSetting { required_bytes: 4_194_321, required_setting }),
            statement: None,
        }) if required_setting == "effective result_bytes >= 4194321"
    ));
    assert!(matches!(
        ReadFailure::owner_limit_exceeded(owner_limit_details[2].clone()).detail(),
        ReadFailureDetail::OwnerLimitExceeded(OwnerLimitExceededDetail {
            statement: Some(StatementRemedy { statement, remedy_command }),
            ..
        }) if statement == "SELECT payload FROM events"
            && remedy_command.contains("--owner-read-result-bytes")
    ));
    assert_eq!(
        ReadFailureKind::ALL,
        [
            ReadFailureKind::WriteRequiresFlag,
            ReadFailureKind::HeldByWriter,
            ReadFailureKind::HeldByReaders,
            ReadFailureKind::OwnerNotRunning,
            ReadFailureKind::OwnerNotServing,
            ReadFailureKind::OwnerUserMismatch,
            ReadFailureKind::OwnerMismatch,
            ReadFailureKind::OwnerAtCapacity,
            ReadFailureKind::OwnerLimitExceeded,
            ReadFailureKind::OwnerTimeout,
            ReadFailureKind::OwnerDisconnected,
            ReadFailureKind::InvalidChannelData,
            ReadFailureKind::LocalProtocolMismatch,
            ReadFailureKind::CursorExpired,
            ReadFailureKind::CursorNotFound,
            ReadFailureKind::DirectReadRequiresWriter,
            ReadFailureKind::StoreNotFound,
            ReadFailureKind::InvalidContinuation,
            ReadFailureKind::CursorAlreadyOpen,
            ReadFailureKind::CursorTransactionActive,
            ReadFailureKind::CursorInvalidStatement,
            ReadFailureKind::OperationAlreadyCompleted,
            ReadFailureKind::OwnerRouteUnsupported,
            ReadFailureKind::DeclaredPrincipalRefused,
        ]
    );
    for kind in ReadFailureKind::ALL {
        let expected_class = match kind {
            ReadFailureKind::WriteRequiresFlag
            | ReadFailureKind::CursorAlreadyOpen
            | ReadFailureKind::CursorTransactionActive => ReadFailureClass::Sql,
            ReadFailureKind::InvalidContinuation | ReadFailureKind::CursorInvalidStatement => {
                ReadFailureClass::Usage
            }
            _ => ReadFailureClass::Io,
        };
        assert_eq!(kind.class(), expected_class);
    }
    assert_eq!(
        ReadFailureDetail::CursorExpired {
            expiry: CursorExpiryKind::Lifetime,
        },
        ReadFailureDetail::CursorExpired {
            expiry: CursorExpiryKind::Lifetime,
        }
    );
    assert_ne!(CursorExpiryKind::Idle, CursorExpiryKind::Lifetime);
}

#[test]
fn shutdown_drain_status_and_typed_error_remain_matchable() {
    let status = OwnerReadStatus {
        state: OwnerServingState::NotServing,
        reason: Some(OwnerServingReason::ShutdownDraining),
    };
    assert_eq!(status.validate(), Ok(()));
    let bytes = encode_to_vec(&status, standard()).expect("encode shutdown-draining status");
    let (decoded, used): (OwnerReadStatus, usize) =
        decode_from_slice(&bytes, standard()).expect("decode shutdown-draining status");
    assert_eq!(used, bytes.len());
    assert_eq!(decoded, status);

    let error = Error::owner_read_drain_timeout();
    assert!(matches!(error, Error::OwnerReadDrainTimeout));
}

#[test]
fn read_cancellation_has_a_typed_core_error() {
    assert!(matches!(Error::ReadCancelled, Error::ReadCancelled));
}

#[test]
fn core_error_carries_read_failure_without_losing_its_structured_kind() {
    let failure = ReadFailure::held_by_readers(HeldByReadersDetail {
        observed_direct_readers: 1,
        verified_readers: vec![],
    })
    .expect("valid generic held-by-readers fallback");
    let error: Error = failure.clone().into();
    assert!(matches!(
        error,
        Error::ReadFailure(carried)
            if carried.kind() == ReadFailureKind::HeldByReaders && carried == failure
    ));
}

#[test]
fn cursor_page_validation_requires_only_column_row_coherence() {
    let empty = CursorPage {
        columns: vec!["id".to_owned()],
        rows: vec![],
        has_more: false,
    };
    assert_eq!(empty.validate(), Ok(()));

    let populated = CursorPage {
        columns: vec!["id".to_owned(), "payload".to_owned()],
        rows: vec![
            vec![
                contextdb_core::Value::Int64(1),
                contextdb_core::Value::Text("first".to_owned()),
            ],
            vec![contextdb_core::Value::Int64(2), contextdb_core::Value::Null],
        ],
        has_more: true,
    };
    assert_eq!(populated.validate(), Ok(()));

    let wrong_arity = CursorPage {
        rows: vec![
            populated.rows[0].clone(),
            vec![contextdb_core::Value::Int64(2)],
        ],
        ..populated.clone()
    };
    assert_eq!(
        wrong_arity.validate(),
        Err(ReadContractViolation::CursorRowArityMismatch {
            row: 1,
            expected: 2,
            actual: 1,
        })
    );
}

#[test]
fn metadata_page_validation_enforces_vocabulary_and_continuation_shape() {
    let tables = MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![MetadataItem::Table("observations".to_owned())],
        has_more: true,
        continuation: Some("next".to_owned()),
    };
    assert_eq!(tables.validate(), Ok(()));

    let mut event = BTreeMap::new();
    event.insert(
        "state".to_owned(),
        contextdb_core::Value::Text("queued".to_owned()),
    );
    let events_status = MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![MetadataItem::EventStatus(event.clone())],
        has_more: false,
        continuation: None,
    };
    assert_eq!(events_status.validate(), Ok(()));

    let table_item_in_events_page = MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![MetadataItem::Table("observations".to_owned())],
        has_more: false,
        continuation: None,
    };
    assert_eq!(
        table_item_in_events_page.validate(),
        Err(ReadContractViolation::MetadataItemVocabularyMismatch { item: 0 })
    );

    let event_item_in_tables_page = MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![MetadataItem::EventStatus(event)],
        has_more: false,
        continuation: None,
    };
    assert_eq!(
        event_item_in_tables_page.validate(),
        Err(ReadContractViolation::MetadataItemVocabularyMismatch { item: 0 })
    );

    let malformed_metadata = MetadataPage {
        continuation: None,
        ..tables.clone()
    };
    assert_eq!(
        malformed_metadata.validate(),
        Err(ReadContractViolation::MissingMetadataContinuation)
    );

    let unexpected_continuation = MetadataPage {
        has_more: false,
        continuation: Some("not allowed".to_owned()),
        ..tables
    };
    assert_eq!(
        unexpected_continuation.validate(),
        Err(ReadContractViolation::UnexpectedMetadataContinuation)
    );
}

#[test]
fn metadata_page_validation_handles_empty_pages_for_every_vocabulary() {
    for vocabulary in [
        MetadataPageVocabulary::Tables,
        MetadataPageVocabulary::EventsStatus,
    ] {
        let non_progressing = MetadataPage {
            vocabulary,
            items: vec![],
            has_more: true,
            continuation: Some("next".to_owned()),
        };
        assert_eq!(
            non_progressing.validate(),
            Err(ReadContractViolation::EmptyMetadataPageWithMore)
        );

        let exhausted = MetadataPage {
            has_more: false,
            continuation: None,
            ..non_progressing
        };
        assert_eq!(exhausted.validate(), Ok(()));
    }
}
