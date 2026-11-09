use super::*;
#[test]
fn each_sample_retains_its_actual_count_and_the_depth_keeps_the_whole_distribution() {
    let counts = [3, 7, 5];
    let mut summary = RerankSummary::first(counts[0]);
    for count in &counts[1..] {
        summary.include(*count).unwrap();
    }
    assert_eq!(
        (
            summary.minimum,
            summary.maximum,
            summary.total,
            summary.samples
        ),
        (3, 7, 15, 3)
    );
    assert_eq!(summary.mean(), 5.0);
    let scope = [9; 64];
    for (sample, count) in counts.into_iter().enumerate() {
        let stat = SampleWork {
            depth_ordinal: 2,
            sample: sample as u32,
            reranked_candidates: count,
            indexed_work: count + 10,
            exact_work: 100,
        };
        let key = sample_work_key(stat.depth_ordinal, stat.sample);
        let mut bytes = [0; RECORD_BYTES];
        encode(&scope, 7, &key, Record::SampleWork(stat), &mut bytes).unwrap();
        assert_eq!(
            decode(&scope, 7, &key, &bytes).unwrap(),
            Record::SampleWork(stat)
        );
        assert!(decode(&scope, 8, &key, &bytes).is_err());
        assert!(decode(&scope, 7, &sample_work_key(2, stat.sample + 1), &bytes).is_err());
    }
    let depth = Depth {
        depth: 10,
        probes: 1,
        factor_hundredths: 400,
        shortlist: 40,
        reranks: Some(summary),
        samples: 3,
        queries_below: 0,
        qualified: true,
        indexed_work: 45,
        exact_work: 300,
    };
    let mut bytes = [0; RECORD_BYTES];
    let key = key(1, 2);
    encode(&scope, 7, &key, Record::Depth(depth), &mut bytes).unwrap();
    assert_eq!(
        decode(&scope, 7, &key, &bytes).unwrap(),
        Record::Depth(depth)
    );
    assert_ne!(depth.shortlist, summary.maximum);
}
#[test]
fn distribution_addition_refuses_overflow_without_changing_its_prior_facts() {
    let mut summary = RerankSummary::first(u64::MAX);
    let before = summary;
    assert!(summary.include(1).is_err());
    assert_eq!(summary, before);
}

#[test]
fn captured_sample_and_bounded_answer_chunks_fit_the_same_authenticated_record() {
    let scope = [31; 64];
    let records = [
        Record::Header(Header {
            epoch: u64::MAX,
            complete: false,
            source_digest: [9; 32],
            next_chunk: u64::MAX,
            generation: 4,
            policy_revision: u64::MAX,
            target_hundredths: 10_000,
            query_source: true,
            source_partitioned: false,
            source_identity: u64::MAX,
            source_frontier: u64::MAX,
            samples: 200,
            planned_samples: 200,
            selection_samples: 100,
            validation_samples: 100,
            depths: 7,
        }),
        Record::Sample(Sample {
            row: u64::MAX,
            generation: 4,
            slot: u64::MAX,
            row_created_tx: u64::MAX,
            row_lsn: u64::MAX,
            vector_created_tx: u64::MAX,
            vector_lsn: u64::MAX,
            visible_from: u64::MAX,
            rank: u64::MAX,
        }),
        Record::Rows(RowChunk {
            count: 16,
            rows: [u64::MAX; 16],
        }),
        Record::Rewrite(Rewrite {
            depth_ordinal: u32::MAX,
            sample: u32::MAX,
            first_chunk: u64::MAX,
            answer_length: u32::MAX,
        }),
    ];
    for (ordinal, record) in records.into_iter().enumerate() {
        let key = key(3, ordinal as u64);
        let mut bytes = [0; RECORD_BYTES];
        encode(&scope, 4, &key, record, &mut bytes).unwrap();
        assert_eq!(decode(&scope, 4, &key, &bytes).unwrap(), record);
    }
}
