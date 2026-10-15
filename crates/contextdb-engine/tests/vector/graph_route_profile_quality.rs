#![cfg(feature = "nightly-tests")]

include!("graph_route_profile_support.rs");

#[test]
fn f32_breadth_below_5001_is_measured_from_frozen_queries() {
    let mut selections = Vec::new();
    for n in [625_usize, 2_500, 5_000, 5_001] {
        let db = fixture(n, "topic_clusters", 17);
        let baselines =
            Scope::ALL.map(|scope| exact_baseline(&db, "topic_clusters", 17, scope, None));
        let mut selected = None;
        for breadth in BREADTH_CANDIDATES {
            db.execute(
                &format!(
                    "ALTER TABLE profile_items ALTER COLUMN embedding SET HNSW (EF_SEARCH = {breadth})"
                ),
                &HashMap::new(),
            )
            .expect("apply one measured search breadth without rebuilding");
            let mut passes_all_scopes = true;
            for (scope, exact) in Scope::ALL.into_iter().zip(&baselines) {
                let (matched, work, p95_micros, observed_ef) =
                    indexed_cell(&db, "topic_clusters", 17, scope, None, exact);
                assert_eq!(observed_ef, breadth);
                let passes = quality_passes(matched, work, exact.work);
                passes_all_scopes &= passes;
                println!(
                    "{}",
                    json!({
                        "kind": "breadth_cell",
                        "n": n,
                        "scope": scope.as_str(),
                        "contexts": scope.count(),
                        "breadth": breadth,
                        "queries": PROBES,
                        "matched_at_10": matched,
                        "possible_at_10": PROBES * K,
                        "recall": matched as f64 / (PROBES * K) as f64,
                        "indexed_work": work,
                        "exact_work": exact.work,
                        "indexed_p95_micros": p95_micros,
                        "exact_p95_micros": exact.p95_micros,
                        "passes": passes,
                    })
                );
            }
            if passes_all_scopes && selected.is_none() {
                selected = Some(breadth);
            }
        }
        let selected = selected.unwrap_or_else(|| panic!("no measured breadth passed at n={n}"));
        db.execute(
            "ALTER TABLE profile_items ALTER COLUMN embedding SET HNSW (EF_SEARCH = DEFAULT)",
            &HashMap::new(),
        )
        .expect("restore the silent compatibility profile");
        let effective_default =
            indexed_cell(&db, "topic_clusters", 17, Scope::One, None, &baselines[0]).3;
        println!(
            "{}",
            json!({
                "kind": "breadth_selection",
                "n": n,
                "selected_breadth": selected,
                "effective_default_at_k": effective_default,
                "k": K,
            })
        );
        selections.push((n, selected, effective_default));
    }
    for (n, selected, effective_default) in selections {
        let profile = if n < 5_001 { selected } else { 400 };
        let expected = profile.max(K * 10);
        assert_eq!(
            effective_default, expected,
            "the silent F32 profile at n={n} must combine the measured profile with the preserved 10*k query floor"
        );
    }
}
