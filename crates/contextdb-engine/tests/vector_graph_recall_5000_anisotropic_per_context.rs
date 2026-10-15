#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[allow(dead_code, unused_imports)] // Each target uses a subset of the canonical helpers.
mod graph_route_profile_quality {
    #![cfg(feature = "nightly-tests")]

    include!("vector/graph_route_profile_support.rs");

    #[test]
    #[ignore = "CDB-160: index-acceleration bar owned by the bounded-storage-rc2 campaign (S5 search-route + S10 never-built graph 1c85c7c6); re-enabled when rc2 lands on dev."]
    fn selected_f32_build_switches_pass_5000_anisotropic_per_context() {
        run_build_quality_profile(0);
    }
}
#[path = "fixtures/maintained_search/mod.rs"]
mod maintained_search;
