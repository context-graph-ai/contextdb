//! One semantic boundary for clustered search declarations.
use contextdb_core::{
    ColumnDef, ColumnType, Error, Result, VectorIndexRef, VectorPolicyDeclarationIssue,
};
use contextdb_parser::ast::{self, VectorPolicyValue as V};
use std::fmt::{self, Write};

fn invalid(table: &str, column: &str, member: &'static str) -> Error {
    Error::InvalidVectorPolicyDeclaration {
        index: VectorIndexRef::new(table, column),
        issue: VectorPolicyDeclarationIssue::InvalidMember { member },
    }
}
pub(crate) fn integer(
    table: &str,
    column: &str,
    member: &'static str,
    raw: &str,
    zero: bool,
) -> Result<u64> {
    if !raw.bytes().all(|b| b.is_ascii_digit()) || raw.is_empty() {
        return Err(invalid(table, column, member));
    }
    raw.parse::<u64>()
        .ok()
        .filter(|v| zero || *v > 0)
        .ok_or_else(|| invalid(table, column, member))
}
pub(crate) fn decimal(table: &str, column: &str, member: &'static str, raw: &str) -> Result<u32> {
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 2
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid(table, column, member));
    }
    let whole = whole
        .parse::<u32>()
        .map_err(|_| invalid(table, column, member))?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<u32>()
            .map_err(|_| invalid(table, column, member))?
            * if fraction.len() == 1 { 10 } else { 1 }
    };
    whole
        .checked_mul(100)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| invalid(table, column, member))
}
fn unsigned(
    table: &str,
    column: &str,
    member: &'static str,
    value: &V,
    zero: bool,
) -> Result<Option<u64>> {
    match value {
        V::Default => Ok(None),
        V::Value(raw) => integer(table, column, member, raw, zero).map(Some),
    }
}
fn hundredths(
    table: &str,
    column: &str,
    member: &'static str,
    value: &V,
    min: u32,
    max: u32,
) -> Result<Option<u32>> {
    match value {
        V::Default => Ok(None),
        V::Value(raw) => decimal(table, column, member, raw).and_then(|v| {
            if v >= min && v <= max {
                Ok(Some(v))
            } else {
                Err(invalid(table, column, member))
            }
        }),
    }
}
pub(crate) fn apply_search(
    table: &str,
    col: &mut ColumnDef,
    search: Option<&ast::VectorSearchObjectives>,
) -> Result<()> {
    if let Some(s) = search {
        if let Some(v) = &s.target_accuracy {
            col.search_target_accuracy_hundredths =
                hundredths(table, &col.name, "TARGET_ACCURACY", v, 1, 10000)?.map(|v| v as u16);
        }
        if let Some(v) = &s.expected_vectors {
            col.search_expected_vectors = unsigned(table, &col.name, "EXPECTED_VECTORS", v, false)?;
        }
    } else {
        col.search_target_accuracy_hundredths = None;
        col.search_expected_vectors = None;
    }
    validate(table, col)
}
pub(crate) fn apply_ivf(
    table: &str,
    col: &mut ColumnDef,
    ivf: Option<&ast::VectorIvfOptions>,
) -> Result<()> {
    if let Some(s) = ivf {
        if let Some(v) = &s.lists {
            col.ivf_lists = unsigned(table, &col.name, "LISTS", v, false)?
                .map(|v| u32::try_from(v).map_err(|_| invalid(table, &col.name, "LISTS")))
                .transpose()?;
        }
        if let Some(v) = &s.probes {
            col.ivf_probes = unsigned(table, &col.name, "PROBES", v, false)?
                .map(|v| u32::try_from(v).map_err(|_| invalid(table, &col.name, "PROBES")))
                .transpose()?;
        }
        if let Some(v) = &s.refine_factor {
            col.ivf_refine_factor_hundredths =
                hundredths(table, &col.name, "REFINE_FACTOR", v, 100, u32::MAX)?;
        }
    } else {
        col.ivf_lists = None;
        col.ivf_probes = None;
        col.ivf_refine_factor_hundredths = None;
    }
    validate(table, col)
}
pub(crate) fn apply_with(
    table: &str,
    col: &mut ColumnDef,
    policy: &ast::VectorEnginePolicyOptions,
) -> Result<()> {
    if let Some(name) = policy.unknown_members.first() {
        return Err(Error::InvalidVectorPolicyDeclaration {
            index: VectorIndexRef::new(table, &col.name),
            issue: VectorPolicyDeclarationIssue::UnknownMember {
                member: name.clone(),
            },
        });
    }
    if let Some(q) = policy.quantization {
        col.quantization = match q.unwrap_or_default() {
            ast::VectorQuantization::F32 => contextdb_core::VectorQuantization::F32,
            ast::VectorQuantization::SQ8 => contextdb_core::VectorQuantization::SQ8,
            ast::VectorQuantization::SQ4 => contextdb_core::VectorQuantization::SQ4,
        };
    }
    if let Some(value) = &policy.calibration_max_k {
        col.vector_engine_policy.calibration_max_k =
            unsigned(table, &col.name, "CALIBRATION_MAX_K", value, false)?;
    }
    if let Some(value) = &policy.calibration_depths {
        col.vector_engine_policy.calibration_depths = match value {
            ast::VectorPolicyListValue::Default => None,
            ast::VectorPolicyListValue::Values(values) => Some(
                values
                    .iter()
                    .map(|raw| {
                        integer(table, &col.name, "CALIBRATION_DEPTHS", raw, false).and_then(|v| {
                            u32::try_from(v)
                                .map_err(|_| invalid(table, &col.name, "CALIBRATION_DEPTHS"))
                        })
                    })
                    .collect::<Result<_>>()?,
            ),
        };
    }
    if let Some(value) = &policy.probe_grid {
        col.vector_engine_policy.probe_grid =
            unsigned(table, &col.name, "PROBE_GRID", value, false)?;
    }
    if let Some(value) = &policy.refine_factors {
        col.vector_engine_policy.refine_factors = match value {
            ast::VectorPolicyListValue::Default => None,
            ast::VectorPolicyListValue::Values(values) => Some(
                values
                    .iter()
                    .map(|raw| {
                        decimal(table, &col.name, "REFINE_FACTORS", raw).and_then(|v| {
                            if v >= 100 {
                                Ok(v)
                            } else {
                                Err(invalid(table, &col.name, "REFINE_FACTORS"))
                            }
                        })
                    })
                    .collect::<Result<_>>()?,
            ),
        };
    }
    if let Some(value) = &policy.refine_growth {
        col.vector_engine_policy.refine_growth =
            hundredths(table, &col.name, "REFINE_GROWTH", value, 101, u32::MAX)?;
    }
    if let Some(value) = &policy.calibration_samples {
        col.vector_engine_policy.calibration_samples =
            unsigned(table, &col.name, "CALIBRATION_SAMPLES", value, false)?;
    }
    if let Some(value) = &policy.calibration_queries {
        col.vector_engine_policy.calibration_queries = match value {
            ast::VectorPolicyReferenceValue::Default => None,
            ast::VectorPolicyReferenceValue::Reference { table, column } => {
                Some(contextdb_core::VectorCalibrationSource {
                    table: table.clone(),
                    column: column.clone(),
                })
            }
        };
    }
    if let Some(value) = &policy.min_lists {
        col.vector_engine_policy.min_lists = unsigned(table, &col.name, "MIN_LISTS", value, false)?;
    }
    if let Some(value) = &policy.vectors_per_list {
        col.vector_engine_policy.vectors_per_list =
            unsigned(table, &col.name, "VECTORS_PER_LIST", value, false)?;
    }
    if let Some(value) = &policy.centroid_memory_percent {
        col.vector_engine_policy.centroid_memory_percent =
            hundredths(table, &col.name, "CENTROID_MEMORY_PERCENT", value, 1, 10000)?;
    }
    if let Some(value) = &policy.training_samples {
        col.vector_engine_policy.training_samples =
            unsigned(table, &col.name, "TRAINING_SAMPLES", value, false)?;
    }
    if let Some(value) = &policy.training_iterations {
        col.vector_engine_policy.training_iterations =
            unsigned(table, &col.name, "TRAINING_ITERATIONS", value, false)?;
    }
    if let Some(value) = &policy.training_seed {
        col.vector_engine_policy.training_seed =
            unsigned(table, &col.name, "TRAINING_SEED", value, true)?;
    }
    if let Some(value) = &policy.max_delta_runs {
        col.vector_engine_policy.max_delta_runs =
            unsigned(table, &col.name, "MAX_DELTA_RUNS", value, false)?;
    }
    if let Some(value) = &policy.write_buffer_bytes {
        col.vector_engine_policy.write_buffer_bytes =
            unsigned(table, &col.name, "WRITE_BUFFER_BYTES", value, false)?;
    }
    if let Some(value) = &policy.ingest_batch_keys {
        col.vector_engine_policy.ingest_batch_keys =
            unsigned(table, &col.name, "INGEST_BATCH_KEYS", value, false)?;
    }
    if let Some(value) = &policy.coalesce_pages_per_step {
        col.vector_engine_policy.coalesce_pages_per_step =
            unsigned(table, &col.name, "COALESCE_PAGES_PER_STEP", value, false)?;
    }
    if let Some(value) = &policy.build_batch_entries {
        col.vector_engine_policy.build_batch_entries =
            unsigned(table, &col.name, "BUILD_BATCH_ENTRIES", value, false)?;
    }
    if let Some(value) = &policy.remap_batch_entries {
        col.vector_engine_policy.remap_batch_entries =
            unsigned(table, &col.name, "REMAP_BATCH_ENTRIES", value, false)?;
    }
    if let Some(value) = &policy.directory_window_objects {
        col.vector_engine_policy.directory_window_objects =
            unsigned(table, &col.name, "DIRECTORY_WINDOW_OBJECTS", value, false)?;
    }
    if let Some(value) = &policy.page_window_bytes {
        col.vector_engine_policy.page_window_bytes =
            unsigned(table, &col.name, "PAGE_WINDOW_BYTES", value, false)?;
    }
    validate(table, col)
}
pub(crate) fn reset_with(table: &str, col: &mut ColumnDef, members: &[String]) -> Result<()> {
    for name in members {
        match name.as_str() {
            "quantization" => col.quantization = Default::default(),
            "calibration_max_k" => col.vector_engine_policy.calibration_max_k = None,
            "calibration_depths" => col.vector_engine_policy.calibration_depths = None,
            "probe_grid" => col.vector_engine_policy.probe_grid = None,
            "refine_factors" => col.vector_engine_policy.refine_factors = None,
            "refine_growth" => col.vector_engine_policy.refine_growth = None,
            "calibration_samples" => col.vector_engine_policy.calibration_samples = None,
            "calibration_queries" => col.vector_engine_policy.calibration_queries = None,
            "min_lists" => col.vector_engine_policy.min_lists = None,
            "vectors_per_list" => col.vector_engine_policy.vectors_per_list = None,
            "centroid_memory_percent" => col.vector_engine_policy.centroid_memory_percent = None,
            "training_samples" => col.vector_engine_policy.training_samples = None,
            "training_iterations" => col.vector_engine_policy.training_iterations = None,
            "training_seed" => col.vector_engine_policy.training_seed = None,
            "max_delta_runs" => col.vector_engine_policy.max_delta_runs = None,
            "write_buffer_bytes" => col.vector_engine_policy.write_buffer_bytes = None,
            "ingest_batch_keys" => col.vector_engine_policy.ingest_batch_keys = None,
            "coalesce_pages_per_step" => col.vector_engine_policy.coalesce_pages_per_step = None,
            "build_batch_entries" => col.vector_engine_policy.build_batch_entries = None,
            "remap_batch_entries" => col.vector_engine_policy.remap_batch_entries = None,
            "directory_window_objects" => col.vector_engine_policy.directory_window_objects = None,
            "page_window_bytes" => col.vector_engine_policy.page_window_bytes = None,
            _ => {
                return Err(Error::InvalidVectorPolicyDeclaration {
                    index: VectorIndexRef::new(table, &col.name),
                    issue: VectorPolicyDeclarationIssue::UnknownMember {
                        member: name.clone(),
                    },
                });
            }
        }
    }
    validate(table, col)
}
pub(crate) fn validate(table: &str, col: &ColumnDef) -> Result<()> {
    let p = &col.vector_engine_policy;
    if col
        .search_target_accuracy_hundredths
        .is_some_and(|v| v == 0 || v > 10000)
    {
        return Err(invalid(table, &col.name, "TARGET_ACCURACY"));
    }
    if col.search_expected_vectors == Some(0) {
        return Err(invalid(table, &col.name, "EXPECTED_VECTORS"));
    }
    if col.ivf_lists == Some(0) {
        return Err(invalid(table, &col.name, "LISTS"));
    }
    if col.ivf_probes == Some(0) {
        return Err(invalid(table, &col.name, "PROBES"));
    }
    if col.ivf_refine_factor_hundredths.is_some_and(|v| v < 100) {
        return Err(invalid(table, &col.name, "REFINE_FACTOR"));
    }
    if p.refine_growth.is_some_and(|v| v <= 100) {
        return Err(invalid(table, &col.name, "REFINE_GROWTH"));
    }
    if p.centroid_memory_percent
        .is_some_and(|v| v == 0 || v > 10000)
    {
        return Err(invalid(table, &col.name, "CENTROID_MEMORY_PERCENT"));
    }
    for (member, value) in [
        ("CALIBRATION_MAX_K", p.calibration_max_k),
        ("PROBE_GRID", p.probe_grid),
        ("CALIBRATION_SAMPLES", p.calibration_samples),
        ("MIN_LISTS", p.min_lists),
        ("VECTORS_PER_LIST", p.vectors_per_list),
        ("TRAINING_SAMPLES", p.training_samples),
        ("TRAINING_ITERATIONS", p.training_iterations),
        ("MAX_DELTA_RUNS", p.max_delta_runs),
        ("WRITE_BUFFER_BYTES", p.write_buffer_bytes),
        ("INGEST_BATCH_KEYS", p.ingest_batch_keys),
        ("COALESCE_PAGES_PER_STEP", p.coalesce_pages_per_step),
        ("BUILD_BATCH_ENTRIES", p.build_batch_entries),
        ("REMAP_BATCH_ENTRIES", p.remap_batch_entries),
        ("DIRECTORY_WINDOW_OBJECTS", p.directory_window_objects),
        ("PAGE_WINDOW_BYTES", p.page_window_bytes),
    ] {
        if value == Some(0) {
            return Err(invalid(table, &col.name, member));
        }
    }
    if p.refine_factors
        .as_ref()
        .is_some_and(|v| v.is_empty() || v.iter().any(|f| *f < 100))
    {
        return Err(invalid(table, &col.name, "REFINE_FACTORS"));
    }
    if p.calibration_depths
        .as_ref()
        .is_some_and(|v| v.is_empty() || v.contains(&0))
    {
        return Err(invalid(table, &col.name, "CALIBRATION_DEPTHS"));
    }
    if p.calibration_queries
        .as_ref()
        .is_some_and(|v| v.table.is_empty() || v.column.is_empty())
    {
        return Err(invalid(table, &col.name, "CALIBRATION_QUERIES"));
    }
    for (member, value) in [
        ("TRAINING_ITERATIONS", p.training_iterations),
        ("MIN_LISTS", p.min_lists),
        ("PROBE_GRID", p.probe_grid),
        ("CALIBRATION_MAX_K", p.calibration_max_k),
    ] {
        if value.is_some_and(|v| u32::try_from(v).is_err()) {
            return Err(invalid(table, &col.name, member));
        }
    }
    if p.max_delta_runs.is_some_and(|v| v.checked_add(1).is_none()) {
        return Err(invalid(table, &col.name, "MAX_DELTA_RUNS"));
    }
    if p.training_samples
        .is_some_and(|v| v.checked_mul(8).is_none())
    {
        return Err(invalid(table, &col.name, "TRAINING_SAMPLES"));
    }
    let has = col.search_target_accuracy_hundredths.is_some()
        || col.search_expected_vectors.is_some()
        || col.ivf_lists.is_some()
        || col.ivf_probes.is_some()
        || col.ivf_refine_factor_hundredths.is_some()
        || p != &Default::default();
    if has && !matches!(col.column_type, ColumnType::Vector(_)) {
        return Err(Error::InvalidVectorPolicyDeclaration {
            index: VectorIndexRef::new(table, &col.name),
            issue: VectorPolicyDeclarationIssue::RequiresVectorColumn,
        });
    }
    if let (Some(probes), Some(lists)) = (col.ivf_probes, col.ivf_lists)
        && probes > lists
    {
        return Err(invalid(table, &col.name, "PROBES"));
    }
    if let Some(depths) = &p.calibration_depths {
        let maximum = p.calibration_max_k.unwrap_or(100);
        for (at, d) in depths.iter().enumerate() {
            if u64::from(*d) > maximum || depths[..at].contains(d) {
                return Err(invalid(table, &col.name, "CALIBRATION_DEPTHS"));
            }
        }
    }
    Ok(())
}
pub(crate) fn from_ast(table: &str, col: &mut ColumnDef, ast: &ast::ColumnDef) -> Result<()> {
    apply_search(table, col, Some(&ast.search))?;
    apply_ivf(table, col, Some(&ast.ivf))?;
    apply_with(table, col, &ast.engine_policy)
}
pub(crate) fn to_ast(
    col: &ColumnDef,
) -> (
    ast::VectorSearchObjectives,
    ast::VectorIvfOptions,
    ast::VectorEnginePolicyOptions,
) {
    let value = |v: u64| Some(V::Value(v.to_string()));
    let search = ast::VectorSearchObjectives {
        target_accuracy: col
            .search_target_accuracy_hundredths
            .map(|v| V::Value(Decimal(u32::from(v)).to_string())),
        expected_vectors: col.search_expected_vectors.and_then(value),
    };
    let ivf = ast::VectorIvfOptions {
        lists: col.ivf_lists.and_then(|v| value(u64::from(v))),
        probes: col.ivf_probes.and_then(|v| value(u64::from(v))),
        refine_factor: col
            .ivf_refine_factor_hundredths
            .map(|v| V::Value(Decimal(v).to_string())),
    };
    let p = &col.vector_engine_policy;
    let mut policy = ast::VectorEnginePolicyOptions::default();
    policy.calibration_max_k = p.calibration_max_k.and_then(value);
    policy.calibration_depths = p.calibration_depths.as_ref().map(|values| {
        ast::VectorPolicyListValue::Values(values.iter().map(|v| v.to_string()).collect())
    });
    policy.probe_grid = p.probe_grid.and_then(value);
    policy.refine_factors = p.refine_factors.as_ref().map(|values| {
        ast::VectorPolicyListValue::Values(values.iter().map(|v| Decimal(*v).to_string()).collect())
    });
    policy.refine_growth = p.refine_growth.map(|v| V::Value(Decimal(v).to_string()));
    policy.calibration_samples = p.calibration_samples.and_then(value);
    policy.calibration_queries =
        p.calibration_queries
            .as_ref()
            .map(|s| ast::VectorPolicyReferenceValue::Reference {
                table: s.table.clone(),
                column: s.column.clone(),
            });
    policy.min_lists = p.min_lists.and_then(value);
    policy.vectors_per_list = p.vectors_per_list.and_then(value);
    policy.centroid_memory_percent = p
        .centroid_memory_percent
        .map(|v| V::Value(Decimal(v).to_string()));
    policy.training_samples = p.training_samples.and_then(value);
    policy.training_iterations = p.training_iterations.and_then(value);
    policy.training_seed = p.training_seed.and_then(value);
    policy.max_delta_runs = p.max_delta_runs.and_then(value);
    policy.write_buffer_bytes = p.write_buffer_bytes.and_then(value);
    policy.ingest_batch_keys = p.ingest_batch_keys.and_then(value);
    policy.coalesce_pages_per_step = p.coalesce_pages_per_step.and_then(value);
    policy.build_batch_entries = p.build_batch_entries.and_then(value);
    policy.remap_batch_entries = p.remap_batch_entries.and_then(value);
    policy.directory_window_objects = p.directory_window_objects.and_then(value);
    policy.page_window_bytes = p.page_window_bytes.and_then(value);
    (search, ivf, policy)
}
pub(crate) struct Decimal(pub(crate) u32);
impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0 / 100)?;
        let remainder = self.0 % 100;
        if remainder != 0 {
            if remainder.is_multiple_of(10) {
                write!(f, ".{}", remainder / 10)?;
            } else {
                write!(f, ".{remainder:02}")?;
            }
        }
        Ok(())
    }
}
pub(crate) struct With<'a>(pub(crate) &'a ColumnDef);
impl fmt::Display for With<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let col = self.0;
        let p = &col.vector_engine_policy;
        let mut first = true;
        let mut member = |name: &str, value: &dyn fmt::Display| -> fmt::Result {
            f.write_str(if first { " WITH (" } else { ", " })?;
            first = false;
            write!(f, "{name} = {value}")
        };
        if col.quantization != contextdb_core::VectorQuantization::F32 {
            member(
                "quantization",
                &format_args!("'{}'", col.quantization.as_str()),
            )?;
        }
        if let Some(v) = p.calibration_max_k {
            member("calibration_max_k", &v)?;
        }
        if let Some(v) = &p.calibration_depths {
            member("calibration_depths", &PolicyList(v, false))?;
        }
        if let Some(v) = p.probe_grid {
            member("probe_grid", &v)?;
        }
        if let Some(v) = &p.refine_factors {
            member("refine_factors", &PolicyList(v, true))?;
        }
        if let Some(v) = p.refine_growth {
            member("refine_growth", &Decimal(v))?;
        }
        if let Some(v) = p.calibration_samples {
            member("calibration_samples", &v)?;
        }
        if let Some(v) = &p.calibration_queries {
            member(
                "calibration_queries",
                &format_args!("{}.{}", v.table, v.column),
            )?;
        }
        if let Some(v) = p.min_lists {
            member("min_lists", &v)?;
        }
        if let Some(v) = p.vectors_per_list {
            member("vectors_per_list", &v)?;
        }
        if let Some(v) = p.centroid_memory_percent {
            member("centroid_memory_percent", &Decimal(v))?;
        }
        if let Some(v) = p.training_samples {
            member("training_samples", &v)?;
        }
        if let Some(v) = p.training_iterations {
            member("training_iterations", &v)?;
        }
        if let Some(v) = p.training_seed {
            member("training_seed", &v)?;
        }
        if let Some(v) = p.max_delta_runs {
            member("max_delta_runs", &v)?;
        }
        if let Some(v) = p.write_buffer_bytes {
            member("write_buffer_bytes", &v)?;
        }
        if let Some(v) = p.ingest_batch_keys {
            member("ingest_batch_keys", &v)?;
        }
        if let Some(v) = p.coalesce_pages_per_step {
            member("coalesce_pages_per_step", &v)?;
        }
        if let Some(v) = p.build_batch_entries {
            member("build_batch_entries", &v)?;
        }
        if let Some(v) = p.remap_batch_entries {
            member("remap_batch_entries", &v)?;
        }
        if let Some(v) = p.directory_window_objects {
            member("directory_window_objects", &v)?;
        }
        if let Some(v) = p.page_window_bytes {
            member("page_window_bytes", &v)?;
        }
        if !first {
            f.write_char(')')?;
        }
        Ok(())
    }
}
struct PolicyList<'a>(&'a [u32], bool);
impl fmt::Display for PolicyList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('[')?;
        for (at, v) in self.0.iter().enumerate() {
            if at > 0 {
                f.write_str(", ")?;
            }
            if self.1 {
                write!(f, "{}", Decimal(*v))?;
            } else {
                write!(f, "{v}")?;
            }
        }
        f.write_char(']')
    }
}
pub(crate) struct Groups<'a>(pub(crate) &'a ColumnDef);
impl fmt::Display for Groups<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = self.0;
        if c.search_target_accuracy_hundredths.is_some() || c.search_expected_vectors.is_some() {
            f.write_str(" SEARCH (TARGET_ACCURACY = ")?;
            if let Some(v) = c.search_target_accuracy_hundredths {
                write!(f, "{}", Decimal(u32::from(v)))?;
            } else {
                f.write_str("DEFAULT")?;
            }
            f.write_str(", EXPECTED_VECTORS = ")?;
            if let Some(v) = c.search_expected_vectors {
                write!(f, "{v}")?;
            } else {
                f.write_str("DEFAULT")?;
            }
            f.write_char(')')?;
        }
        let mut first = true;
        for (name, value, decimal) in [
            ("LISTS", c.ivf_lists, false),
            ("PROBES", c.ivf_probes, false),
            ("REFINE_FACTOR", c.ivf_refine_factor_hundredths, true),
        ] {
            if let Some(v) = value {
                f.write_str(if first { " IVF (" } else { ", " })?;
                first = false;
                write!(f, "{name} = ")?;
                if decimal {
                    write!(f, "{}", Decimal(v))?;
                } else {
                    write!(f, "{v}")?;
                }
            }
        }
        if !first {
            f.write_char(')')?;
        }
        Ok(())
    }
}

pub(crate) fn validate_query(
    table: &str,
    column: &str,
    policy: Option<&ast::VectorSearchOverride>,
) -> Result<()> {
    match policy {
        Some(ast::VectorSearchOverride::Objectives { target_accuracy }) => {
            hundredths(table, column, "TARGET_ACCURACY", target_accuracy, 1, 10000)?;
        }
        Some(ast::VectorSearchOverride::Ivf {
            probes,
            refine_factor,
        }) => {
            if let Some(v) = probes {
                if let Some(v) = unsigned(table, column, "PROBES", v, false)? {
                    u32::try_from(v).map_err(|_| invalid(table, column, "PROBES"))?;
                }
            }
            if let Some(v) = refine_factor {
                hundredths(table, column, "REFINE_FACTOR", v, 100, u32::MAX)?;
            }
        }
        None => {}
    }
    Ok(())
}
/// Literal statement breadth, retained as scalars with the query continuation.
/// An inner None clears the corresponding column declaration for this query.
#[derive(Clone, Copy, Default)]
pub(crate) struct QueryBreadth {
    pub target: Option<Option<u16>>,
    pub probes: Option<Option<u32>>,
    pub refine_factor: Option<Option<u32>>,
}
impl QueryBreadth {
    pub(crate) fn resolve(
        table: &str,
        column: &str,
        policy: Option<&ast::VectorSearchOverride>,
    ) -> Result<Self> {
        validate_query(table, column, policy)?;
        if let Some(ast::VectorSearchOverride::Objectives { target_accuracy }) = policy {
            return Ok(Self {
                target: Some(
                    hundredths(table, column, "TARGET_ACCURACY", target_accuracy, 1, 10000)?
                        .map(|value| {
                            u16::try_from(value)
                                .map_err(|_| invalid(table, column, "TARGET_ACCURACY"))
                        })
                        .transpose()?,
                ),
                ..Self::default()
            });
        }
        let Some(ast::VectorSearchOverride::Ivf {
            probes,
            refine_factor,
        }) = policy
        else {
            return Ok(Self::default());
        };
        Ok(Self {
            target: None,
            probes: probes
                .as_ref()
                .map(|v| {
                    unsigned(table, column, "PROBES", v, false).and_then(|v| {
                        v.map(|v| u32::try_from(v).map_err(|_| invalid(table, column, "PROBES")))
                            .transpose()
                    })
                })
                .transpose()?,
            refine_factor: refine_factor
                .as_ref()
                .map(|v| hundredths(table, column, "REFINE_FACTOR", v, 100, u32::MAX))
                .transpose()?,
        })
    }
}
pub(crate) fn query_policy_capacity(policy: Option<&ast::VectorSearchOverride>) -> usize {
    let value = |v: &V| match v {
        V::Value(s) => s.capacity(),
        V::Default => 0,
    };
    match policy {
        Some(ast::VectorSearchOverride::Objectives { target_accuracy }) => value(target_accuracy),
        Some(ast::VectorSearchOverride::Ivf {
            probes,
            refine_factor,
        }) => probes
            .as_ref()
            .map_or(0, value)
            .saturating_add(refine_factor.as_ref().map_or(0, value)),
        None => 0,
    }
}

/// Render parsed declarations without allocating a second schema or validating by panic.
pub(crate) struct AstWith<'a>(pub(crate) &'a ast::ColumnDef);
struct AstNumber<'a>(&'a str);
impl fmt::Display for AstNumber<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(value) = self.0.parse::<u64>() {
            return write!(f, "{value}");
        }
        if let Some((whole, fraction)) = self.0.split_once('.')
            && !fraction.is_empty()
            && fraction.len() <= 2
            && fraction.bytes().all(|b| b.is_ascii_digit())
            && let (Ok(whole), Ok(fraction_value)) = (whole.parse::<u32>(), fraction.parse::<u32>())
            && let Some(value) = whole.checked_mul(100).and_then(|n| {
                n.checked_add(fraction_value * if fraction.len() == 1 { 10 } else { 1 })
            })
        {
            return write!(f, "{}", Decimal(value));
        }
        f.write_str(self.0)
    }
}
enum AstMember<'a> {
    Number(&'a V),
    List(&'a ast::VectorPolicyListValue),
    Reference(&'a ast::VectorPolicyReferenceValue),
}
impl AstMember<'_> {
    fn explicit(&self) -> bool {
        match self {
            Self::Number(V::Value(_))
            | Self::List(ast::VectorPolicyListValue::Values(_))
            | Self::Reference(ast::VectorPolicyReferenceValue::Reference { .. }) => true,
            _ => false,
        }
    }
}
impl fmt::Display for AstMember<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(V::Value(raw)) => write!(f, "{}", AstNumber(raw)),
            Self::List(ast::VectorPolicyListValue::Values(values)) => {
                f.write_char('[')?;
                for (at, raw) in values.iter().enumerate() {
                    if at > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", AstNumber(raw))?;
                }
                f.write_char(']')
            }
            Self::Reference(ast::VectorPolicyReferenceValue::Reference { table, column }) => {
                write!(f, "{table}.{column}")
            }
            _ => f.write_str("DEFAULT"),
        }
    }
}
impl fmt::Display for AstWith<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let col = self.0;
        let p = &col.engine_policy;
        let mut first = true;
        let mut member = |name: &str, value: &dyn fmt::Display| -> fmt::Result {
            f.write_str(if first { " WITH (" } else { ", " })?;
            first = false;
            write!(f, "{name} = {value}")
        };
        let quantization = match col.quantization {
            ast::VectorQuantization::F32 => None,
            ast::VectorQuantization::SQ8 => Some("'SQ8'"),
            ast::VectorQuantization::SQ4 => Some("'SQ4'"),
        };
        if let Some(q) = quantization {
            member("quantization", &q)?;
        }
        if let Some(value) = &p.calibration_max_k {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("calibration_max_k", &value)?;
            }
        }
        if let Some(value) = &p.calibration_depths {
            let value = AstMember::List(value);
            if value.explicit() {
                member("calibration_depths", &value)?;
            }
        }
        if let Some(value) = &p.probe_grid {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("probe_grid", &value)?;
            }
        }
        if let Some(value) = &p.refine_factors {
            let value = AstMember::List(value);
            if value.explicit() {
                member("refine_factors", &value)?;
            }
        }
        if let Some(value) = &p.refine_growth {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("refine_growth", &value)?;
            }
        }
        if let Some(value) = &p.calibration_samples {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("calibration_samples", &value)?;
            }
        }
        if let Some(value) = &p.calibration_queries {
            let value = AstMember::Reference(value);
            if value.explicit() {
                member("calibration_queries", &value)?;
            }
        }
        if let Some(value) = &p.min_lists {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("min_lists", &value)?;
            }
        }
        if let Some(value) = &p.vectors_per_list {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("vectors_per_list", &value)?;
            }
        }
        if let Some(value) = &p.centroid_memory_percent {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("centroid_memory_percent", &value)?;
            }
        }
        if let Some(value) = &p.training_samples {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("training_samples", &value)?;
            }
        }
        if let Some(value) = &p.training_iterations {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("training_iterations", &value)?;
            }
        }
        if let Some(value) = &p.training_seed {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("training_seed", &value)?;
            }
        }
        if let Some(value) = &p.max_delta_runs {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("max_delta_runs", &value)?;
            }
        }
        if let Some(value) = &p.write_buffer_bytes {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("write_buffer_bytes", &value)?;
            }
        }
        if let Some(value) = &p.ingest_batch_keys {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("ingest_batch_keys", &value)?;
            }
        }
        if let Some(value) = &p.coalesce_pages_per_step {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("coalesce_pages_per_step", &value)?;
            }
        }
        if let Some(value) = &p.build_batch_entries {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("build_batch_entries", &value)?;
            }
        }
        if let Some(value) = &p.remap_batch_entries {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("remap_batch_entries", &value)?;
            }
        }
        if let Some(value) = &p.directory_window_objects {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("directory_window_objects", &value)?;
            }
        }
        if let Some(value) = &p.page_window_bytes {
            let value = AstMember::Number(value);
            if value.explicit() {
                member("page_window_bytes", &value)?;
            }
        }
        for name in &p.unknown_members {
            member(name, &"DEFAULT")?;
        }
        if !first {
            f.write_char(')')?;
        }
        Ok(())
    }
}
pub(crate) struct AstGroups<'a>(pub(crate) &'a ast::ColumnDef);
impl fmt::Display for AstGroups<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = self.0;
        let explicit = |v: &Option<V>| matches!(v, Some(V::Value(_)));
        if explicit(&c.search.target_accuracy) || explicit(&c.search.expected_vectors) {
            f.write_str(" SEARCH (TARGET_ACCURACY = ")?;
            match &c.search.target_accuracy {
                Some(v) => write!(f, "{}", AstMember::Number(v))?,
                None => f.write_str("DEFAULT")?,
            }
            f.write_str(", EXPECTED_VECTORS = ")?;
            match &c.search.expected_vectors {
                Some(v) => write!(f, "{}", AstMember::Number(v))?,
                None => f.write_str("DEFAULT")?,
            }
            f.write_char(')')?;
        }
        let mut first = true;
        for (name, value) in [
            ("LISTS", &c.ivf.lists),
            ("PROBES", &c.ivf.probes),
            ("REFINE_FACTOR", &c.ivf.refine_factor),
        ] {
            if let Some(v) = value
                && explicit(value)
            {
                f.write_str(if first { " IVF (" } else { ", " })?;
                first = false;
                write!(f, "{name} = {}", AstMember::Number(v))?;
            }
        }
        if !first {
            f.write_char(')')?;
        }
        Ok(())
    }
}
