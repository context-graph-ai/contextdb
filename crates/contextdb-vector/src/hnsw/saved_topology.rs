//! Borrowed inspection and finite export of an immutable saved topology.
//! The engine owns publication; this adapter never builds or inserts a graph.
use super::paged_builder::{PagedEdge, PagedGraphState, PagedPoint, PagedVectorPoint};
use super::*;

fn invalid() -> Error {
    Error::Other("saved graph topology is invalid".into())
}

/// Fixed topology controls and borrowed directories. No population-sized
/// allocation is retained while inspecting or emitting this representation.
pub struct SavedTopology<'a> {
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub extend_candidates: bool,
    pub keep_pruned: bool,
    pub max_layer: usize,
    pub scale_bits: u64,
    pub state: PagedGraphState,
    pub max_point_bytes: usize,
    graph: &'a [u8],
    offsets: [usize; 16],
    rows: &'a [u8],
    exact: &'a [u8],
}

/// A scalar restart cursor, independent of graph population.
#[derive(Clone, Copy, Default)]
pub struct SavedPointCursor {
    pub layer: usize,
    pub rank: usize,
    pub offset: usize,
}

pub struct SavedPoint<'a> {
    pub id: PointId,
    pub origin: usize,
    pub row: RowId,
    pub encoded_bytes: usize,
    values: &'a [u8],
    edges: &'a [u8],
    quantization: VectorQuantization,
}
impl SavedPoint<'_> {
    /// Upper bound for the one native decode, native encoding and key frame
    /// that coexist. The caller admits this before `decode`.
    pub fn workspace_bytes(&self) -> Result<usize> {
        self.encoded_bytes
            .checked_mul(8)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(invalid)
    }
    pub fn exact_key(&self) -> Vec<u8> {
        if self.quantization == VectorQuantization::F32 {
            self.values
                .chunks_exact(4)
                .flat_map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()).to_be_bytes())
                .collect()
        } else {
            self.values.to_vec()
        }
    }
    pub fn decode(&self) -> Result<PagedVectorPoint> {
        let mut reader = DurableReader::new(self.edges);
        let mut neighbours = Vec::with_capacity(16);
        for _ in 0..16 {
            let count = reader.read_usize()?;
            let mut edges = Vec::with_capacity(count);
            for _ in 0..count {
                edges.push(PagedEdge {
                    origin_id: reader.read_usize()?,
                    point_id: reader.read_point_id()?,
                    distance: reader.read_f32()?,
                });
            }
            neighbours.push(edges);
        }
        if !reader.is_empty() {
            return Err(invalid());
        }
        Ok(if self.quantization == VectorQuantization::F32 {
            let values = self
                .values
                .chunks_exact(4)
                .map(|bytes| f32::from_bits(u32::from_le_bytes(bytes.try_into().unwrap())))
                .collect();
            PagedVectorPoint::F32(PagedPoint {
                id: self.id,
                origin_id: self.origin,
                values,
                neighbours,
            })
        } else {
            PagedVectorPoint::Quantized(PagedPoint {
                id: self.id,
                origin_id: self.origin,
                values: self.values.to_vec(),
                neighbours,
            })
        })
    }
}
impl<'a> SavedTopology<'a> {
    pub fn inspect(
        bytes: &'a [u8],
        dimension: usize,
        quantization: VectorQuantization,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Self> {
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(8)? != DURABLE_ENVELOPE_MAGIC
            || reader.read_u16()? != DURABLE_ENVELOPE_VERSION
        {
            return Err(invalid());
        }
        let checksum = reader.read_u64()?;
        let mut actual = 0xcbf2_9ce4_8422_2325u64;
        for chunk in reader.remaining().chunks(64 * 1024) {
            checkpoint()?;
            for byte in chunk {
                actual = (actual ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        if actual != checksum
            || reader.read_usize()? != dimension
            || quantization_from_tag(reader.read_u8()?)? != quantization
        {
            return Err(invalid());
        }
        let ef_search = reader.read_usize()?;
        if ef_search == 0 {
            return Err(invalid());
        }
        let graph_bytes = reader.read_bytes()?;
        let mut graph = DurableReader::new(graph_bytes);
        if graph.read_u16()? != 1 {
            return Err(invalid());
        }
        let m = graph.read_usize()?;
        let ef_construction = graph.read_usize()?;
        let extend_candidates = match graph.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(invalid()),
        };
        let keep_pruned = match graph.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(invalid()),
        };
        let max_layer = graph.read_usize()?;
        let value_dimension = graph.read_usize()?;
        let scale = graph.read_f64()?;
        let expected_values = match quantization {
            VectorQuantization::F32 => dimension,
            VectorQuantization::SQ8 => 12usize.checked_add(dimension).ok_or_else(invalid)?,
            VectorQuantization::SQ4 => 12usize
                .checked_add(dimension.div_ceil(2))
                .ok_or_else(invalid)?,
        };
        if m == 0
            || ef_construction == 0
            || max_layer == 0
            || max_layer > 16
            || !scale.is_finite()
            || scale <= 0.0
            || value_dimension != expected_values
            || graph.read_usize()? != max_layer
        {
            return Err(invalid());
        }
        let mut state = PagedGraphState::new(HNSW_BUILD_SEED);
        let mut offsets = [0usize; 16];
        let mut max_point_bytes = 0usize;
        for (layer, offset) in offsets.iter_mut().enumerate().take(max_layer) {
            let count = graph.read_usize()?;
            if count > graph.remaining().len() / 29 || count > i32::MAX as usize {
                return Err(invalid());
            }
            state.layer_counts[layer] = count;
            *offset = graph.position;
            for rank in 0..count {
                checkpoint()?;
                let start = graph.position;
                graph.read_usize()?;
                if graph.read_point_id()? != PointId(layer as u8, rank as i32)
                    || graph.read_usize()? != value_dimension
                {
                    return Err(invalid());
                }
                let width = if quantization == VectorQuantization::F32 {
                    4
                } else {
                    1
                };
                let values =
                    graph.read_exact(value_dimension.checked_mul(width).ok_or_else(invalid)?)?;
                validate_values(values, dimension, quantization)?;
                if graph.read_usize()? != 16 {
                    return Err(invalid());
                }
                for neighbor_layer in 0..16 {
                    let count = graph.read_usize()?;
                    if count > graph.remaining().len() / 17
                        || (neighbor_layer >= max_layer && count != 0)
                    {
                        return Err(invalid());
                    }
                    if neighbor_layer == 0 {
                        state.layer0_edges =
                            state.layer0_edges.checked_add(count).ok_or_else(invalid)?;
                    }
                    for _ in 0..count {
                        graph.read_usize()?;
                        graph.read_point_id()?;
                        if !graph.read_f32()?.is_finite() {
                            return Err(invalid());
                        }
                    }
                }
                // Native fixed widths differ only by their framing; the
                // actual input record bounds every point and edge allocation.
                max_point_bytes = max_point_bytes.max(graph.position - start + 64);
                state.points = state.points.checked_add(1).ok_or_else(invalid)?;
            }
        }
        state.entry_point = match graph.read_u8()? {
            0 => None,
            1 => Some(graph.read_point_id()?),
            _ => return Err(invalid()),
        };
        if !graph.is_empty()
            || (state.points == 0) != state.entry_point.is_none()
            || state.entry_point.is_some_and(|id| {
                id.1 < 0
                    || state
                        .layer_counts
                        .get(id.0 as usize)
                        .is_none_or(|n| id.1 as usize >= *n)
            })
        {
            return Err(invalid());
        }
        let row_count = reader.read_usize()?;
        if row_count != state.points {
            return Err(invalid());
        }
        let rows = reader.read_exact(row_count.checked_mul(16).ok_or_else(invalid)?)?;
        let mut prior = None;
        for row in rows.chunks_exact(16) {
            checkpoint()?;
            let origin = u64::from_le_bytes(row[..8].try_into().unwrap());
            if prior.is_some_and(|prior| origin <= prior) {
                return Err(invalid());
            }
            prior = Some(origin);
        }
        let exact = reader.remaining();
        let view = Self {
            dimension,
            quantization,
            m,
            ef_construction,
            ef_search,
            extend_candidates,
            keep_pruned,
            max_layer,
            scale_bits: scale.to_bits(),
            state,
            max_point_bytes,
            graph: graph_bytes,
            offsets,
            rows,
            exact,
        };
        view.visit_exact(&mut |_, _| checkpoint())?;
        Ok(view)
    }
    fn row(&self, origin: usize) -> Result<RowId> {
        let mut low = 0usize;
        let mut high = self.rows.len() / 16;
        while low < high {
            let mid = low + (high - low) / 2;
            let entry = &self.rows[mid * 16..mid * 16 + 16];
            let found = u64::from_le_bytes(entry[..8].try_into().unwrap());
            match found.cmp(&(origin as u64)) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    return Ok(RowId(u64::from_le_bytes(entry[8..].try_into().unwrap())));
                }
            }
        }
        Err(invalid())
    }
    pub fn next_point(&self, cursor: &mut SavedPointCursor) -> Result<Option<SavedPoint<'a>>> {
        while cursor.layer < self.max_layer && cursor.rank == self.state.layer_counts[cursor.layer]
        {
            cursor.layer += 1;
            cursor.rank = 0;
            cursor.offset = 0;
        }
        if cursor.layer == self.max_layer {
            return Ok(None);
        }
        let mut reader = DurableReader::new(self.graph);
        reader.position = if cursor.offset == 0 {
            self.offsets[cursor.layer]
        } else {
            cursor.offset
        };
        let start = reader.position;
        let origin = reader.read_usize()?;
        let id = reader.read_point_id()?;
        if id != PointId(cursor.layer as u8, cursor.rank as i32) {
            return Err(invalid());
        }
        let count = reader.read_usize()?;
        let values = reader.read_exact(
            count
                .checked_mul(if self.quantization == VectorQuantization::F32 {
                    4
                } else {
                    1
                })
                .ok_or_else(invalid)?,
        )?;
        if reader.read_usize()? != 16 {
            return Err(invalid());
        }
        let edge_start = reader.position;
        for _ in 0..16 {
            let count = reader.read_usize()?;
            reader.skip(count.checked_mul(17).ok_or_else(invalid)?)?;
        }
        let edges = &self.graph[edge_start..reader.position];
        cursor.offset = reader.position;
        cursor.rank += 1;
        Ok(Some(SavedPoint {
            id,
            origin,
            row: self.row(origin)?,
            encoded_bytes: reader.position - start + 64,
            values,
            edges,
            quantization: self.quantization,
        }))
    }
    pub fn visit_exact(&self, visitor: &mut dyn FnMut(&[u8], RowId) -> Result<()>) -> Result<()> {
        let mut reader = DurableReader::new(self.exact);
        let count = reader.read_usize()?;
        if count > reader.remaining().len() / 16 {
            return Err(invalid());
        }
        let mut prior = None;
        for _ in 0..count {
            let key = reader.read_bytes()?;
            if key.is_empty() || prior.is_some_and(|p: &[u8]| key <= p) {
                return Err(invalid());
            }
            if self.quantization == VectorQuantization::F32 {
                if self.dimension.checked_mul(4) != Some(key.len())
                    || key.chunks_exact(4).any(|bytes| {
                        !f32::from_bits(u32::from_be_bytes(bytes.try_into().unwrap())).is_finite()
                    })
                {
                    return Err(invalid());
                }
            } else {
                validate_values(key, self.dimension, self.quantization)?;
            }
            let rows = reader.read_usize()?;
            if rows == 0 || rows > reader.remaining().len() / 8 {
                return Err(invalid());
            }
            let mut prior_row = None;
            for _ in 0..rows {
                let row = RowId(reader.read_u64()?);
                if prior_row.is_some_and(|p| row <= p) {
                    return Err(invalid());
                }
                visitor(key, row)?;
                prior_row = Some(row);
            }
            prior = Some(key);
        }
        if !reader.is_empty() {
            return Err(invalid());
        }
        Ok(())
    }
}
fn validate_values(bytes: &[u8], dimension: usize, quantization: VectorQuantization) -> Result<()> {
    match quantization {
        VectorQuantization::F32 => {
            if dimension.checked_mul(4) != Some(bytes.len())
                || bytes
                    .chunks_exact(4)
                    .any(|b| !f32::from_bits(u32::from_le_bytes(b.try_into().unwrap())).is_finite())
            {
                return Err(invalid());
            }
        }
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            let payload = if quantization == VectorQuantization::SQ8 {
                dimension
            } else {
                dimension.div_ceil(2)
            };
            if payload.checked_add(12) != Some(bytes.len())
                || bytes.len() < 12
                || u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize != dimension
            {
                return Err(invalid());
            }
            let min = f32::from_bits(u32::from_le_bytes(bytes[4..8].try_into().unwrap()));
            let max = f32::from_bits(u32::from_le_bytes(bytes[8..12].try_into().unwrap()));
            if !min.is_finite() || !max.is_finite() || min > max {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
