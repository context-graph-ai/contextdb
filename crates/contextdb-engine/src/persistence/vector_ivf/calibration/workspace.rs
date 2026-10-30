//! One original-maintenance query/output object; no population-sized answer map.
use super::{super::*, capture::Capture};
use contextdb_vector::ivf::Hit;
use std::alloc::Layout;

pub(super) struct Workspace {
    pub query: Vec<f32>,
    pub hits: Vec<Hit>,
    pub exact: Vec<u64>,
    pub rewritten: Option<Vec<u64>>,
    pub page: [u8; PAGE_BYTES],
    // All physical backings and the complete control free before the invoice.
    _invoice: memory::Invoice,
}
impl Workspace {
    pub(super) fn new(capture: &Capture, rewritten: bool) -> Result<Self> {
        let depth = usize::try_from(capture.maximum_depth).map_err(|_| corrupt())?;
        let dimensions = capture.manifest.dimensions;
        let mut layout = Layout::new::<Self>();
        for part in [
            Layout::array::<f32>(dimensions),
            Layout::array::<Hit>(depth),
            Layout::array::<u64>(depth),
        ] {
            layout = layout
                .extend(part.map_err(|_| corrupt())?)
                .map_err(|_| corrupt())?
                .0;
        }
        if rewritten {
            layout = layout
                .extend(Layout::array::<u64>(depth).map_err(|_| corrupt())?)
                .map_err(|_| corrupt())?
                .0;
        }
        let invoice = memory::Invoice::reserve(
            &capture.owner,
            layout.pad_to_align().size(),
            "clustered_calibration_query_output",
        )?;
        Ok(Self {
            query: vec![0.0; dimensions],
            hits: vec![Hit::default(); depth],
            exact: vec![0; depth],
            rewritten: rewritten.then(|| vec![0; depth]),
            page: [0; PAGE_BYTES],
            _invoice: invoice,
        })
    }
}
