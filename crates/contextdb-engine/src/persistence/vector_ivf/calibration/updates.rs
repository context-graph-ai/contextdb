//! Fixed receipt operations seal in the actual native transaction.
use super::super::qualification::{
    self, Depth, Header, Oracle, Prefix, Record, Rewrite, RowChunk, SampleWork,
};
use super::{
    super::*,
    capture::{Capture, Sample},
};
use contextdb_vector::ivf::Hit;
pub(super) enum Update<'a> {
    Header(Header),
    Oracle {
        sample: u32,
        identity: Sample,
        hits: &'a [Hit],
        work: u64,
    },
    Rewrite {
        depth_ordinal: u32,
        sample: u32,
        answer: &'a [u64],
    },
    Work(SampleWork),
    Depth {
        ordinal: u32,
        previous: u32,
        row: Depth,
        minimum: &'a [u32],
        below: &'a [u32],
    },
    Finish,
}
fn ordinal(key: [u8; 9]) -> u64 {
    u64::from_be_bytes(key[1..].try_into().expect("fixed qualification key suffix"))
}
fn header(scope: &write::WriteScope<'_, '_>, generation: u64) -> Result<Header> {
    match scope.read_qualification(generation, 0, 0)? {
        Some(Record::Header(header)) => Ok(header),
        _ => Err(corrupt()),
    }
}
impl RedbPersistence {
    pub(in crate::persistence::vector_ivf::calibration) fn apply_calibration_update(
        &self,
        capture: &Capture,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        update: &Update<'_>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        let _control = memory::Invoice::reserve(
            &capture.owner,
            std::mem::size_of::<Update<'_>>(),
            "clustered_calibration_metadata_control",
        )?;
        self.write_calibration(capture, index, tuple, checkpoint, |scope| {
            let generation = capture.manifest.generation;
            match update {
                Update::Header(header) => {
                    scope.write_qualification(generation, 0, 0, Record::Header(*header))?
                }
                Update::Oracle {
                    sample,
                    identity,
                    hits,
                    work,
                } => {
                    scope.write_qualification(
                        generation,
                        3,
                        u64::from(*sample),
                        Record::Sample(*identity),
                    )?;
                    scope.write_qualification(
                        generation,
                        8,
                        u64::from(*sample),
                        Record::Oracle(Oracle {
                            count: u32::try_from(hits.len()).map_err(|_| corrupt())?,
                            work: *work,
                        }),
                    )?;
                    for (chunk, part) in hits.chunks(16).enumerate() {
                        checkpoint()?;
                        let mut rows = [0; 16];
                        for (to, hit) in rows.iter_mut().zip(part) {
                            *to = hit.row;
                        }
                        let key = qualification::paired_key(
                            4,
                            *sample,
                            u32::try_from(chunk).map_err(|_| corrupt())?,
                        );
                        scope.write_qualification(
                            generation,
                            4,
                            ordinal(key),
                            Record::Rows(RowChunk {
                                count: part.len() as u32,
                                rows,
                            }),
                        )?;
                    }
                    let mut header = header(scope, generation)?;
                    header.samples = header
                        .samples
                        .max(sample.checked_add(1).ok_or_else(corrupt)?);
                    header.selection_samples = header.samples / 2;
                    header.validation_samples = header.samples - header.selection_samples;
                    scope.write_qualification(generation, 0, 0, Record::Header(header))?;
                }
                Update::Rewrite {
                    depth_ordinal,
                    sample,
                    answer,
                } => {
                    let mut header = header(scope, generation)?;
                    let first = header.next_chunk;
                    for part in answer.chunks(16) {
                        checkpoint()?;
                        let mut rows = [0; 16];
                        rows[..part.len()].copy_from_slice(part);
                        scope.write_qualification(
                            generation,
                            5,
                            header.next_chunk,
                            Record::Rows(RowChunk {
                                count: part.len() as u32,
                                rows,
                            }),
                        )?;
                        header.next_chunk = header.next_chunk.checked_add(1).ok_or_else(corrupt)?;
                    }
                    let key = qualification::paired_key(6, *depth_ordinal, *sample);
                    scope.write_qualification(
                        generation,
                        6,
                        ordinal(key),
                        Record::Rewrite(Rewrite {
                            depth_ordinal: *depth_ordinal,
                            sample: *sample,
                            first_chunk: first,
                            answer_length: u32::try_from(answer.len()).map_err(|_| corrupt())?,
                        }),
                    )?;
                    scope.write_qualification(generation, 0, 0, Record::Header(header))?;
                }
                Update::Work(work) => {
                    let key = qualification::sample_work_key(work.depth_ordinal, work.sample);
                    scope.write_qualification(
                        generation,
                        7,
                        ordinal(key),
                        Record::SampleWork(*work),
                    )?;
                }
                Update::Depth {
                    ordinal: depth_ordinal,
                    previous,
                    row,
                    minimum,
                    below,
                } => {
                    scope.write_qualification(
                        generation,
                        1,
                        u64::from(*depth_ordinal),
                        Record::Depth(*row),
                    )?;
                    for k in previous.checked_add(1).ok_or_else(corrupt)?..=row.depth {
                        checkpoint()?;
                        let at = k as usize - 1;
                        scope.write_qualification(
                            generation,
                            2,
                            u64::from(k),
                            Record::Prefix(Prefix {
                                depth: row.depth,
                                k,
                                minimum_hits: if row.samples == 0 {
                                    0
                                } else {
                                    *minimum.get(at).ok_or_else(corrupt)?
                                },
                                samples: row.samples,
                                queries_below: if row.samples == 0 {
                                    0
                                } else {
                                    *below.get(at).ok_or_else(corrupt)?
                                },
                            }),
                        )?;
                    }
                    let mut header = header(scope, generation)?;
                    header.depths = depth_ordinal.checked_add(1).ok_or_else(corrupt)?;
                    scope.write_qualification(generation, 0, 0, Record::Header(header))?;
                }
                Update::Finish => {
                    let mut header = header(scope, generation)?;
                    header.complete = true;
                    scope.write_qualification(generation, 0, 0, Record::Header(header))?;
                }
            }
            Ok(())
        })
    }
}
pub(super) fn load_oracle(
    source: &read::PageSource<'_>,
    generation: u64,
    sample: u32,
    rows: &mut [u64],
) -> Result<Oracle> {
    let Some(Record::Oracle(oracle)) =
        source.calibration_record(generation, 8, u64::from(sample))?
    else {
        return Err(corrupt());
    };
    let count = oracle.count as usize;
    if count > rows.len() {
        return Err(corrupt());
    }
    let mut copied = 0;
    while copied < count {
        let chunk = u32::try_from(copied / 16).map_err(|_| corrupt())?;
        let key = qualification::paired_key(4, sample, chunk);
        let Some(Record::Rows(part)) = source.calibration_record(generation, 4, ordinal(key))?
        else {
            return Err(corrupt());
        };
        let take = (count - copied).min(16);
        if part.count as usize != take {
            return Err(corrupt());
        }
        rows[copied..copied + take].copy_from_slice(&part.rows[..take]);
        copied += take;
    }
    Ok(oracle)
}
pub(super) fn load_rewrite(
    source: &read::PageSource<'_>,
    generation: u64,
    depth_ordinal: u32,
    sample: u32,
    rows: &mut [u64],
) -> Result<Option<usize>> {
    let key = qualification::paired_key(6, depth_ordinal, sample);
    let Some(record) = source.calibration_record(generation, 6, ordinal(key))? else {
        return Ok(None);
    };
    let Record::Rewrite(rewrite) = record else {
        return Err(corrupt());
    };
    if rewrite.depth_ordinal != depth_ordinal
        || rewrite.sample != sample
        || rewrite.answer_length as usize > rows.len()
    {
        return Err(corrupt());
    }
    let count = rewrite.answer_length as usize;
    let mut copied = 0;
    while copied < count {
        let chunk = rewrite
            .first_chunk
            .checked_add((copied / 16) as u64)
            .ok_or_else(corrupt)?;
        let Some(Record::Rows(part)) = source.calibration_record(generation, 5, chunk)? else {
            return Err(corrupt());
        };
        let take = (count - copied).min(16);
        if part.count as usize != take {
            return Err(corrupt());
        }
        rows[copied..copied + take].copy_from_slice(&part.rows[..take]);
        copied += take;
    }
    Ok(Some(count))
}
