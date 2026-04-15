use crate::compaction::publish::record::PkPublishDelta;
use crate::primary_key::DeleteVector;
use crate::rowset::RowsetSharedPtr;
use crate::tablet::{PhysicalRowRef, Tablet};
use paro_common::error::{self as paro_error, Result};
use std::collections::{HashMap, HashSet};

pub(crate) fn apply_pk_publish(
    tablet: &Tablet,
    inputs: &[RowsetSharedPtr],
    output: &RowsetSharedPtr,
    pk_delta: PkPublishDelta,
) -> Result<()> {
    if pk_delta.upsert_candidates.is_empty() && pk_delta.internal_delete_vectors.is_empty() {
        return Ok(());
    }

    let mut latest = HashMap::new();
    for candidate in pk_delta.upsert_candidates {
        latest.insert(
            candidate.key,
            (candidate.output_location, candidate.source_location),
        );
    }

    let mut delete_vectors: HashMap<u32, DeleteVector> = HashMap::new();
    for delta in pk_delta.internal_delete_vectors {
        delete_vectors.insert(delta.segment_id, delta.delete_vector);
    }

    resolve_primary_keys(
        tablet,
        inputs,
        output,
        latest,
        delete_vectors,
        pk_delta.max_input_version,
    )
}

fn resolve_primary_keys(
    tablet: &Tablet,
    inputs: &[RowsetSharedPtr],
    output: &RowsetSharedPtr,
    latest: HashMap<Vec<u8>, (PhysicalRowRef, PhysicalRowRef)>,
    mut delete_vectors: HashMap<u32, DeleteVector>,
    max_input_version: i64,
) -> Result<()> {
    if latest.is_empty() {
        persist_delete_vectors(output, delete_vectors)?;
        return Ok(());
    }

    let pairs: Vec<_> = latest
        .iter()
        .map(|(key, (output_location, _))| (key.clone(), *output_location))
        .collect();
    let successful_pairs = tablet.try_replace_primary_index_entries(pairs, max_input_version)?;
    let successful_keys: HashSet<&Vec<u8>> = successful_pairs.iter().map(|(key, _)| key).collect();

    let mut survivors = Vec::new();
    for (key, (output_location, source_location)) in latest {
        if !successful_keys.contains(&key) {
            delete_vectors
                .entry(output_location.segment_id)
                .or_default()
                .mark_deleted(output_location.row_offset);
        } else {
            survivors.push((key, output_location, source_location));
        }
    }

    let input_map: HashMap<u64, &RowsetSharedPtr> = inputs
        .iter()
        .map(|rowset| (rowset.rowset_id(), rowset))
        .collect();
    let mut input_dv_cache: HashMap<(u64, u32), Option<DeleteVector>> = HashMap::new();

    for (_key, output_location, source_location) in survivors {
        let delete_vector = if let Some(cached) = input_dv_cache.get(&source_location.segment_key())
        {
            cached.as_ref()
        } else {
            let rowset = input_map.get(&source_location.rowset_id).ok_or_else(|| {
                paro_error::serialization_failure(format!(
                    "missing source rowset {} during compaction pk publish",
                    source_location.rowset_id
                ))
            })?;
            let loaded = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                source_location.segment_id,
                max_input_version,
            )?;
            input_dv_cache.insert(source_location.segment_key(), loaded);
            input_dv_cache
                .get(&source_location.segment_key())
                .and_then(|candidate| candidate.as_ref())
        };

        if delete_vector
            .as_ref()
            .is_some_and(|dv| dv.is_deleted(source_location.row_offset))
        {
            delete_vectors
                .entry(output_location.segment_id)
                .or_default()
                .mark_deleted(output_location.row_offset);
        }
    }

    if !successful_pairs.is_empty() {
        let persisted = successful_pairs
            .iter()
            .map(|(key, location)| Ok((key.clone(), tablet.encode_row_location(*location)?)))
            .collect::<Result<Vec<_>>>()?;
        tablet.persist_primary_index_upserts(&persisted)?;
    }

    persist_delete_vectors(output, delete_vectors)
}

fn persist_delete_vectors(
    output: &RowsetSharedPtr,
    delete_vectors: HashMap<u32, DeleteVector>,
) -> Result<()> {
    for (segment_id, delete_vector) in delete_vectors {
        if delete_vector.cardinality() == 0 {
            continue;
        }
        let path = delete_vector.save_to_dir(output.rowset_path(), segment_id)?;
        output.add_delete_stats(1, delete_vector.cardinality());
        if !path.exists() {
            return Err(paro_error::io_error(
                "failed to persist compaction publish delete vector",
            ));
        }
    }
    Ok(())
}
