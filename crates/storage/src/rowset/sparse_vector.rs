// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Sparse Vector Storage
//!
//! Rowset-level storage for sparse vectors.
//! Stores dimension IDs in sorted order and compresses them using delta-varint encoding.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use std::fs;
use std::path::Path;

/// Dimension id type for sparse vectors.
pub type DimensionId = u32;

/// Weight type for sparse vectors.
pub type DimWeight = f32;

const MAGIC: &[u8; 4] = b"SPV1";
const ROW_IMAGE_MAGIC: &[u8; 4] = b"SVR1";
const ROW_IMAGE_HEADER_LEN: usize = 8;
const ROW_IMAGE_ENTRY_LEN: usize =
    std::mem::size_of::<DimensionId>() + std::mem::size_of::<DimWeight>();

/// Sparse vector (dimension IDs + weights).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SparseVector {
    pub dims: Vec<DimensionId>,
    pub weights: Vec<DimWeight>,
}

impl SparseVector {
    pub fn new(dims: Vec<DimensionId>, weights: Vec<DimWeight>) -> Result<Self> {
        if dims.len() != weights.len() {
            return Err(paro_error::invalid_input(format!(
                "SparseVector: dims/weights length mismatch ({} vs {})",
                dims.len(),
                weights.len()
            )));
        }
        Ok(Self { dims, weights })
    }

    pub fn len(&self) -> usize {
        self.dims.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dims.is_empty()
    }

    /// Returns true if dimension IDs are strictly increasing.
    pub fn is_sorted(&self) -> bool {
        self.dims.windows(2).all(|w| w[0] < w[1])
    }

    /// Sort by dimension IDs and validate uniqueness.
    pub fn sort_by_dim(&mut self) -> Result<()> {
        if self.dims.len() != self.weights.len() {
            return Err(paro_error::invalid_input(format!(
                "SparseVector: dims/weights length mismatch ({} vs {})",
                self.dims.len(),
                self.weights.len()
            )));
        }

        if self.dims.len() <= 1 {
            return Ok(());
        }

        if self.dims.len() <= 64 {
            for idx in 1..self.dims.len() {
                let dim = self.dims[idx];
                let weight = self.weights[idx];
                let mut insert_at = idx;
                while insert_at > 0 && self.dims[insert_at - 1] > dim {
                    self.dims[insert_at] = self.dims[insert_at - 1];
                    self.weights[insert_at] = self.weights[insert_at - 1];
                    insert_at -= 1;
                }
                self.dims[insert_at] = dim;
                self.weights[insert_at] = weight;
            }
            if self.dims.windows(2).any(|w| w[0] == w[1]) {
                return Err(paro_error::invalid_input(
                    "SparseVector: duplicate dimension IDs",
                ));
            }
            return Ok(());
        }

        let mut pairs: Vec<(DimensionId, DimWeight)> = self
            .dims
            .iter()
            .copied()
            .zip(self.weights.iter().copied())
            .collect();
        pairs.sort_unstable_by_key(|(dim, _)| *dim);

        if pairs.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(paro_error::invalid_input(
                "SparseVector: duplicate dimension IDs",
            ));
        }

        self.dims.clear();
        self.weights.clear();
        self.dims.extend(pairs.iter().map(|(d, _)| *d));
        self.weights.extend(pairs.iter().map(|(_, w)| *w));
        Ok(())
    }

    /// Ensure the vector is sorted and has unique dimension IDs.
    pub fn ensure_sorted(&self) -> Result<()> {
        if !self.is_sorted() {
            return Err(paro_error::invalid_input(
                "SparseVector: dimension IDs must be sorted and unique",
            ));
        }
        Ok(())
    }

    /// Dot product between two sparse vectors.
    /// Returns None if there is no overlap.
    pub fn dot(&self, other: &SparseVector) -> Option<f32> {
        debug_assert!(self.is_sorted());
        debug_assert!(other.is_sorted());
        let mut i = 0usize;
        let mut j = 0usize;
        let mut score = 0.0f32;
        let mut overlap = false;
        while i < self.dims.len() && j < other.dims.len() {
            match self.dims[i].cmp(&other.dims[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    overlap = true;
                    score += self.weights[i] * other.weights[j];
                    i += 1;
                    j += 1;
                }
            }
        }
        if overlap {
            Some(score)
        } else {
            None
        }
    }

    /// Parse sparse vector from string format: "{dim:weight, dim:weight, ...}"
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if !s.starts_with('{') || !s.ends_with('}') {
            return Err(paro_error::invalid_input(format!(
                "SparseVector: invalid format (expected {{dim:weight, ...}}): {}",
                s
            )));
        }
        let inner = &s[1..s.len() - 1].trim();
        if inner.is_empty() {
            return Ok(Self::default());
        }

        let mut dims = Vec::new();
        let mut weights = Vec::new();
        for part in inner.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let mut kv = part.split(':');
            let k = kv
                .next()
                .ok_or_else(|| paro_error::invalid_input("missing dimension id"))?
                .trim();
            let v = kv
                .next()
                .ok_or_else(|| paro_error::invalid_input("missing weight"))?
                .trim();

            let dim: u32 = k
                .parse()
                .map_err(|_| paro_error::invalid_input(format!("invalid dim id: {}", k)))?;
            let weight: f32 = v
                .parse()
                .map_err(|_| paro_error::invalid_input(format!("invalid weight: {}", v)))?;
            dims.push(dim);
            weights.push(weight);
        }

        let mut vec = Self::new(dims, weights)?;
        vec.sort_by_dim()?;
        Ok(vec)
    }

    /// Encode this vector as a typed sparse row image.
    ///
    /// Layout: `SVR1` magic, `u32` nnz, then sorted `(u32 dim, f32 weight)` pairs.
    pub fn to_row_image_v1(&self) -> Result<Vec<u8>> {
        let mut vector = self.clone();
        vector.sort_by_dim()?;
        let len = vector.len();
        let byte_len = ROW_IMAGE_HEADER_LEN
            .checked_add(len.checked_mul(ROW_IMAGE_ENTRY_LEN).ok_or_else(|| {
                paro_error::out_of_range("sparse row image entry length overflow")
            })?)
            .ok_or_else(|| paro_error::out_of_range("sparse row image length overflow"))?;
        let mut out = Vec::with_capacity(byte_len);
        out.extend_from_slice(ROW_IMAGE_MAGIC);
        out.extend_from_slice(
            &u32::try_from(len)
                .map_err(|_| paro_error::out_of_range("sparse row image nnz exceeds u32"))?
                .to_le_bytes(),
        );
        for (dim, weight) in vector.dims.iter().zip(vector.weights.iter()) {
            out.extend_from_slice(&dim.to_le_bytes());
            out.extend_from_slice(&weight.to_le_bytes());
        }
        Ok(out)
    }

    pub fn from_row_image_v1(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < ROW_IMAGE_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "sparse row image shorter than header",
            ));
        }
        if &bytes[..4] != ROW_IMAGE_MAGIC {
            return Err(paro_error::data_corrupted(
                "sparse row image has invalid magic",
            ));
        }
        let nnz = u32::from_le_bytes(bytes[4..8].try_into().expect("nnz header")) as usize;
        let expected_len = ROW_IMAGE_HEADER_LEN
            .checked_add(nnz.checked_mul(ROW_IMAGE_ENTRY_LEN).ok_or_else(|| {
                paro_error::data_corrupted("sparse row image entry length overflow")
            })?)
            .ok_or_else(|| paro_error::data_corrupted("sparse row image length overflow"))?;
        if bytes.len() != expected_len {
            return Err(paro_error::data_corrupted(format!(
                "sparse row image length mismatch: expected {}, got {}",
                expected_len,
                bytes.len()
            )));
        }

        let mut dims = Vec::with_capacity(nnz);
        let mut weights = Vec::with_capacity(nnz);
        let mut offset = ROW_IMAGE_HEADER_LEN;
        for _ in 0..nnz {
            let dim = u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("dim"));
            offset += 4;
            let weight = f32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("weight"));
            offset += 4;
            dims.push(dim);
            weights.push(weight);
        }
        let vector = Self::new(dims, weights)?;
        vector.ensure_sorted()?;
        Ok(vector)
    }
}

/// Column file for sparse vectors.
///
/// Layout:
/// - elem_offsets: prefix sum of element counts (len = num_vectors + 1)
/// - dim_offsets: prefix sum of encoded dim bytes (len = num_vectors + 1)
/// - dim_data: delta-varint encoded dimension IDs
/// - weights: flat f32 array aligned with element offsets
#[derive(Debug, Clone, Default)]
pub struct SparseVectorColumnFile {
    elem_offsets: Vec<u32>,
    dim_offsets: Vec<u32>,
    dim_data: Vec<u8>,
    weights: Vec<DimWeight>,
}

impl SparseVectorColumnFile {
    pub fn new() -> Self {
        Self {
            elem_offsets: vec![0],
            dim_offsets: vec![0],
            dim_data: Vec::new(),
            weights: Vec::new(),
        }
    }

    pub fn num_vectors(&self) -> usize {
        self.elem_offsets.len().saturating_sub(1)
    }

    pub fn num_elements(&self) -> usize {
        self.weights.len()
    }

    pub fn is_empty(&self) -> bool {
        self.num_vectors() == 0
    }

    /// Append a sparse vector (will be sorted if needed).
    pub fn append(&mut self, vector: &SparseVector) -> Result<()> {
        let mut vec = vector.clone();
        vec.sort_by_dim()?;
        self.append_sorted(&vec)
    }

    /// Append a sparse vector that is already sorted by dimension IDs.
    pub fn append_sorted(&mut self, vector: &SparseVector) -> Result<()> {
        vector.ensure_sorted()?;

        if vector.dims.len() != vector.weights.len() {
            return Err(paro_error::invalid_input(
                "SparseVectorColumnFile: dims/weights length mismatch",
            ));
        }

        let start_elem = *self.elem_offsets.last().unwrap();
        let start_dim = *self.dim_offsets.last().unwrap();

        let mut encoded_dims = Vec::with_capacity(vector.dims.len() * 2);
        encode_dims(&vector.dims, &mut encoded_dims)?;

        self.dim_data.extend_from_slice(&encoded_dims);
        self.weights.extend_from_slice(&vector.weights);

        self.elem_offsets
            .push(start_elem + vector.dims.len() as u32);
        self.dim_offsets.push(start_dim + encoded_dims.len() as u32);

        Ok(())
    }

    /// Get a sparse vector by index.
    pub fn get(&self, index: usize) -> Result<SparseVector> {
        if index >= self.num_vectors() {
            return Err(paro_error::out_of_range(format!(
                "SparseVectorColumnFile: index {} out of range (num_vectors={})",
                index,
                self.num_vectors()
            )));
        }

        let elem_start = self.elem_offsets[index] as usize;
        let elem_end = self.elem_offsets[index + 1] as usize;
        let dim_start = self.dim_offsets[index] as usize;
        let dim_end = self.dim_offsets[index + 1] as usize;

        if elem_end < elem_start || dim_end < dim_start {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: invalid offsets",
            ));
        }

        let count = elem_end - elem_start;
        let dims_slice = &self.dim_data[dim_start..dim_end];
        let (dims, used) = decode_dims(dims_slice, count)?;
        if used != dims_slice.len() {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: dim data length mismatch",
            ));
        }

        let weights = self.weights[elem_start..elem_end].to_vec();
        Ok(SparseVector { dims, weights })
    }

    /// Iterate over sparse vectors in the column file.
    pub fn iter(&self) -> SparseVectorColumnIter<'_> {
        SparseVectorColumnIter {
            file: self,
            index: 0,
        }
    }

    /// Serialize column file to bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        if self.elem_offsets.is_empty() || self.dim_offsets.is_empty() {
            return Err(paro_error::invalid_input(
                "SparseVectorColumnFile: offsets cannot be empty",
            ));
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(MAGIC);
        buf.put_u32_le(self.num_vectors() as u32);
        buf.put_u32_le(self.elem_offsets.len() as u32);
        for off in &self.elem_offsets {
            buf.put_u32_le(*off);
        }
        buf.put_u32_le(self.dim_offsets.len() as u32);
        for off in &self.dim_offsets {
            buf.put_u32_le(*off);
        }
        buf.put_u32_le(self.dim_data.len() as u32);
        buf.extend_from_slice(&self.dim_data);
        buf.put_u32_le(self.weights.len() as u32);
        for w in &self.weights {
            buf.put_f32_le(*w);
        }
        Ok(buf.freeze())
    }

    /// Deserialize column file from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut buf = data;
        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: data too small",
            ));
        }

        let mut magic = [0u8; 4];
        buf.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: invalid magic",
            ));
        }

        if buf.remaining() < 8 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated header",
            ));
        }

        let num_vectors = buf.get_u32_le() as usize;
        let elem_offsets_len = buf.get_u32_le() as usize;

        if elem_offsets_len == 0 || elem_offsets_len != num_vectors + 1 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: invalid elem_offsets length",
            ));
        }

        if buf.remaining() < elem_offsets_len * 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated elem_offsets",
            ));
        }
        let mut elem_offsets = Vec::with_capacity(elem_offsets_len);
        for _ in 0..elem_offsets_len {
            elem_offsets.push(buf.get_u32_le());
        }

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated dim_offsets length",
            ));
        }
        let dim_offsets_len = buf.get_u32_le() as usize;
        if dim_offsets_len == 0 || dim_offsets_len != num_vectors + 1 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: invalid dim_offsets length",
            ));
        }
        if buf.remaining() < dim_offsets_len * 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated dim_offsets",
            ));
        }
        let mut dim_offsets = Vec::with_capacity(dim_offsets_len);
        for _ in 0..dim_offsets_len {
            dim_offsets.push(buf.get_u32_le());
        }

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated dim_data length",
            ));
        }
        let dim_data_len = buf.get_u32_le() as usize;
        if buf.remaining() < dim_data_len {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated dim_data",
            ));
        }
        let dim_data = buf[..dim_data_len].to_vec();
        buf.advance(dim_data_len);

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated weights length",
            ));
        }
        let weights_len = buf.get_u32_le() as usize;
        if buf.remaining() < weights_len * 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: truncated weights",
            ));
        }
        let mut weights = Vec::with_capacity(weights_len);
        for _ in 0..weights_len {
            weights.push(buf.get_f32_le());
        }

        if *elem_offsets.last().unwrap() as usize != weights.len() {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: elem_offsets/weights mismatch",
            ));
        }
        if *dim_offsets.last().unwrap() as usize != dim_data.len() {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: dim_offsets/dim_data mismatch",
            ));
        }

        Ok(Self {
            elem_offsets,
            dim_offsets,
            dim_data,
            weights,
        })
    }

    /// Save to the specified path.
    pub fn save_to_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let bytes = self.to_bytes()?;
        fs::write(&path, bytes).map_err(|e| {
            paro_error::io_error(format!(
                "write sparse vector file {:?}: {}",
                path.as_ref(),
                e
            ))
        })?;
        Ok(())
    }

    /// Load from the specified path.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let data = fs::read(&path).map_err(|e| {
            paro_error::io_error(format!(
                "read sparse vector file {:?}: {}",
                path.as_ref(),
                e
            ))
        })?;
        Self::from_bytes(&data)
    }
}

pub struct SparseVectorColumnIter<'a> {
    file: &'a SparseVectorColumnFile,
    index: usize,
}

impl<'a> Iterator for SparseVectorColumnIter<'a> {
    type Item = Result<SparseVector>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.file.num_vectors() {
            return None;
        }
        let idx = self.index;
        self.index += 1;
        Some(self.file.get(idx))
    }
}

fn encode_dims(dims: &[DimensionId], out: &mut Vec<u8>) -> Result<()> {
    let mut prev = 0u32;
    for (i, &dim) in dims.iter().enumerate() {
        if i > 0 && dim <= prev {
            return Err(paro_error::invalid_input(
                "SparseVector: dimension IDs must be sorted and unique",
            ));
        }
        let delta = if i == 0 { dim } else { dim - prev };
        encode_varint(delta, out);
        prev = dim;
    }
    Ok(())
}

fn decode_dims(data: &[u8], count: usize) -> Result<(Vec<DimensionId>, usize)> {
    let mut dims = Vec::with_capacity(count);
    let mut offset = 0usize;
    let mut prev = 0u32;
    for i in 0..count {
        let delta = decode_varint(data, &mut offset)?;
        let dim = if i == 0 { delta } else { prev + delta };
        if i > 0 && dim <= prev {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: non-increasing dimension IDs",
            ));
        }
        dims.push(dim);
        prev = dim;
    }
    Ok((dims, offset))
}

fn encode_varint(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(data: &[u8], offset: &mut usize) -> Result<u32> {
    let mut shift = 0u32;
    let mut result = 0u32;
    loop {
        if *offset >= data.len() {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: unexpected end of varint",
            ));
        }
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift > 28 {
            return Err(paro_error::data_corrupted(
                "SparseVectorColumnFile: varint overflow",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_vector_column_roundtrip() {
        let mut file = SparseVectorColumnFile::new();
        let v0 = SparseVector::new(vec![3, 1], vec![0.5, 1.0]).unwrap();
        let v1 = SparseVector::new(vec![10], vec![2.0]).unwrap();
        file.append(&v0).unwrap();
        file.append(&v1).unwrap();

        assert_eq!(file.num_vectors(), 2);
        let got0 = file.get(0).unwrap();
        assert_eq!(got0.dims, vec![1, 3]);
        assert_eq!(got0.weights, vec![1.0, 0.5]);

        let bytes = file.to_bytes().unwrap();
        let restored = SparseVectorColumnFile::from_bytes(&bytes).unwrap();
        let got1 = restored.get(1).unwrap();
        assert_eq!(got1.dims, vec![10]);
        assert_eq!(got1.weights, vec![2.0]);
    }

    #[test]
    fn test_sparse_vector_row_image_v1_roundtrip() {
        let vector = SparseVector::new(vec![3, 1], vec![0.5, 1.0]).unwrap();
        let bytes = vector.to_row_image_v1().unwrap();
        let restored = SparseVector::from_row_image_v1(&bytes).unwrap();
        assert_eq!(restored.dims, vec![1, 3]);
        assert_eq!(restored.weights, vec![1.0, 0.5]);
    }
}
