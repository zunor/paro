//! # BM25 Scoring
//!
//! Implements BM25 scoring with configurable parameters.

/// BM25 parameters.
#[derive(Debug, Clone, Copy)]
pub struct Bm25 {
    /// Term frequency saturation parameter.
    pub k1: f32,
    /// Document length normalization parameter.
    pub b: f32,
}

impl Default for Bm25 {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

impl Bm25 {
    pub fn new(k1: f32, b: f32) -> Self {
        Self { k1, b }
    }

    /// Compute IDF for a term.
    #[inline]
    pub fn idf(total_docs: f32, doc_freq: f32) -> f32 {
        if doc_freq <= 0.0 || total_docs <= 0.0 {
            return 0.0;
        }
        ((total_docs - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln()
    }

    /// Compute BM25 score contribution for a term.
    #[inline]
    pub fn score(&self, tf: f32, doc_len: f32, avgdl: f32, doc_freq: f32, total_docs: f32) -> f32 {
        if tf <= 0.0 || doc_len <= 0.0 || avgdl <= 0.0 {
            return 0.0;
        }
        let idf = Self::idf(total_docs, doc_freq);
        if idf == 0.0 {
            return 0.0;
        }
        let denom = tf + self.k1 * (1.0 - self.b + self.b * (doc_len / avgdl));
        if denom == 0.0 {
            return 0.0;
        }
        idf * (tf * (self.k1 + 1.0) / denom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_idf_monotonic() {
        let idf_rare = Bm25::idf(100.0, 1.0);
        let idf_common = Bm25::idf(100.0, 50.0);
        assert!(idf_rare > idf_common);
    }

    #[test]
    fn bm25_score_tf_increases() {
        let bm25 = Bm25::default();
        let score_low = bm25.score(1.0, 10.0, 10.0, 5.0, 100.0);
        let score_high = bm25.score(3.0, 10.0, 10.0, 5.0, 100.0);
        assert!(score_high > score_low);
    }
}
