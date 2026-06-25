//! HDR histogram V2 compressed bytes 编解码与合并工具。
//!
//! 用于 worker 端把 [`hdrhistogram::Histogram`] 序列化进
//! [`pb::PerformanceMetrics::latency_histogram_hdr`] 字段，master 端在终态
//! 聚合时再 decode + merge 重算分位数（spec §4.2）。

use std::io::Cursor;

use hdrhistogram::serialization::{Deserializer, Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HistogramError {
    #[error("serialize: {0}")]
    Serialize(String),
    #[error("deserialize: {0}")]
    Deserialize(String),
}

/// 序列化为 V2 deflate 压缩格式（最常见的 HDR histogram on-the-wire 形态）。
pub fn encode_v2_compressed(h: &Histogram<u64>) -> Result<Vec<u8>, HistogramError> {
    let mut buf = Vec::new();
    let mut serializer = V2DeflateSerializer::new();
    serializer
        .serialize(h, &mut buf)
        .map_err(|e| HistogramError::Serialize(e.to_string()))?;
    Ok(buf)
}

/// 反序列化 V2 deflate 格式（自动检测 V2 / V2-deflate / V1）。
pub fn decode(bytes: &[u8]) -> Result<Histogram<u64>, HistogramError> {
    let mut deser = Deserializer::new();
    let mut cursor = Cursor::new(bytes);
    deser
        .deserialize(&mut cursor)
        .map_err(|e| HistogramError::Deserialize(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hist() -> Histogram<u64> {
        let mut h = Histogram::<u64>::new_with_max(60_000_000, 3).unwrap();
        for v in [10u64, 20, 30, 100, 200, 500, 1_000] {
            h.record(v).unwrap();
        }
        h
    }

    #[test]
    fn round_trip_preserves_quantiles() {
        let h = make_hist();
        let bytes = encode_v2_compressed(&h).unwrap();
        assert!(!bytes.is_empty());
        let h2 = decode(&bytes).unwrap();
        assert_eq!(h.value_at_quantile(0.5), h2.value_at_quantile(0.5));
        assert_eq!(h.value_at_quantile(0.99), h2.value_at_quantile(0.99));
        assert_eq!(h.len(), h2.len());
        assert_eq!(h.max(), h2.max());
    }

    #[test]
    fn merging_after_decode_is_associative() {
        let h1 = make_hist();
        let h2 = make_hist();
        let bytes1 = encode_v2_compressed(&h1).unwrap();
        let bytes2 = encode_v2_compressed(&h2).unwrap();
        let mut merged = decode(&bytes1).unwrap();
        merged.add(decode(&bytes2).unwrap()).unwrap();
        // 合并两份相同 hist：count 翻倍，p50 不变
        assert_eq!(merged.len(), h1.len() * 2);
        assert_eq!(merged.value_at_quantile(0.5), h1.value_at_quantile(0.5));
    }

    #[test]
    fn decode_rejects_garbage() {
        let err = decode(&[1, 2, 3, 4]).unwrap_err();
        match err {
            HistogramError::Deserialize(_) => {}
            HistogramError::Serialize(_) => panic!("unexpected variant"),
        }
    }
}
