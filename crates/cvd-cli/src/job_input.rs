//! `job.json` 反序列化 → [`cvd_proto::cvdbench::BenchSpec`]。
//!
//! 借助 cvd-proto 上 `#[derive(::serde::Deserialize)]` 的全局加成，job 模板
//! 直接 deserialize 成 proto 类型，无需再写一份手动镜像结构。

use std::path::Path;

use cvd_proto::cvdbench as pb;
use serde::Deserialize;

/// spec.md §3 的顶层包裹：`{ "cvdbench": { ... } }`。
#[derive(Deserialize)]
struct JobFile {
    cvdbench: pb::BenchSpec,
}

/// 从给定路径加载 job 配置。
pub fn load_from_path(path: &Path) -> anyhow::Result<pb::BenchSpec> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
    let job: JobFile = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parse {} as job.json: {e}", path.display()))?;
    Ok(job.cvdbench)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
{
  "cvdbench": {
    "fs_name": "examplefs",
    "target_workers": 2,
    "io_mode": "seq",
    "io_aligned": true,
    "direct_io": false,
    "block_size": "1Mi",
    "duration": "200ms",
    "warmup": "",
    "read": {
      "concurrency": 2,
      "file_manifest": "manifests/r.csv",
      "dir_manifest": "",
      "think_time": "",
      "rate_limit": "1GB/s",
      "loop_files": false
    }
  }
}"#;

    #[test]
    fn parses_sample_job_file() {
        let spec: pb::BenchSpec = serde_json::from_str::<JobFile>(SAMPLE).unwrap().cvdbench;
        assert_eq!(spec.fs_name, "examplefs");
        assert_eq!(spec.target_workers, 2);
        assert_eq!(spec.block_size, "1Mi");
        let read = spec.read.as_ref().unwrap();
        assert_eq!(read.concurrency, 2);
        assert_eq!(read.rate_limit, "1GB/s");
    }
}
