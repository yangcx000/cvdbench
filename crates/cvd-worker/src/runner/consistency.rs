//! FS vs S3 SDK 一致性比对（spec §6.10）。
//!
//! 工作模式：
//! - master 在 CreateJob 时把 `s3_consistency_check` 凭据从存储态脱敏，明文随
//!   `FetchJobResponse.s3_credentials` 单独下发；spec、JobEvent、QueryJob、
//!   结果文件都看不到明文（spec §9.5）。
//! - worker 端 read runner 收到一致性配置后构造 [`ConsistencyClient`]，每读完
//!   一个文件即 `check()`：S3 GetObject → 比 size → 比 sha256；首条不一致即
//!   `ConsistencyError` 返回，runner fail-fast 终止 worker（spec §6.10 / §6.7）。
//!
//! 错误分类按 HTTP status 落到 [`pb::ConsistencyErrorType`]：
//! - `404` / NoSuchKey → `CET_S3_NOT_FOUND`
//! - `403` → `CET_PERMISSION_DENIED`
//! - `429` / `503` → `CET_S3_THROTTLE`
//! - 其他 / Timeout / Dispatch / Construction → `CET_S3_READ_ERROR`
//! - FS 读取错误（如调用方未读到完整内容）→ `CET_FS_READ_ERROR`

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::Client;
use cvd_proto::cvdbench as pb;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("missing S3 credentials in FetchJobResponse")]
    MissingCredentials,
    #[error("region must be non-empty")]
    EmptyRegion,
    #[error("bucket_name must be non-empty")]
    EmptyBucket,
}

#[derive(Debug, Clone)]
pub struct ConsistencyClient {
    s3: Client,
    bucket: String,
    /// 当 `FileEntry.s3_key` 为空时拼到 fs_path 前面的前缀（spec §9.1）。
    prefix: String,
    /// worker_id；写到 ConsistencyError 里方便事后定位。
    worker_id: String,
}

impl ConsistencyClient {
    pub fn build(
        cfg: &pb::ConsistencyConfig,
        creds: &pb::S3CredentialMaterial,
        worker_id: &str,
    ) -> Result<Self, BuildError> {
        if cfg.bucket_name.is_empty() {
            return Err(BuildError::EmptyBucket);
        }
        if cfg.region.is_empty() {
            return Err(BuildError::EmptyRegion);
        }
        if creds.access_key.is_empty() || creds.secret_key.is_empty() {
            return Err(BuildError::MissingCredentials);
        }

        let session_token = if creds.session_token.is_empty() {
            None
        } else {
            Some(creds.session_token.clone())
        };
        let credentials = Credentials::new(
            creds.access_key.clone(),
            creds.secret_key.clone(),
            session_token,
            None,
            "cvdbench",
        );
        let mut builder = aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials)
            .region(Region::new(cfg.region.clone()))
            .behavior_version(BehaviorVersion::latest());
        if !cfg.bucket_url.is_empty() {
            // 兼容 OSS / MinIO 等 S3-compat 存储
            builder = builder.endpoint_url(&cfg.bucket_url).force_path_style(true);
        }
        let client = Client::from_conf(builder.build());
        Ok(Self {
            s3: client,
            bucket: cfg.bucket_name.clone(),
            prefix: cfg.prefix.clone(),
            worker_id: worker_id.to_owned(),
        })
    }

    /// `s3_key` 优先；否则 `prefix + fs_path`（spec §9.1）。
    pub fn resolve_key(&self, file: &pb::FileEntry) -> String {
        if !file.s3_key.is_empty() {
            file.s3_key.clone()
        } else {
            format!("{}{}", self.prefix, file.fs_path)
        }
    }

    /// 比对 FS 内容与 S3 对象。`fs_hash` 由调用方在 FS 读取时增量累加得到，
    /// `fs_size` 是 FS 端实际读到的总字节数。
    ///
    /// 成功返回 `Ok(s3_size)`（用于 metrics）；任何不一致返回 `Err(ConsistencyError)`。
    /// S3 body 用流式增量读取 + 增量 sha256，避免 GB 级对象 OOM（spec §6.10）。
    pub async fn check(
        &self,
        file: &pb::FileEntry,
        fs_size: u64,
        fs_hash: [u8; 32],
    ) -> Result<u64, pb::ConsistencyError> {
        let key = self.resolve_key(file);
        let resp = match self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(o) => o,
            Err(err) => {
                let kind = classify_get_object_error(&err);
                return Err(self.error(file, &key, kind, &format!("{err}")));
            }
        };

        // 流式累加 sha256 + size；每次 next() 只持有当前 chunk
        let mut s3_hasher = Sha256::new();
        let mut s3_size: u64 = 0;
        let mut body = resp.body;
        loop {
            match body.next().await {
                Some(Ok(bytes)) => {
                    s3_hasher.update(&bytes);
                    s3_size = s3_size.saturating_add(bytes.len() as u64);
                }
                Some(Err(e)) => {
                    return Err(self.error(
                        file,
                        &key,
                        pb::ConsistencyErrorType::CetS3ReadError,
                        &format!("body stream: {e}"),
                    ));
                }
                None => break,
            }
        }

        if s3_size != fs_size {
            return Err(self.error(
                file,
                &key,
                pb::ConsistencyErrorType::CetSizeMismatch,
                &format!("fs={fs_size} s3={s3_size}"),
            ));
        }

        let s3_hash: [u8; 32] = s3_hasher.finalize().into();
        if s3_hash != fs_hash {
            return Err(self.error(
                file,
                &key,
                pb::ConsistencyErrorType::CetHashMismatch,
                "sha256 mismatch",
            ));
        }
        Ok(s3_size)
    }

    fn error(
        &self,
        file: &pb::FileEntry,
        key: &str,
        kind: pb::ConsistencyErrorType,
        message: &str,
    ) -> pb::ConsistencyError {
        pb::ConsistencyError {
            worker_id: self.worker_id.clone(),
            fs_path: file.fs_path.clone(),
            s3_key: key.to_owned(),
            r#type: kind.into(),
            message: message.to_owned(),
        }
    }
}

/// 从 SDK 错误推断 [`pb::ConsistencyErrorType`]。优先级：
/// 1. ServiceError + GetObjectError::NoSuchKey → 404；
/// 2. ServiceError + ProvideErrorMetadata::code() → AccessDenied / SlowDown / ...；
/// 3. 其它（Timeout / Dispatch / Construction / Response）→ S3_READ_ERROR。
fn classify_get_object_error<R>(err: &SdkError<GetObjectError, R>) -> pb::ConsistencyErrorType {
    let SdkError::ServiceError(svc_err) = err else {
        return pb::ConsistencyErrorType::CetS3ReadError;
    };
    let inner = svc_err.err();
    if matches!(inner, GetObjectError::NoSuchKey(_)) {
        return pb::ConsistencyErrorType::CetS3NotFound;
    }
    match inner.code() {
        Some("NoSuchKey") => pb::ConsistencyErrorType::CetS3NotFound,
        Some("AccessDenied") => pb::ConsistencyErrorType::CetPermissionDenied,
        Some("SlowDown") | Some("RequestThrottled") | Some("ServiceUnavailable") => {
            pb::ConsistencyErrorType::CetS3Throttle
        }
        _ => pb::ConsistencyErrorType::CetS3ReadError,
    }
}

/// 给调用方的工具：增量哈希 + 读取累计大小。
pub struct StreamingHash {
    hasher: Sha256,
    size: u64,
}

impl StreamingHash {
    #[must_use]
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            size: 0,
        }
    }
    pub fn update(&mut self, chunk: &[u8]) {
        self.hasher.update(chunk);
        self.size = self.size.saturating_add(chunk.len() as u64);
    }
    #[must_use]
    pub fn finalize(self) -> ([u8; 32], u64) {
        (self.hasher.finalize().into(), self.size)
    }
}

impl Default for StreamingHash {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good_cfg() -> pb::ConsistencyConfig {
        pb::ConsistencyConfig {
            bucket_name: "bench".into(),
            bucket_url: "http://localhost:9000".into(),
            access_key: "***".into(), // master 已脱敏
            secret_key: "***".into(),
            region: "us-east-1".into(),
            prefix: "data/".into(),
            session_token: String::new(),
        }
    }

    fn good_creds() -> pb::S3CredentialMaterial {
        pb::S3CredentialMaterial {
            access_key: "EXAMPLE_ACCESS_KEY".into(),
            secret_key: "secret".into(),
            session_token: String::new(),
        }
    }

    #[test]
    fn build_rejects_missing_credentials() {
        let mut creds = good_creds();
        creds.access_key = String::new();
        let err = ConsistencyClient::build(&good_cfg(), &creds, "w1").unwrap_err();
        assert!(matches!(err, BuildError::MissingCredentials));
    }

    #[test]
    fn build_rejects_empty_bucket() {
        let mut cfg = good_cfg();
        cfg.bucket_name = String::new();
        let err = ConsistencyClient::build(&cfg, &good_creds(), "w1").unwrap_err();
        assert!(matches!(err, BuildError::EmptyBucket));
    }

    #[test]
    fn build_rejects_empty_region() {
        let mut cfg = good_cfg();
        cfg.region = String::new();
        let err = ConsistencyClient::build(&cfg, &good_creds(), "w1").unwrap_err();
        assert!(matches!(err, BuildError::EmptyRegion));
    }

    #[test]
    fn build_succeeds_with_endpoint_url() {
        let c = ConsistencyClient::build(&good_cfg(), &good_creds(), "w1").unwrap();
        assert_eq!(c.bucket, "bench");
        assert_eq!(c.prefix, "data/");
    }

    #[test]
    fn resolve_key_uses_s3_key_when_provided() {
        let c = ConsistencyClient::build(&good_cfg(), &good_creds(), "w1").unwrap();
        let key = c.resolve_key(&pb::FileEntry {
            fs_path: "x/y.dat".into(),
            s3_key: "explicit/y.dat".into(),
        });
        assert_eq!(key, "explicit/y.dat");
    }

    #[test]
    fn resolve_key_concatenates_prefix_when_s3_key_empty() {
        let c = ConsistencyClient::build(&good_cfg(), &good_creds(), "w1").unwrap();
        let key = c.resolve_key(&pb::FileEntry {
            fs_path: "x/y.dat".into(),
            s3_key: String::new(),
        });
        assert_eq!(key, "data/x/y.dat");
    }

    #[test]
    fn streaming_hash_matches_one_shot() {
        let payload = b"hello world this is a test payload";
        let mut sh = StreamingHash::new();
        for chunk in payload.chunks(7) {
            sh.update(chunk);
        }
        let (h, size) = sh.finalize();
        let mut once = Sha256::new();
        once.update(payload);
        let h2: [u8; 32] = once.finalize().into();
        assert_eq!(h, h2);
        assert_eq!(size as usize, payload.len());
    }
}
