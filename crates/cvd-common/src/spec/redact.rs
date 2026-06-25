//! Spec §9.5 凭据脱敏：`access_key` / `secret_key` / `session_token` → `"***"`。
//!
//! 工作流程：
//! 1. master 在 `CreateJob` 同步阶段调用 [`extract_credentials`] 把凭据抽出，
//!    与 `Job` 分开存储；
//! 2. 紧接着调用 [`redact_in_place`]，把 `pb::BenchSpec` 内的明文换成 `"***"`，
//!    再把脱敏后的 spec 落到 `Job.spec`；
//! 3. `FetchJobResponse.spec` 复用脱敏副本；明文凭据走 `s3_credentials` 通道，
//!    且仅下发给被分配到该 job 的 worker。
//!
//! 不变式：脱敏函数不会改写 `bucket_name` / `bucket_url` / `region` / `prefix`，
//! 这些字段只是 S3 客户端连接参数，没有保密属性。

use cvd_proto::cvdbench as pb;

/// 脱敏占位符。Spec §9.5 / §9.3 强制此字面量。
pub const REDACTED: &str = "***";

/// 抽取一致性测试凭据，用于 `FetchJobResponse.s3_credentials`。
///
/// 在 spec 被 [`redact_in_place`] 之前调用；返回 `None` 表示无一致性测试。
#[must_use]
pub fn extract_credentials(spec: &pb::BenchSpec) -> Option<pb::S3CredentialMaterial> {
    let s3 = spec.read.as_ref()?.s3_consistency_check.as_ref()?;
    Some(pb::S3CredentialMaterial {
        access_key: s3.access_key.clone(),
        secret_key: s3.secret_key.clone(),
        session_token: s3.session_token.clone(),
    })
}

/// 原地脱敏。
///
/// 当前协议下凭据只可能出现在 `read.s3_consistency_check` 内；如未来扩展更多带
/// 凭据的字段，需要在此添加分支。
pub fn redact_in_place(spec: &mut pb::BenchSpec) {
    if let Some(read) = spec.read.as_mut() {
        if let Some(s3) = read.s3_consistency_check.as_mut() {
            s3.access_key = REDACTED.to_owned();
            s3.secret_key = REDACTED.to_owned();
            s3.session_token = REDACTED.to_owned();
        }
    }
}

/// 按值返回脱敏副本。等价于 `redact_in_place(&mut clone)` + 返回。
#[must_use]
pub fn redacted(mut spec: pb::BenchSpec) -> pb::BenchSpec {
    redact_in_place(&mut spec);
    spec
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with_credentials() -> pb::BenchSpec {
        pb::BenchSpec {
            fs_name: "examplefs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "1M".into(),
            duration: "1h".into(),
            warmup: "5m".into(),
            target_workers: 4,
            read: Some(pb::ReadConfig {
                concurrency: 4,
                file_manifest: "m.csv".into(),
                dir_manifest: String::new(),
                think_time: String::new(),
                rate_limit: String::new(),
                s3_consistency_check: Some(pb::ConsistencyConfig {
                    bucket_name: "example-bucket".into(),
                    bucket_url: "http://s3.example.com".into(),
                    access_key: "AK".into(),
                    secret_key: "SK".into(),
                    region: "us-east-1".into(),
                    prefix: "bench/".into(),
                    session_token: "TOKEN".into(),
                }),
                loop_files: false,
            }),
            write: None,
            metadata: None,
        }
    }

    #[test]
    fn extracts_credentials_before_redact() {
        let spec = spec_with_credentials();
        let creds = extract_credentials(&spec).unwrap();
        assert_eq!(creds.access_key, "AK");
        assert_eq!(creds.secret_key, "SK");
        assert_eq!(creds.session_token, "TOKEN");
    }

    #[test]
    fn extract_returns_none_when_no_consistency() {
        let mut spec = spec_with_credentials();
        spec.read.as_mut().unwrap().s3_consistency_check = None;
        assert!(extract_credentials(&spec).is_none());
    }

    #[test]
    fn redact_in_place_replaces_secrets_only() {
        let mut spec = spec_with_credentials();
        redact_in_place(&mut spec);
        let s3 = spec
            .read
            .as_ref()
            .unwrap()
            .s3_consistency_check
            .as_ref()
            .unwrap();
        assert_eq!(s3.access_key, REDACTED);
        assert_eq!(s3.secret_key, REDACTED);
        assert_eq!(s3.session_token, REDACTED);
        // 非保密字段不受影响
        assert_eq!(s3.bucket_name, "example-bucket");
        assert_eq!(s3.bucket_url, "http://s3.example.com");
        assert_eq!(s3.region, "us-east-1");
        assert_eq!(s3.prefix, "bench/");
    }

    #[test]
    fn redact_is_safe_when_no_consistency() {
        let mut spec = spec_with_credentials();
        spec.read.as_mut().unwrap().s3_consistency_check = None;
        redact_in_place(&mut spec); // 不应 panic
        assert!(spec.read.as_ref().unwrap().s3_consistency_check.is_none());
    }

    #[test]
    fn redacted_returns_independent_copy() {
        let original = spec_with_credentials();
        let copy = redacted(original.clone());
        assert_eq!(
            original
                .read
                .as_ref()
                .unwrap()
                .s3_consistency_check
                .as_ref()
                .unwrap()
                .access_key,
            "AK",
            "原 spec 不应被改写"
        );
        assert_eq!(
            copy.read
                .as_ref()
                .unwrap()
                .s3_consistency_check
                .as_ref()
                .unwrap()
                .access_key,
            REDACTED
        );
    }
}
