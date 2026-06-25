//! ConsistencyClient 与 mock S3 (wiremock) 交互验证（spec §6.10）。
//!
//! 不依赖真实 S3：用 wiremock 起一个本地 HTTP server 模拟 GetObject 路径，
//! 通过 endpoint_url 让 aws-sdk-s3 与 force_path_style=true 直接打这个端点。
//!
//! 覆盖 4 条路径：
//! 1. 200 + 内容一致 → Ok
//! 2. 200 + 内容不一致 → CET_HASH_MISMATCH
//! 3. 200 + size 不一致 → CET_SIZE_MISMATCH
//! 4. 404 → CET_S3_NOT_FOUND

use cvd_proto::cvdbench as pb;
use cvd_worker::runner::consistency::{ConsistencyClient, StreamingHash};
use sha2::Digest;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BUCKET: &str = "bench";

fn build_client(endpoint: &str) -> ConsistencyClient {
    let cfg = pb::ConsistencyConfig {
        bucket_name: BUCKET.into(),
        bucket_url: endpoint.to_owned(),
        access_key: "***".into(),
        secret_key: "***".into(),
        region: "us-east-1".into(),
        prefix: "data/".into(),
        session_token: String::new(),
    };
    let creds = pb::S3CredentialMaterial {
        access_key: "EXAMPLE_ACCESS_KEY".into(),
        secret_key: "secret".into(),
        session_token: String::new(),
    };
    ConsistencyClient::build(&cfg, &creds, "host-1-aaaa1111").unwrap()
}

fn sha256_of(bytes: &[u8]) -> [u8; 32] {
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    h.finalize().into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_returns_ok_when_content_matches() {
    let mock_server = MockServer::start().await;
    let payload = b"hello cvdbench consistency".to_vec();
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/data/x.dat")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.clone()))
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    let mut sh = StreamingHash::new();
    sh.update(&payload);
    let (fs_hash, fs_size) = sh.finalize();
    let result = client
        .check(
            &pb::FileEntry {
                fs_path: "x.dat".into(),
                s3_key: String::new(),
            },
            fs_size,
            fs_hash,
        )
        .await;
    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_mismatch_classified() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/data/x.dat")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"in-s3".to_vec()))
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    // FS 端 size 与 s3 相同（都是 5 字节）但内容不同
    let fs_payload = b"on-fs";
    let fs_hash = sha256_of(fs_payload);
    let err = client
        .check(
            &pb::FileEntry {
                fs_path: "x.dat".into(),
                s3_key: String::new(),
            },
            fs_payload.len() as u64,
            fs_hash,
        )
        .await
        .unwrap_err();
    assert_eq!(err.r#type, pb::ConsistencyErrorType::CetHashMismatch as i32);
    assert_eq!(err.fs_path, "x.dat");
    assert_eq!(err.s3_key, "data/x.dat");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn size_mismatch_classified() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/data/x.dat")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"longer-content".to_vec()))
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    let fs_payload = b"short";
    let fs_hash = sha256_of(fs_payload);
    let err = client
        .check(
            &pb::FileEntry {
                fs_path: "x.dat".into(),
                s3_key: String::new(),
            },
            fs_payload.len() as u64,
            fs_hash,
        )
        .await
        .unwrap_err();
    assert_eq!(err.r#type, pb::ConsistencyErrorType::CetSizeMismatch as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_404_classified_as_not_found() {
    let mock_server = MockServer::start().await;
    // 真实 S3 用 XML body 表达 NoSuchKey；mock 也得这么返才能让 aws-sdk-s3
    // 解析成 GetObjectError::NoSuchKey。
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>data/x.dat</Key>
  <RequestId>cvdbench-test</RequestId>
  <HostId>cvdbench-mock</HostId>
</Error>"#;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/data/x.dat")))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_string(body)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    let payload = b"anything";
    let fs_hash = sha256_of(payload);
    let err = client
        .check(
            &pb::FileEntry {
                fs_path: "x.dat".into(),
                s3_key: String::new(),
            },
            payload.len() as u64,
            fs_hash,
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.r#type,
        pb::ConsistencyErrorType::CetS3NotFound as i32,
        "expected CET_S3_NOT_FOUND, got {} (msg: {})",
        err.r#type,
        err.message,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_throttle_classified() {
    let mock_server = MockServer::start().await;
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>SlowDown</Code>
  <Message>Please reduce your request rate.</Message>
  <RequestId>cvdbench-test</RequestId>
  <HostId>cvdbench-mock</HostId>
</Error>"#;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/data/x.dat")))
        .respond_with(
            ResponseTemplate::new(503)
                .set_body_string(body)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    let payload = b"anything";
    let fs_hash = sha256_of(payload);
    let err = client
        .check(
            &pb::FileEntry {
                fs_path: "x.dat".into(),
                s3_key: String::new(),
            },
            payload.len() as u64,
            fs_hash,
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.r#type,
        pb::ConsistencyErrorType::CetS3Throttle as i32,
        "expected CET_S3_THROTTLE, got {} (msg: {})",
        err.r#type,
        err.message,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_access_denied_classified() {
    let mock_server = MockServer::start().await;
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>AccessDenied</Code>
  <Message>Access Denied</Message>
  <RequestId>cvdbench-test</RequestId>
  <HostId>cvdbench-mock</HostId>
</Error>"#;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/data/x.dat")))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_string(body)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    let payload = b"anything";
    let fs_hash = sha256_of(payload);
    let err = client
        .check(
            &pb::FileEntry {
                fs_path: "x.dat".into(),
                s3_key: String::new(),
            },
            payload.len() as u64,
            fs_hash,
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.r#type,
        pb::ConsistencyErrorType::CetPermissionDenied as i32,
        "expected CET_PERMISSION_DENIED, got {} (msg: {})",
        err.r#type,
        err.message,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_s3_key_overrides_prefix() {
    let mock_server = MockServer::start().await;
    let payload = b"explicit-key-content".to_vec();
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/explicit/key.dat")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.clone()))
        .mount(&mock_server)
        .await;

    let client = build_client(&mock_server.uri());
    let mut sh = StreamingHash::new();
    sh.update(&payload);
    let (fs_hash, fs_size) = sh.finalize();
    let result = client
        .check(
            &pb::FileEntry {
                fs_path: "ignored.dat".into(),
                s3_key: "explicit/key.dat".into(),
            },
            fs_size,
            fs_hash,
        )
        .await;
    assert!(
        result.is_ok(),
        "expected Ok using explicit s3_key, got {result:?}"
    );
}
