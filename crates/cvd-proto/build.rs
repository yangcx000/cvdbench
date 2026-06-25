use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("proto");
    let proto_file = proto_root.join("master.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // 系统 protoc 3.12 需要显式开启 proto3 optional；3.15+ 默认开启。
        .protoc_arg("--experimental_allow_proto3_optional")
        // 让 CLI 把 job.json 直接 deserialize 为 pb::BenchSpec、把结果 serialize
        // 成 JSON，省掉手写镜像结构。
        .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
        // 容器级 `#[serde(default)]` 让用户写 JSON 时可以省略字段；prost 已为
        // 所有 message 派生 Default，缺省字段会取该类型零值。注意 enum 不需要
        // 这条 attribute（默认值由 0 = `_UNSPECIFIED` 提供，但用户 JSON 一般用
        // 字符串单位字段，无需 enum 默认）。这里只针对用户 JSON 涉及的 message。
        .type_attribute("cvdbench.BenchSpec", "#[serde(default)]")
        .type_attribute("cvdbench.ReadConfig", "#[serde(default)]")
        .type_attribute("cvdbench.WriteConfig", "#[serde(default)]")
        .type_attribute("cvdbench.MetadataConfig", "#[serde(default)]")
        .type_attribute("cvdbench.ConsistencyConfig", "#[serde(default)]")
        .type_attribute("cvdbench.FileSizeRange", "#[serde(default)]")
        // bytes 字段（PerformanceMetrics.latency_histogram_hdr）默认 Vec<u8>，
        // serde-json 会序列化为整数数组；若需要切到 base64 可后续单独配置。
        .compile_protos(&[proto_file], &[proto_root])?;

    Ok(())
}
