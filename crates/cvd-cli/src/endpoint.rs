//! Master gRPC endpoint URL 解析（spec §9.8）。
//!
//! `--master` 参数支持两种形式：
//! - `host:port` —— 自动加 `http://` 前缀；
//! - 已含 `http://` —— 原样返回。
//!
//! 当前 CLI 未暴露 TLS/CA 配置，因此显式拒绝 `https://`，避免形成伪支持。

pub fn resolve(master_arg: &str) -> anyhow::Result<String> {
    let trimmed = master_arg.trim();
    if trimmed.starts_with("https://") {
        Err(anyhow::anyhow!(
            "https:// master endpoints are not supported by cvd-cli yet; use host:port or http://host:port"
        ))
    } else if trimmed.starts_with("http://") {
        Ok(trimmed.to_owned())
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_port_gets_http_prefix() {
        assert_eq!(resolve("127.0.0.1:9090").unwrap(), "http://127.0.0.1:9090");
        assert_eq!(
            resolve("master.example.com:9090").unwrap(),
            "http://master.example.com:9090"
        );
    }

    #[test]
    fn http_url_passes_through() {
        assert_eq!(
            resolve("http://1.2.3.4:9090").unwrap(),
            "http://1.2.3.4:9090"
        );
    }

    #[test]
    fn https_url_is_rejected() {
        assert!(resolve("https://master.example.com:9090").is_err());
    }

    #[test]
    fn whitespace_trimmed() {
        assert_eq!(
            resolve("  127.0.0.1:9090  ").unwrap(),
            "http://127.0.0.1:9090"
        );
    }
}
