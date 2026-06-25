//! `cvd-cli query <job_id>`：一次性 QueryJob，打印汇总 + JSON 落盘（可选）。

use std::path::PathBuf;

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;

use crate::{display, endpoint, output};

pub async fn run(
    master: &str,
    job_id: &str,
    output_path: Option<PathBuf>,
    verbose: bool,
) -> anyhow::Result<()> {
    let endpoint = endpoint::resolve(master)?;
    let mut client = MasterServiceClient::connect(endpoint)
        .await
        .map_err(|e| anyhow::anyhow!("connect master {master}: {e}"))?;
    let q = client
        .query_job(pb::QueryJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("QueryJob: {e}"))?
        .into_inner();
    display::summary::print_with_options(&q, display::summary::SummaryOptions { verbose });
    if let Some(path) = output_path {
        output::write_report(&path, &q)?;
        println!("result written to {}", path.display());
    }
    Ok(())
}
