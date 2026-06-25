//! `cvd-cli delete <job_id>`：DeleteJob —— PENDING / PREPARING / RUNNING 都可以取消。

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;

use crate::endpoint;

pub async fn run(master: &str, job_id: &str) -> anyhow::Result<()> {
    let endpoint = endpoint::resolve(master)?;
    let mut client = MasterServiceClient::connect(endpoint)
        .await
        .map_err(|e| anyhow::anyhow!("connect master {master}: {e}"))?;
    client
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("DeleteJob: {e}"))?;
    println!("delete request submitted for job {job_id}");
    Ok(())
}
