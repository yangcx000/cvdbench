//! Generated tonic/prost stubs for the cvdbench MasterService.
//!
//! 单一来源：`proto/master.proto`，由 `build.rs` 调用 `tonic-build` 生成。
//! 业务 crate 通过 `cvd_proto::cvdbench::*` 访问消息与服务。

#![allow(clippy::pedantic, clippy::nursery, clippy::all, missing_docs)]

pub mod cvdbench {
    tonic::include_proto!("cvdbench");
}

pub use cvdbench::master_service_client::MasterServiceClient;
pub use cvdbench::master_service_server::{MasterService, MasterServiceServer};
