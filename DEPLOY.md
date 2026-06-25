# cvdbench 部署指南

本文档面向把 cvdbench 装到生产或预发环境跑长时间稳定性压测的运维者。

总览：cvdbench 是「中心 master + 多机 worker daemon + 客户端 CLI」的纯拉取
模型，**Master 不监听 Worker 的入站连接**，所有 worker→master 流量都由
worker 主动发起。详细架构与调度规则见 [`spec.md`](spec.md)。

---

## 1. 系统要求

| 项 | 要求 |
|---|---|
| 操作系统 | Linux（Debian / Ubuntu / CentOS 等） |
| 内核 | ≥ 5.4（FUSE 3.x、可选 io_uring） |
| 架构 | x86_64 |
| Rust 工具链（编译时） | stable，由 `rust-toolchain.toml` 锁定 |
| protoc | 3.12+（编译期 `cvd-proto` 需要） |
| musl target | `x86_64-unknown-linux-musl`（生产分发建议使用，避免依赖目标机 glibc） |
| FD 上限 | worker 建议 `ulimit -n ≥ 65536`（spec §9.8） |

生产部署建议分发 musl 静态二进制，避免目标机 glibc 版本差异导致无法运行。
仓库当前没有单独的 build 脚本自动切换 musl，编译时需要显式指定 target。

---

## 2. 编译

```bash
git clone <this repo> cvdbench
cd cvdbench
rustup target add x86_64-unknown-linux-musl

RUSTFLAGS='-C target-feature=+crt-static' \
  cargo build --release --workspace --target x86_64-unknown-linux-musl

ls target/x86_64-unknown-linux-musl/release/cvd-{master,worker,cli}
ldd target/x86_64-unknown-linux-musl/release/cvd-worker  # 通常显示 not a dynamic executable
```

本机开发调试可以直接运行 `cargo build --release --workspace`，但该产物通常依赖
构建机 glibc，不建议直接分发到异构集群。

测试：

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
```

打包到容器或拷到目标机时只需上述三个二进制 + 配置 + manifest 文件。

---

## 3. Master 配置

参考 [`examples/cvd-master.toml`](examples/cvd-master.toml)。关键字段：

```toml
[server]
listen = "0.0.0.0:9090"            # 监听地址；worker / CLI 访问入口

[metrics]
listen = "0.0.0.0:9100"            # Prometheus text endpoint

[scheduler]
worker_staleness_secs = 60          # 任一 run_worker 超 60s 没 RPC 即 job FAILED
job_retention_secs    = 259200      # 终态 job 保留 3 天后 GC
prepare_timeout_secs  = 600         # PREPARING 等满 10 分钟没全员 ready 即 FAILED
start_delay_ms        = 5000        # 起跑屏障开放后再统一延迟 5s
file_queue_capacity   = 100000      # 读 job：每 job 内存上界 ≈ 100k 文件
dir_queue_capacity    = 50000       # dir_manifest 模式中间队列
dir_scan_concurrency  = 8           # dir_manifest 并行 scanner

# fs_name → mount_point 映射
[[filesystems]]
name        = "examplefs"
mount_point = "/mnt/examplefs"
```

**注意（spec §9.9）**：

- 配置不支持热重载。任何修改必须重启 master 进程。
- 重启即丢失全部内存里的 job 记录；正在跑的 worker 会在下一次 RPC 收到
  `unknown_job=true`，干净放弃当前 job 后回到 FetchJob 轮询。
- 运行中的 job 持有的 `mount_point` / `worker_staleness` 等是 CreateJob 时刻
  的快照，重启后这些快照随 job 一并丢失。

---

## 4. systemd unit 模板

### 4.1 master

`/etc/systemd/system/cvd-master.service`：

```ini
[Unit]
Description=cvdbench master daemon
After=network-online.target

[Service]
Type=simple
User=cvdbench
Group=cvdbench
ExecStart=/usr/local/bin/cvd-master --config /etc/cvdbench/cvd-master.toml
Restart=on-failure
RestartSec=2s
LimitNOFILE=65536
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

`Restart=on-failure` 注意：spec 设计上「重启即丢 job 记录」，所以崩溃后
正在跑的 job 会全部失联。生产建议把 systemd Restart 配合外部告警，不要
盲目无限重启造成长时间空转。

### 4.2 worker

`/etc/systemd/system/cvd-worker.service`：

```ini
[Unit]
Description=cvdbench worker daemon
After=network-online.target
# 如果 worker 依赖 FUSE 挂载点先就绪，可以加：
# RequiresMountsFor=/mnt/examplefs

[Service]
Type=simple
User=cvdbench
Group=cvdbench
ExecStart=/usr/local/bin/cvd-worker --master master.example.com:9090
Restart=always
RestartSec=2s
LimitNOFILE=65536
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

`Restart=always` 因为 worker 是无状态长驻 daemon；崩溃重启后会重新生成
worker_id 并立即恢复 FetchJob 轮询。

启用：

```bash
useradd --system --shell /usr/sbin/nologin cvdbench
install -d /etc/cvdbench/manifests -o cvdbench -g cvdbench
install -m 0644 examples/cvd-master.toml /etc/cvdbench/cvd-master.toml
install -m 0644 examples/manifests/sample.csv /etc/cvdbench/manifests/read.csv

systemctl daemon-reload
systemctl enable --now cvd-master    # 中心机
systemctl enable --now cvd-worker    # 每台测试机
```

---

## 5. 多机部署清单

| 角色 | 数量 | 说明 |
|---|---|---|
| master | 1 | 集群中心；spec 设计上无 HA，崩溃即所有 job 丢失 |
| worker daemon | N | 每台跑被测 FUSE 的测试机各起一个，spec §6.3 |
| CLI 客户端 | 任意 | 任何能访问 master 9090 的机器都可提交 job |

网络要求：

- worker → master：必须可达 master `[server].listen` 端口
- CLI → master：必须可达 master `[server].listen` 端口
- worker → S3（可选）：仅 `s3_consistency_check` 启用时；通过
  `s3_consistency_check.bucket_url` 指向你的 S3 / OSS / MinIO 端点
- master 不主动连 worker，也不需要 S3 网络（凭据只是中转）

时钟：spec §6.4 worker 用 `master_now_ms` 估算 clock offset 后用
monotonic 等待 start_at_ms。**不要求**机器间 NTP 严格同步，但偏差过大
（>>start_delay）会让起跑落点参差。生产建议 NTP/chrony。

---

## 6. 凭据与安全

spec §9.5 / §9.8：

- `s3_consistency_check.access_key` / `secret_key` / `session_token` 是
  **明文字段**，不支持 `*_ref` 间接引用。CLI 把它们随 CreateJob 上传给 master。
- master 收到后立即抽出明文凭据，把 `Job.spec` 中的对应字段替换为 `"***"`。
  之后 `JobEvent` / `QueryJob` / 结果文件里看到的都是脱敏副本。
- 明文凭据只通过 `FetchJobResponse.s3_credentials` 下发给被分配到该 job
  的 worker。**不进入** `Job.spec`、`JobEvent`、`QueryJob`、`workers[]`、
  CSV 输出。
- 链路安全：spec §9.8 建议用 mTLS 保护 master ↔ worker / CLI；本仓库
  当前不内建 mTLS（按 CLAUDE.md 跳过），生产部署请放在受控网络（VPC、
  WireGuard、ssh 隧道）或自行加 sidecar 代理终结 TLS。

CLI 写入 JSON / CSV 时还会再次 enforce 凭据脱敏（防御性二次校验，
即便 master 漏了也安全）。

---

## 7. 监控接入

cvd-master 可通过 `[metrics].listen` 暴露 Prometheus text endpoint，导出 master
已收到的每个 worker `ReportProgress.per_op` 指标，例如：

```text
cvdbench_worker_op_throughput_mbps{job_id="...",worker_id="...",op="read"} 293.6
cvdbench_worker_op_latency_us{job_id="...",worker_id="...",op="read.open",stat="p99"} 1234
```

Prometheus scrape 示例：

```yaml
scrape_configs:
  - job_name: "cvdbench"
    static_configs:
      - targets: ["127.0.0.1:9100"]
```

cvdbench **不内建** master / worker / FUSE 进程的资源指标采集（spec §6.8 / §9）。
这类指标请部署：

- **node_exporter**：每台主机 CPU / RSS / disk / network。
- **process-exporter**：按 `name=("cvd-master"|"cvd-worker"|"<your-fuse-bin>")`
  采集 RSS / fd / thread / context-switch。
- **cAdvisor**（容器化时）：cgroup 视角的资源用量。

把这些数据存进 Prometheus，配 `instance` / `process_name` / 容器 label。
压测时 `cvd-cli list` 给出 `created_at_ms` 时间窗口，运维侧据此关联看图。

cvdbench 自身的 master log 用 `RUST_LOG=info` 默认就有：

- `cvd_master::scheduler::fetch_job` slot 占满进 PREPARING
- `cvd_master::scheduler::ready` 全员 ready 进 RUNNING
- `cvd_master::service::worker_rpc` 结果到达 + COMPLETED
- `cvd_master::scheduler::staleness` watcher → FAILED
- `cvd_master::gc` 终态 GC

worker log（`RUST_LOG=info`）：

- `cvd_worker` 启动 + 连接 master + lifecycle loop
- `cvd_worker::lifecycle` 拿到 job + ReportReady barrier + ReportResult

---

## 8. 故障排查

| 现象 | 排查方向 |
|---|---|
| `cvd-cli create` 返回 `FailedPrecondition: fs_name "X" not registered` | master `cvd-master.toml` 的 `[[filesystems]]` 没有该 `fs_name`，或写错 |
| `cvd-cli create` 返回 `FailedPrecondition: file_manifest "..." does not exist` | manifest 文件路径必须 master 进程 CWD 下可读，建议用绝对路径 |
| job 卡在 PENDING 不动 | 等够 `target_workers` 个 worker 上线后才会进 PREPARING；检查 worker daemon 是否都连上了（`cvd-cli list` + master log 中是否有 `entering PREPARING`） |
| job 进 FAILED 报 `worker X stale` | worker 卡在某次 IO；spec §5.5 默认 60s staleness，建议 worker 进度上报间隔 ≤ 20s；调大 `worker_staleness_secs` 不解决根因 |
| job FAILED 报 `prepare_timeout` | 某 worker 的 `pre_build` 阶段超时（如元数据 layout 太大）；调大 `prepare_timeout_secs` 或缩小 layout |
| 一致性测试 `CET_S3_NOT_FOUND` | 检查 manifest 第二列 `s3_key` 或 `prefix + fs_path` 推导后是否真存在 |
| 一致性测试 `CET_PERMISSION_DENIED` | 凭据没权限读 bucket；用同一组凭据手动 `aws s3 ls` 验证 |
| `cvd-cli watch` stream 突然 None 但 job 还没终态 | 通常是 master 重启或 GC 提前；spec 设计上 stream 不做断线续订，重新跑 `watch <job_id>` |
| worker 反复打印 `FetchJob RPC failed` | master 没起来 / 网络 / 防火墙；worker 走指数退避至 30s 上限自动重连，spec §6.4 |

调试建议：`RUST_LOG=cvd_master=debug,cvd_worker=debug,info`。

---

## 9. 升级 / 重启注意事项

spec §9.9：

- `cvd-master.toml` 改了之后必须重启 `cvd-master`；重启即所有内存中
  job 记录丢失。生产升级时建议「先排空」：CLI 看 `list --status running`
  为空、`list --status pending` 为空，再重启 master。
- worker daemon 重启对正在跑的 job 是致命的：master 在
  `worker_staleness_secs` 内没收到该 worker 的 RPC 即把 job 转 FAILED。
  建议先 `cvd-cli list --status running` 没 job 涉及该 worker 再重启。
- master 地址（`cvd-worker --master`）不支持热切换，需要重启 worker。

---

## 10. 容量规划速算

spec §5.1 默认参数下 master 的内存上界（per job）：

```
file_queue:  100_000 × sizeof(FileEntry) ≈ 100k × ~64B = ~6 MB / job
dir_queue:    50_000 × sizeof(PathBuf)  ≈ 50k × ~96B = ~5 MB / job
worker state: per_op map (5 op × ~1KB hist) × per worker ≈ 5KB / worker
```

100 个并发 job × 50 个 worker，估算上界 ~1 GB 内存。生产中通常远低于此。

---

## 11. 验证部署

`cvd-cli` 自带最简单的活性检查：

```bash
$ cvd-cli --master master.example.com:9090 list
(no jobs)                          # ← master 在线，无 job
```

如果连不上：

```bash
$ cvd-cli --master master.example.com:9090 list
Error: ListJobs: transport error
```

提交一个最小 metadata job 验证全链路（不需要 manifest）：

```bash
$ cvd-cli ... create --config examples/job_metadata.json
```

观察 master log 应当出现：`PENDING → PREPARING → RUNNING → COMPLETED`。
