# cvdbench Grafana Dashboard

This directory contains a Grafana provisioning bundle for cvdbench's Prometheus metrics endpoint.

## Files

- `provisioning/datasources/prometheus.yml`: Prometheus datasource. Set `PROMETHEUS_URL` if Prometheus is not `http://prometheus:9090`.
- `provisioning/dashboards/cvdbench.yml`: Dashboard provider.
- `dashboards/cvdbench-metrics.json`: Dashboard to import or provision.
- `../prometheus-cvdbench.yml`: Minimal Prometheus scrape config for `127.0.0.1:19100`.

## Docker mount example

```bash
docker run --rm -p 3000:3000 \
  -e PROMETHEUS_URL=http://prometheus:9090 \
  -v "$PWD/deploy/grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro" \
  -v "$PWD/deploy/grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro" \
  -v "$PWD/deploy/grafana/dashboards:/etc/grafana/provisioning/dashboards/cvdbench:ro" \
  grafana/grafana:latest
```

## Prometheus scrape target

For the current test cluster, scrape `127.0.0.1:19100`:

```yaml
scrape_configs:
  - job_name: cvdbench
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:19100"]
```
