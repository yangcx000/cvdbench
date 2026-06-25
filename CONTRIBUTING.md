# Contributing

Thanks for your interest in contributing to cvdbench.

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Guidelines

- Keep changes focused and covered by tests where practical.
- Do not commit real credentials, internal hostnames, private endpoints, or production paths.
- Use example values such as `examplefs`, `example-bucket`, `s3.example.com`, and `EXAMPLE_ACCESS_KEY` in docs and tests.
