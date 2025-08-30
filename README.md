# IBM COS Benchmark

A Rust-based tool to benchmark IBM Cloud Object Storage throughput and latency, designed for Kubernetes environments.

## Features
- Upload/download tests for small, medium, and large files
- Mixed workload simulation
- Parallel upload/download stress test
- Bottleneck analysis and recommendations
- Detailed JSON report output

## Usage

### Build
```sh
cargo build --release
```

### Run Benchmark
```sh
./target/release/cos-benchmark benchmark \
    --endpoint https://s3.eu-de.cloud-object-storage.appdomain.cloud \
    --access-key <ACCESS_KEY> \
    --secret-key <SECRET_KEY> \
    --bucket <BUCKET_NAME> \
    --prefix benchmark \
    --duration 60
```

### Run Cleanup
```sh
./target/release/cos-benchmark cleanup \
    --endpoint https://s3.eu-de.cloud-object-storage.appdomain.cloud \
    --access-key <ACCESS_KEY> \
    --secret-key <SECRET_KEY> \
    --bucket <BUCKET_NAME> \
    --prefix benchmark
```

### Select Specific Tests
You can use the `--tests` argument to run only specific benchmark scenarios. Repeat the argument for multiple tests:

Available test names:
- small_files_1kb
- small_files_64kb
- medium_files_1mb
- medium_files_16mb
- large_files_100mb
- large_files_1gb
- mixed_workload

Example:
```sh
./target/release/cos-benchmark benchmark \
    --endpoint https://s3.eu-de.cloud-object-storage.appdomain.cloud \
    --access-key <ACCESS_KEY> \
    --secret-key <SECRET_KEY> \
    --bucket <BUCKET_NAME> \
    --prefix benchmark \
    --duration 60 \
    --tests small_files_1kb --tests large_files_1gb
```

### Region Selection
You can specify the IBM COS region using the `--region` argument (default: eu-de):

Example:
```sh
./target/release/cos-benchmark benchmark \
    --endpoint https://s3.eu-de.cloud-object-storage.appdomain.cloud \
    --access-key <ACCESS_KEY> \
    --secret-key <SECRET_KEY> \
    --bucket <BUCKET_NAME> \
    --prefix benchmark \
    --duration 60 \
    --region eu-de
```

## Example Output
```
🏗️ IBM Cloud Object Storage Benchmark Starting...
📍 Endpoint: https://s3.eu-de.cloud-object-storage.appdomain.cloud
🪣 Bucket: my-bucket
⏱️ Duration: 60s per test

🔥 Running test: small_files_1kb
  └─ Concurrency 1: 0.2 MB/s, 3.4 ops/s
  └─ Concurrency 5: 0.8 MB/s, 13.1 ops/s
...etc...

📈 BENCHMARK RESULTS SUMMARY
═══════════════════════════════
🚀 Maximum Throughput: 120.5 MB/s
📊 Average Throughput: 85.2 MB/s
🔍 Likely Bottleneck: Network or Node Resource Limits
💾 CPU Utilization: 50.0%
🧠 Memory Utilization: 23.1%
🌐 Network Saturation: 9.6%

💡 RECOMMENDATIONS:
  1. Low throughput detected - check node network interface speed and COS endpoint proximity
  2. Optimal concurrency level appears to be around 20 threads

💾 Detailed report saved to: cos_benchmark_report_20250830_153000.json
```

## Notes
- For best results, run in a dedicated pod or node with sufficient resources.
- The tool requires valid IBM COS credentials and bucket access.
- Reports are saved as JSON for further analysis.

## Help
Run with `--help` for detailed CLI options:
```sh
./target/release/cos-benchmark --help
```
