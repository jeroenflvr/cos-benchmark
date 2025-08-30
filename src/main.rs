use std::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Semaphore;
use aws_sdk_s3::{Client, config::Builder as ConfigBuilder};
use aws_sdk_s3::config::Region;
use aws_credential_types::Credentials;
use serde::{Deserialize, Serialize};
use clap::{Parser, Subcommand};
use uuid::Uuid;
use rand::seq::SliceRandom;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rayon::prelude::*;
use sysinfo::System;

#[derive(Parser)]
#[command(name = "cos-benchmark")]
#[command(about = "IBM Cloud Object Storage Kubernetes Throughput Benchmark")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run comprehensive benchmark suite
    Benchmark {
        #[arg(long, help = "IBM COS endpoint URL")]
        endpoint: String,
        #[arg(long, help = "Access key ID")]
        access_key: String,
        #[arg(long, help = "Secret access key")]
        secret_key: String,
        #[arg(long, help = "Bucket name")]
        bucket: String,
        #[arg(long, default_value = "benchmark", help = "Test prefix")]
        prefix: String,
        #[arg(long, default_value = "60", help = "Test duration in seconds")]
        duration: u64,
        #[arg(long, default_value = "false", help = "Skip cleanup after tests")]
        skip_cleanup: bool,
    },
    /// Clean up test objects
    Cleanup {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        access_key: String,
        #[arg(long)]
        secret_key: String,
        #[arg(long)]
        bucket: String,
        #[arg(long, default_value = "benchmark")]
        prefix: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkResult {
    test_name: String,
    file_size: u64,
    concurrent_operations: usize,
    total_operations: u64,
    total_bytes: u64,
    duration_ms: u64,
    throughput_mbps: f64,
    operations_per_second: f64,
    avg_latency_ms: f64,
    p95_latency_ms: f64,
    errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SystemInfo {
    hostname: String,
    cpu_count: usize,
    cpu_frequency: u64,
    total_memory: u64,
    available_memory: u64,
    network_interfaces: Vec<NetworkInterface>,
    kubernetes_info: KubernetesInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkInterface {
    name: String,
    received_bytes: u64,
    transmitted_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KubernetesInfo {
    pod_name: Option<String>,
    node_name: Option<String>,
    namespace: Option<String>,
    limits: ResourceLimits,
    requests: ResourceRequests,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResourceLimits {
    memory: Option<String>,
    cpu: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResourceRequests {
    memory: Option<String>,
    cpu: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkReport {
    timestamp: String,
    system_info: SystemInfo,
    results: Vec<BenchmarkResult>,
    summary: BenchmarkSummary,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkSummary {
    max_throughput_mbps: f64,
    avg_throughput_mbps: f64,
    bottleneck_analysis: BottleneckAnalysis,
    recommendations: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BottleneckAnalysis {
    likely_bottleneck: String,
    evidence: Vec<String>,
    cpu_utilization: f64,
    memory_utilization: f64,
    network_saturation_estimate: f64,
}

struct BenchmarkSuite {
    client: Client,
    bucket: String,
    prefix: String,
    system_info: SystemInfo,
}

impl BenchmarkSuite {
    async fn new(endpoint: &str, access_key: &str, secret_key: &str, bucket: &str, prefix: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Configure S3 client for IBM COS
        let credentials = Credentials::new(access_key, secret_key, None, None, "benchmark");
        
        let config = ConfigBuilder::new()
            .endpoint_url(endpoint)
            .region(Region::new("eu-de")) // IBM COS doesn't care about region for S3 API
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(config);
        
        // Gather system information
        let system_info = Self::gather_system_info().await;
        
        Ok(BenchmarkSuite {
            client,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            system_info,
        })
    }

    async fn gather_system_info() -> SystemInfo {
        let mut system = System::new_all();
        system.refresh_all();
        
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        
    let network_interfaces = Vec::new(); // fallback for sysinfo versions without networks()

        let kubernetes_info = Self::gather_kubernetes_info().await;
        
        SystemInfo {
            hostname,
            cpu_count: system.cpus().len(),
            cpu_frequency: system.cpus().first().map(|cpu| cpu.frequency()).unwrap_or(0),
            total_memory: system.total_memory(),
            available_memory: system.available_memory(),
            network_interfaces,
            kubernetes_info,
        }
    }

    async fn gather_kubernetes_info() -> KubernetesInfo {
        let pod_name = std::env::var("HOSTNAME").ok();
        let node_name = std::env::var("NODE_NAME").ok();
        let namespace = std::env::var("POD_NAMESPACE").ok();
        
        // Try to read resource limits/requests from downward API
        let memory_limit = std::env::var("MEMORY_LIMIT").ok();
        let cpu_limit = std::env::var("CPU_LIMIT").ok();
        let memory_request = std::env::var("MEMORY_REQUEST").ok();
        let cpu_request = std::env::var("CPU_REQUEST").ok();
        
        KubernetesInfo {
            pod_name,
            node_name,
            namespace,
            limits: ResourceLimits {
                memory: memory_limit,
                cpu: cpu_limit,
            },
            requests: ResourceRequests {
                memory: memory_request,
                cpu: cpu_request,
            },
        }
    }

    async fn run_comprehensive_benchmark(&self, duration: u64) -> Result<BenchmarkReport, Box<dyn std::error::Error>> {
        println!("🚀 Starting IBM COS Kubernetes Throughput Benchmark");
        println!("📊 System Info: {} CPUs, {:.1} GB RAM", 
                 self.system_info.cpu_count, 
                 self.system_info.total_memory as f64 / 1024.0 / 1024.0 / 1024.0);
        
        let mut results = Vec::new();
        
        // Test scenarios with different file sizes and concurrency levels
        let test_scenarios = vec![
            // Small files (simulating metadata-heavy workloads)
            ("small_files_1kb", 1024, vec![1, 5, 10, 20]),
            ("small_files_64kb", 64 * 1024, vec![1, 5, 10, 20]),
            
            // Medium files (typical application files)
            ("medium_files_1mb", 1024 * 1024, vec![1, 5, 10, 20, 50]),
            ("medium_files_16mb", 16 * 1024 * 1024, vec![1, 5, 10, 20]),
            
            // Large files (big data, ML models)
            ("large_files_100mb", 100 * 1024 * 1024, vec![1, 5, 10]),
            ("large_files_1gb", 1024 * 1024 * 1024, vec![1, 2, 5]),
            
            // Mixed workload simulation
            ("mixed_workload", 0, vec![10]), // Special case handled separately
        ];

        for (test_name, file_size, concurrency_levels) in test_scenarios {
            println!("\n🔥 Running test: {}", test_name);
            
            if test_name == "mixed_workload" {
                let result = self.run_mixed_workload_test(duration).await?;
                results.push(result);
                continue;
            }
            
            for &concurrency in &concurrency_levels {
                let result = if file_size < 1024 * 1024 {
                    self.run_small_file_test(test_name, file_size, concurrency, duration).await?
                } else {
                    self.run_large_file_test(test_name, file_size, concurrency, duration).await?
                };
                
                println!("  └─ Concurrency {}: {:.1} MB/s, {:.1} ops/s", 
                         concurrency, result.throughput_mbps, result.operations_per_second);
                
                results.push(result);
                
                // Brief pause between tests
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }

        // Parallel upload/download test
        let parallel_result = self.run_parallel_upload_download_test(duration).await?;
        results.push(parallel_result);

        // Generate report
        let summary = self.analyze_results(&results);
        
        let report = BenchmarkReport {
            timestamp: chrono::Utc::now().to_rfc3339(),
            system_info: self.system_info.clone(),
            results,
            summary,
        };

        Ok(report)
    }

    async fn run_small_file_test(&self, test_name: &str, file_size: u64, concurrency: usize, duration: u64) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    println!("[DEBUG] run_small_file_test: file_size={} concurrency={}", file_size, concurrency);
        let data = vec![0u8; file_size as usize];
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut latencies = Vec::new();
        let mut operations = 0u64;
        let mut errors = 0u64;
        
        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration);
        
        while Instant::now() < end_time {
            let tasks: Vec<_> = (0..concurrency).map(|_| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let prefix = self.prefix.clone();
                let data = data.clone();
                let semaphore = semaphore.clone();
                
                tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let key = format!("{}/small_file_{}", prefix, Uuid::new_v4());
                    
                    let op_start = Instant::now();
                    
                    // Upload
                    let upload_result = client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(data.into())
                        .send()
                        .await;
                    
                    if upload_result.is_err() {
                        return (0, true, op_start.elapsed());
                    }
                    
                    // Download
                    let download_result = client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await;
                    
                    let success = download_result.is_ok();
                    (1, !success, op_start.elapsed())
                })
            }).collect();
            
            for task in tasks {
                let (ops, error, latency) = task.await.unwrap();
                operations += ops;
                if error { errors += 1; }
                latencies.push(latency.as_millis() as f64);
            }
            
            if operations % 100 == 0 {
                println!("    Progress: {} operations", operations);
            }
        }
        
    let duration_ms = start_time.elapsed().as_millis() as u64;
    let total_bytes = operations * file_size * 2; // Upload + download
    println!("[DEBUG] run_small_file_test: operations={} total_bytes={} duration_ms={} errors={}", operations, total_bytes, duration_ms, errors);
        
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95_index = (latencies.len() as f64 * 0.95) as usize;
        
        Ok(BenchmarkResult {
            test_name: format!("{}_{}_threads", test_name, concurrency),
            file_size,
            concurrent_operations: concurrency,
            total_operations: operations * 2, // Upload + download
            total_bytes,
            duration_ms,
            throughput_mbps: (total_bytes as f64) / (duration_ms as f64) * 1000.0 / 1024.0 / 1024.0,
            operations_per_second: (operations * 2) as f64 / (duration_ms as f64 / 1000.0),
            avg_latency_ms: latencies.iter().sum::<f64>() / latencies.len() as f64,
            p95_latency_ms: latencies.get(p95_index).copied().unwrap_or(0.0),
            errors,
        })
    }

    async fn run_large_file_test(&self, test_name: &str, file_size: u64, concurrency: usize, duration: u64) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    println!("[DEBUG] run_large_file_test: file_size={} concurrency={}", file_size, concurrency);
        // For large files, use multipart upload for better performance
        let chunk_size = 8 * 1024 * 1024; // 8MB chunks (multiple of 4MB as recommended)
        let data_chunk = vec![0u8; chunk_size];
        let chunks_per_file = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
        
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut latencies = Vec::new();
        let mut operations = 0u64;
        let mut errors = 0u64;
        
        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration);
        
        while Instant::now() < end_time {
            let tasks: Vec<_> = (0..concurrency).map(|_| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let prefix = self.prefix.clone();
                let data_chunk = data_chunk.clone();
                let semaphore = semaphore.clone();
                
                tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let key = format!("{}/large_file_{}", prefix, Uuid::new_v4());
                    
                    let op_start = Instant::now();
                    
                    // Multipart upload
                    let multipart_upload = client
                        .create_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await;
                    
                    if let Ok(upload_resp) = multipart_upload {
                        let upload_id = upload_resp.upload_id().unwrap();
                        let mut parts = Vec::new();
                        
                        for part_number in 1..=chunks_per_file {
                            let upload_part = client
                                .upload_part()
                                .bucket(&bucket)
                                .key(&key)
                                .upload_id(upload_id)
                                .part_number(part_number as i32)
                                .body(data_chunk.clone().into())
                                .send()
                                .await;
                            
                            if let Ok(part_resp) = upload_part {
                                parts.push(aws_sdk_s3::types::CompletedPart::builder()
                                    .part_number(part_number as i32)
                                    .e_tag(part_resp.e_tag().unwrap())
                                    .build());
                            }
                        }
                        
                        let _complete = client
                            .complete_multipart_upload()
                            .bucket(&bucket)
                            .key(&key)
                            .upload_id(upload_id)
                            .multipart_upload(
                                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                                    .set_parts(Some(parts))
                                    .build()
                            )
                            .send()
                            .await;
                    }
                    
                    // Download test
                    let download_result = client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await;
                    
                    let success = download_result.is_ok();
                    (1, !success, op_start.elapsed())
                })
            }).collect();
            
            for task in tasks {
                let (ops, error, latency) = task.await.unwrap();
                operations += ops;
                if error { errors += 1; }
                latencies.push(latency.as_millis() as f64);
            }
            
            if operations % 10 == 0 {
                println!("    Progress: {} operations", operations);
            }
        }
        
    let duration_ms = start_time.elapsed().as_millis() as u64;
    println!("[DEBUG] run_large_file_test: operations={} total_bytes={} duration_ms={} errors={}", operations, operations * file_size * 2, duration_ms, errors);
        let total_bytes = operations * file_size * 2;
        
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95_index = (latencies.len() as f64 * 0.95) as usize;
        
        Ok(BenchmarkResult {
            test_name: format!("{}_{}_threads", test_name, concurrency),
            file_size,
            concurrent_operations: concurrency,
            total_operations: operations * 2,
            total_bytes,
            duration_ms,
            throughput_mbps: (total_bytes as f64) / (duration_ms as f64) * 1000.0 / 1024.0 / 1024.0,
            operations_per_second: (operations * 2) as f64 / (duration_ms as f64 / 1000.0),
            avg_latency_ms: latencies.iter().sum::<f64>() / latencies.len() as f64,
            p95_latency_ms: latencies.get(p95_index).copied().unwrap_or(0.0),
            errors,
        })
    }

    async fn run_mixed_workload_test(&self, duration: u64) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        // Simulate real-world mixed workload: 70% small files, 20% medium, 10% large
        let file_sizes = vec![
            (1024, 7),           // 1KB - 70%
            (1024 * 1024, 2),    // 1MB - 20%  
            (10 * 1024 * 1024, 1), // 10MB - 10%
        ];
        
        let concurrency = 20;
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut latencies = Vec::new();
        let mut operations = 0u64;
        let mut errors = 0u64;
        let mut total_bytes = 0u64;
        
        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration);
        
        while Instant::now() < end_time {
            // Create weighted file size selection
            let mut weighted_sizes = Vec::new();
            for (size, weight) in &file_sizes {
                for _ in 0..*weight {
                    weighted_sizes.push(*size);
                }
            }
            
            let tasks: Vec<_> = (0..concurrency).map(|_| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let prefix = self.prefix.clone();
                let semaphore = semaphore.clone();
                let weighted_sizes = weighted_sizes.clone();
                
                tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    // Randomly select file size based on weights
                    let mut rng = StdRng::from_seed(rand::random());
                    let file_size = *weighted_sizes.choose(&mut rng).unwrap();
                    let data = vec![0u8; file_size];
                    let key = format!("{}/mixed_file_{}", prefix, Uuid::new_v4());
                    
                    let op_start = Instant::now();
                    
                    // Upload
                    let upload_result = client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(data.into())
                        .send()
                        .await;
                    
                    if upload_result.is_err() {
                        return (0, file_size, true, op_start.elapsed());
                    }
                    
                    // Download
                    let download_result = client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await;
                    
                    let success = download_result.is_ok();
                    (1, file_size, !success, op_start.elapsed())
                })
            }).collect();
            
            for task in tasks {
                let (ops, size, error, latency) = task.await.unwrap();
                operations += ops;
                total_bytes += size as u64 * 2; // Upload + download
                if error { errors += 1; }
                latencies.push(latency.as_millis() as f64);
            }
        }
        
        let duration_ms = start_time.elapsed().as_millis() as u64;
        
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95_index = (latencies.len() as f64 * 0.95) as usize;
        
        Ok(BenchmarkResult {
            test_name: "mixed_workload".to_string(),
            file_size: 0, // Mixed sizes
            concurrent_operations: concurrency,
            total_operations: operations * 2,
            total_bytes,
            duration_ms,
            throughput_mbps: (total_bytes as f64 / 1024.0 / 1024.0) / (duration_ms as f64 / 1000.0),
            operations_per_second: (operations * 2) as f64 / (duration_ms as f64 / 1000.0),
            avg_latency_ms: latencies.iter().sum::<f64>() / latencies.len() as f64,
            p95_latency_ms: latencies.get(p95_index).copied().unwrap_or(0.0),
            errors,
        })
    }

    async fn run_parallel_upload_download_test(&self, duration: u64) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        println!("\n🔥 Running parallel upload/download stress test");
        
        let file_size = 4 * 1024 * 1024; // 4MB per IBM COS best practices
        let data = vec![0u8; file_size];
        let upload_concurrency = 50;
        let download_concurrency = 50;
        
        let upload_semaphore = Arc::new(Semaphore::new(upload_concurrency));
        let download_semaphore = Arc::new(Semaphore::new(download_concurrency));
        
        let mut upload_latencies = Vec::new();
        let mut download_latencies = Vec::new();
        let mut upload_ops = 0u64;
        let mut download_ops = 0u64;
        let mut errors = 0u64;
        
        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration);
        
        let upload_tasks = tokio::spawn({
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let prefix = self.prefix.clone();
            let upload_semaphore = upload_semaphore.clone();
            let data = data.clone();
            
            async move {
                let mut ops = 0u64;
                let mut latencies = Vec::new();
                let mut errors = 0u64;
                
                while Instant::now() < end_time {
                    let _permit = upload_semaphore.acquire().await.unwrap();
                    let key = format!("{}/parallel_upload_{}", prefix, Uuid::new_v4());
                    
                    let op_start = Instant::now();
                    let result = client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(data.clone().into())
                        .send()
                        .await;
                    
                    if result.is_err() { errors += 1; }
                    ops += 1;
                    latencies.push(op_start.elapsed().as_millis() as f64);
                }
                
                (ops, latencies, errors)
            }
        });
        
        let download_tasks = tokio::spawn({
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let prefix = self.prefix.clone();
            let download_semaphore = download_semaphore.clone();
            
            async move {
                let mut ops = 0u64;
                let mut latencies = Vec::new();
                let mut errors = 0u64;
                
                // Give uploads a head start
                tokio::time::sleep(Duration::from_secs(5)).await;
                
                while Instant::now() < end_time {
                    let _permit = download_semaphore.acquire().await.unwrap();
                    let key = format!("{}/parallel_upload_{}", prefix, Uuid::new_v4());
                    
                    let op_start = Instant::now();
                    let result = client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await;
                    
                    if result.is_err() { errors += 1; }
                    ops += 1;
                    latencies.push(op_start.elapsed().as_millis() as f64);
                }
                
                (ops, latencies, errors)
            }
        });
        
        let (upload_result, download_result) = tokio::join!(upload_tasks, download_tasks);
        let (u_ops, u_lat, u_err) = upload_result.unwrap();
        let (d_ops, d_lat, d_err) = download_result.unwrap();
        
        upload_ops = u_ops;
        download_ops = d_ops;
        upload_latencies = u_lat;
        download_latencies = d_lat;
        errors = u_err + d_err;
        
        let duration_ms = start_time.elapsed().as_millis() as u64;
        let total_ops = upload_ops + download_ops;
        let total_bytes = (upload_ops + download_ops) * file_size as u64;
        
        let mut all_latencies = upload_latencies;
        all_latencies.extend(download_latencies);
        all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let p95_index = (all_latencies.len() as f64 * 0.95) as usize;
        
        Ok(BenchmarkResult {
            test_name: "parallel_upload_download".to_string(),
            file_size: file_size as u64,
            concurrent_operations: upload_concurrency + download_concurrency,
            total_operations: total_ops,
            total_bytes,
            duration_ms,
            throughput_mbps: (total_bytes as f64 / 1024.0 / 1024.0) / (duration_ms as f64 / 1000.0),
            operations_per_second: total_ops as f64 / (duration_ms as f64 / 1000.0),
            avg_latency_ms: all_latencies.iter().sum::<f64>() / all_latencies.len() as f64,
            p95_latency_ms: all_latencies.get(p95_index).copied().unwrap_or(0.0),
            errors,
        })
    }

    fn analyze_results(&self, results: &[BenchmarkResult]) -> BenchmarkSummary {
        let max_throughput = results.iter()
            .map(|r| r.throughput_mbps)
            .fold(0.0, f64::max);
        
        let avg_throughput = results.iter()
            .map(|r| r.throughput_mbps)
            .sum::<f64>() / results.len() as f64;
        
        // Analyze bottlenecks
        let bottleneck_analysis = self.analyze_bottlenecks(results, max_throughput);
        
        let recommendations = self.generate_recommendations(results, &bottleneck_analysis);
        
        BenchmarkSummary {
            max_throughput_mbps: max_throughput,
            avg_throughput_mbps: avg_throughput,
            bottleneck_analysis,
            recommendations,
        }
    }

    fn analyze_bottlenecks(&self, results: &[BenchmarkResult], max_throughput: f64) -> BottleneckAnalysis {
        let mut evidence = Vec::new();
        let mut likely_bottleneck = "Unknown".to_string();
        
        // Analyze throughput scaling with concurrency
        let small_file_results: Vec<_> = results.iter()
            .filter(|r| r.test_name.contains("small_files") && r.file_size <= 64 * 1024)
            .collect();
        
        let large_file_results: Vec<_> = results.iter()
            .filter(|r| r.test_name.contains("large_files"))
            .collect();
        
        // Check if throughput scales with concurrency
        let throughput_scaling = self.calculate_throughput_scaling(&small_file_results);
        
        if throughput_scaling < 0.5 {
            likely_bottleneck = "CPU or API Rate Limiting".to_string();
            evidence.push("Throughput doesn't scale well with concurrency".to_string());
        }
        
        // Check latency patterns
        let avg_small_latency = small_file_results.iter()
            .map(|r| r.avg_latency_ms)
            .sum::<f64>() / small_file_results.len() as f64;
        
        let avg_large_latency = large_file_results.iter()
            .map(|r| r.avg_latency_ms)
            .sum::<f64>() / large_file_results.len() as f64;
        
        if avg_small_latency > 1000.0 {
            evidence.push("High latency for small files suggests network or API overhead".to_string());
        }
        
        if max_throughput < 100.0 {
            likely_bottleneck = "Network or Node Resource Limits".to_string();
            evidence.push("Low overall throughput suggests network or resource constraints".to_string());
        } else if max_throughput > 500.0 {
            likely_bottleneck = "Not Network Limited".to_string();
            evidence.push("High throughput achieved, bottleneck likely elsewhere".to_string());
        }
        
        // Estimate resource utilization (simplified)
        let cpu_util = if max_throughput > 200.0 { 80.0 } else { 50.0 };
        let memory_util = (self.system_info.total_memory - self.system_info.available_memory) as f64 
                         / self.system_info.total_memory as f64 * 100.0;
        
        // Rough network saturation estimate based on throughput
        let estimated_network_capacity = if self.system_info.kubernetes_info.pod_name.is_some() {
            1000.0 // Assume 1Gbps for Kubernetes pods
        } else {
            10000.0 // Assume 10Gbps for bare metal
        };
        
        let network_saturation = (max_throughput * 8.0) / estimated_network_capacity * 100.0;
        
        BottleneckAnalysis {
            likely_bottleneck,
            evidence,
            cpu_utilization: cpu_util,
            memory_utilization: memory_util,
            network_saturation_estimate: network_saturation,
        }
    }

    fn calculate_throughput_scaling(&self, results: &[&BenchmarkResult]) -> f64 {
        if results.len() < 2 { return 1.0; }
        
        let mut scaling_factors = Vec::new();
        
        for i in 1..results.len() {
            let prev = &results[i-1];
            let curr = &results[i];
            
            if prev.concurrent_operations > 0 && curr.concurrent_operations > prev.concurrent_operations {
                let concurrency_ratio = curr.concurrent_operations as f64 / prev.concurrent_operations as f64;
                let throughput_ratio = curr.throughput_mbps / prev.throughput_mbps;
                scaling_factors.push(throughput_ratio / concurrency_ratio);
            }
        }
        
        scaling_factors.iter().sum::<f64>() / scaling_factors.len() as f64
    }

    fn generate_recommendations(&self, results: &[BenchmarkResult], bottleneck: &BottleneckAnalysis) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        // Check if running in Kubernetes
        if self.system_info.kubernetes_info.pod_name.is_some() {
            recommendations.push("Running in Kubernetes pod - consider resource limits and requests".to_string());
            
            if let Some(cpu_limit) = &self.system_info.kubernetes_info.limits.cpu {
                recommendations.push(format!("Pod CPU limit: {} - ensure adequate CPU allocation", cpu_limit));
            }
            
            if let Some(memory_limit) = &self.system_info.kubernetes_info.limits.memory {
                recommendations.push(format!("Pod memory limit: {} - ensure adequate memory allocation", memory_limit));
            }
        }
        
        if bottleneck.network_saturation_estimate > 80.0 {
            recommendations.push("Network appears saturated - consider node network upgrade or multiple endpoints".to_string());
        }
        
        let max_throughput = results.iter().map(|r| r.throughput_mbps).fold(0.0, f64::max);
        
        if max_throughput < 50.0 {
            recommendations.push("Low throughput detected - check node network interface speed and COS endpoint proximity".to_string());
        }
        
        // Check for high error rates
        let total_errors: u64 = results.iter().map(|r| r.errors).sum();
        let total_operations: u64 = results.iter().map(|r| r.total_operations).sum();
        let error_rate = total_errors as f64 / total_operations as f64 * 100.0;
        
        if error_rate > 5.0 {
            recommendations.push(format!("High error rate ({:.1}%) - implement exponential backoff and retry logic", error_rate));
        }
        
        // File size specific recommendations
        let small_file_performance = results.iter()
            .filter(|r| r.file_size <= 64 * 1024)
            .map(|r| r.throughput_mbps)
            .fold(0.0, f64::max);
        
        if small_file_performance < 10.0 {
            recommendations.push("Poor small file performance - consider batching small operations or using larger chunk sizes".to_string());
        }
        
        // Concurrency analysis
        let concurrency_results: HashMap<usize, f64> = results.iter()
            .map(|r| (r.concurrent_operations, r.throughput_mbps))
            .collect();
        
        let optimal_concurrency = concurrency_results.iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(k, _)| *k)
            .unwrap_or(10);
        
        recommendations.push(format!("Optimal concurrency level appears to be around {} threads", optimal_concurrency));
        
        recommendations
    }

    async fn cleanup_test_objects(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🧹 Cleaning up test objects...");
        
        let list_result = self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .send()
            .await?;
        
        let objects = list_result.contents();
        for object in objects {
            if let Some(key) = object.key() {
                let _ = self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await;
            }
        }
        println!("✅ Cleaned up {} test objects", objects.len());
    // ...existing code...
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    match args.command {
        Commands::Benchmark { 
            endpoint, 
            access_key, 
            secret_key, 
            bucket, 
            prefix, 
            duration,
            skip_cleanup 
        } => {
            let benchmark = BenchmarkSuite::new(&endpoint, &access_key, &secret_key, &bucket, &prefix).await?;
            
            println!("🏗️ IBM Cloud Object Storage Benchmark Starting...");
            println!("📍 Endpoint: {}", endpoint);
            println!("🪣 Bucket: {}", bucket);
            println!("⏱️ Duration: {}s per test", duration);
            
            if let Some(pod_name) = &benchmark.system_info.kubernetes_info.pod_name {
                println!("🚢 Running in Kubernetes pod: {}", pod_name);
                if let Some(node_name) = &benchmark.system_info.kubernetes_info.node_name {
                    println!("🖥️  Node: {}", node_name);
                }
            }
            
            let report = benchmark.run_comprehensive_benchmark(duration).await?;
            
            // Print summary
            println!("\n📈 BENCHMARK RESULTS SUMMARY");
            println!("═══════════════════════════════");
            println!("🚀 Maximum Throughput: {:.1} MB/s", report.summary.max_throughput_mbps);
            println!("📊 Average Throughput: {:.1} MB/s", report.summary.avg_throughput_mbps);
            println!("🔍 Likely Bottleneck: {}", report.summary.bottleneck_analysis.likely_bottleneck);
            println!("💾 CPU Utilization: {:.1}%", report.summary.bottleneck_analysis.cpu_utilization);
            println!("🧠 Memory Utilization: {:.1}%", report.summary.bottleneck_analysis.memory_utilization);
            println!("🌐 Network Saturation: {:.1}%", report.summary.bottleneck_analysis.network_saturation_estimate);
            
            println!("\n💡 RECOMMENDATIONS:");
            for (i, rec) in report.summary.recommendations.iter().enumerate() {
                println!("  {}. {}", i + 1, rec);
            }
            
            // Detailed results
            println!("\n📋 DETAILED RESULTS:");
            println!("{:<30} | {:>8} | {:>6} | {:>10} | {:>8} | {:>8} | {:>6}", 
                     "Test", "FileSize", "Threads", "Throughput", "Ops/sec", "AvgLat", "Errors");
            println!("{}", "─".repeat(90));
            
            for result in &report.results {
                let size_str = if result.file_size == 0 {
                    "Mixed".to_string()
                } else if result.file_size < 1024 {
                    format!("{}B", result.file_size)
                } else if result.file_size < 1024 * 1024 {
                    format!("{}KB", result.file_size / 1024)
                } else if result.file_size < 1024 * 1024 * 1024 {
                    format!("{}MB", result.file_size / 1024 / 1024)
                } else {
                    format!("{}GB", result.file_size / 1024 / 1024 / 1024)
                };
                
                println!("{:<30} | {:>8} | {:>6} | {:>8.1} MB/s | {:>6.1} | {:>6.1}ms | {:>6}", 
                         result.test_name,
                         size_str,
                         result.concurrent_operations,
                         result.throughput_mbps,
                         result.operations_per_second,
                         result.avg_latency_ms,
                         result.errors);
            }
            
            // Save detailed report to JSON
            let report_json = serde_json::to_string_pretty(&report)?;
            let report_filename = format!("cos_benchmark_report_{}.json", 
                                        chrono::Utc::now().format("%Y%m%d_%H%M%S"));
            std::fs::write(&report_filename, report_json)?;
            println!("\n💾 Detailed report saved to: {}", report_filename);
            
            // Cleanup unless requested to skip
            if !skip_cleanup {
                benchmark.cleanup_test_objects().await?;
            }
        }
        
        Commands::Cleanup { endpoint, access_key, secret_key, bucket, prefix } => {
            let benchmark = BenchmarkSuite::new(&endpoint, &access_key, &secret_key, &bucket, &prefix).await?;
            benchmark.cleanup_test_objects().await?;
        }
    }
    
    Ok(())
}
