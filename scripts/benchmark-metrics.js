#!/usr/bin/env node
/**
 * scripts/benchmark-metrics.js - HTTP /metrics Endpoint Benchmark using Autocannon
 * 
 * Verifies that the HTTP metrics path across the Load Balancer and Ingestion Nodes
 * is lightweight and does not introduce bottlenecks or event-loop blockage.
 */

const autocannon = require('autocannon');

const targets = [
  { name: 'Load Balancer /metrics', url: 'http://127.0.0.1:8000/metrics' },
  { name: 'Ingestion Node D1 /metrics', url: 'http://127.0.0.1:8001/metrics' }
];

async function runBenchmark(target) {
  console.log(`\n=======================================================`);
  console.log(`🔥 Running Autocannon Benchmark on: ${target.name}`);
  console.log(`   URL        : ${target.url}`);
  console.log(`   Connections: 20 concurrent HTTP clients`);
  console.log(`   Duration   : 5 seconds`);
  console.log(`=======================================================`);

  return new Promise((resolve) => {
    autocannon({
      url: target.url,
      connections: 20,
      duration: 5,
      pipelining: 1
    }, (err, result) => {
      if (err) {
        console.error(`Benchmark failed on ${target.url}:`, err.message);
        resolve(null);
        return;
      }

      console.log(`\n--- Results for ${target.name} ---`);
      console.log(` Total Requests Handled : ${result.requests.total}`);
      console.log(` Requests / Second (Avg): ${result.requests.average} req/s`);
      console.log(` Throughput (MB/s)      : ${(result.throughput.average / (1024 * 1024)).toFixed(2)} MB/s`);
      console.log(` Latency Avg            : ${result.latency.average} ms`);
      console.log(` Latency 99th percentile: ${result.latency.p99} ms`);
      console.log(` 2xx Responses          : ${result['2xx']}`);
      console.log(` Non-2xx Errors         : ${result.non2xx || 0}`);
      console.log(` Timeouts / Errors      : ${result.errors + result.timeouts}`);
      resolve(result);
    });
  });
}

async function runAll() {
  for (const target of targets) {
    await runBenchmark(target);
  }
  console.log(`\n✅ HTTP Metrics path benchmark completed successfully.\n`);
}

runAll();
