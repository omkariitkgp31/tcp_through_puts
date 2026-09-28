#!/usr/bin/env node
/**
 * scripts/run-latency-benchmark.js - Automated Latency & Throughput Benchmark Harness
 * 
 * Drives staged load ramps (e.g. 50 -> 100 -> 200 -> 400 -> 800 connections)
 * through the L4 TCP Load Balancer and backend ingestion nodes.
 * 
 * Measures two distinct latency categories client-side using HDR histograms:
 *   1. T_conn: TCP handshake & routing latency (connect callback delta)
 *   2. T_data: End-to-end application RTT via seq ACK round-trips
 * 
 * Collects steady-state /metrics from the LB and Ingestion Nodes once per second,
 * produces an ASCII summary table, and exports complete benchmark telemetry to JSON.
 */

const fs = require('fs');
const path = require('path');
const http = require('http');
const { DeviceSimulator } = require('../simulator/simulate-devices');

// -------------------------------------------------------------
// 1. CLI Argument Parsing
// -------------------------------------------------------------
function parseArgs() {
  const args = process.argv.slice(2);
  const params = {
    stages: [50, 100, 200, 400, 800],
    rate: 5, // msgs/sec per device
    interval: null,
    duration: 30, // seconds steady-state hold
    warmup: 5, // seconds warm-up
    output: null,
    lbHost: '127.0.0.1',
    lbPort: 7000,
    lbHttpPort: 8000,
    nodeHttpPorts: [8001, 8002, 8003, 8004]
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--stages' && args[i + 1]) {
      params.stages = args[++i].split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n) && n > 0);
    } else if (arg === '--rate' && args[i + 1]) {
      params.rate = parseFloat(args[++i]);
    } else if (arg === '--interval' && args[i + 1]) {
      params.interval = parseInt(args[++i], 10);
    } else if (arg === '--duration' && args[i + 1]) {
      params.duration = parseInt(args[++i], 10);
    } else if (arg === '--warmup' && args[i + 1]) {
      params.warmup = parseInt(args[++i], 10);
    } else if (arg === '--output' && args[i + 1]) {
      params.output = args[++i];
    } else if (arg === '--lb-host' && args[i + 1]) {
      params.lbHost = args[++i];
    } else if (arg === '--lb-port' && args[i + 1]) {
      params.lbPort = parseInt(args[++i], 10);
    } else if (arg === '--lb-http-port' && args[i + 1]) {
      params.lbHttpPort = parseInt(args[++i], 10);
    }
  }

  if (params.interval === null) {
    params.interval = Math.max(1, Math.round(1000 / params.rate));
  }

  return params;
}

// -------------------------------------------------------------
// 2. HTTP Polling Utilities
// -------------------------------------------------------------
function fetchJson(url, timeoutMs = 1500) {
  return new Promise((resolve) => {
    const req = http.get(url, { timeout: timeoutMs }, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch {
          resolve(null);
        }
      });
    });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.on('error', () => resolve(null));
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// -------------------------------------------------------------
// 3. Main Benchmark Execution
// -------------------------------------------------------------
async function runBenchmark() {
  const config = parseArgs();

  console.log(`================================================================================`);
  console.log(`🚀 TCP LOAD BALANCER LATENCY & THROUGHPUT BENCHMARK (Phase 2)`);
  console.log(`   Load Balancer Target   : tcp://${config.lbHost}:${config.lbPort}`);
  console.log(`   LB HTTP Metrics        : http://${config.lbHost}:${config.lbHttpPort}/metrics`);
  console.log(`   Ramping Stages         : [${config.stages.join(', ')}] connections`);
  console.log(`   Heartbeat Rate         : ${config.rate} msg/sec (${config.interval} ms interval)`);
  console.log(`   Warm-Up Window         : ${config.warmup} seconds per stage (samples discarded)`);
  console.log(`   Steady-State Window    : ${config.duration} seconds per stage`);
  console.log(`================================================================================\n`);

  // Pre-flight check: Verify LB is reachable
  const lbHealth = await fetchJson(`http://${config.lbHost}:${config.lbHttpPort}/health`);
  if (!lbHealth || (lbHealth.status !== 'ok' && lbHealth.status !== 'degraded')) {
    console.error(`❌ Load Balancer is unreachable at http://${config.lbHost}:${config.lbHttpPort}/health`);
    console.error(`   Ensure PM2 ingestion nodes and the LB are running before starting the benchmark.`);
    console.error(`   e.g.: npm run start:pm2 && npm run lb\n`);
    process.exit(1);
  }

  // Pre-flight check: Verify Ingestion Nodes
  const nodeHealths = await Promise.all(
    config.nodeHttpPorts.map(port => fetchJson(`http://127.0.0.1:${port}/health`))
  );
  const activeNodes = nodeHealths.filter(h => h && h.status === 'ok');
  if (activeNodes.length === 0) {
    console.error(`❌ No healthy ingestion nodes detected on ports: ${config.nodeHttpPorts.join(', ')}`);
    process.exit(1);
  }
  console.log(`✅ Pre-flight checks passed: LB online with ${activeNodes.length} backend node(s).\n`);

  // Initialize shared simulator instance to ramp up across stages
  const initialTarget = config.stages[0];
  const simulator = new DeviceSimulator({
    host: config.lbHost,
    port: config.lbPort,
    targetConnections: initialTarget,
    intervalMs: config.interval,
    rampRate: 150,
    collectingSamples: true
  });

  simulator.start();

  const stageResults = [];
  const startTimeIso = new Date().toISOString();

  // Iterate through configured stages
  for (let sIdx = 0; sIdx < config.stages.length; sIdx++) {
    const targetConns = config.stages[sIdx];
    console.log(`▶ [Stage ${sIdx + 1}/${config.stages.length}] Target: ${targetConns} Connections`);

    // Reset stage-specific histograms for clean per-stage measurement
    simulator.resetStageHistograms();

    // Scale simulator to target connections
    if (simulator.targetConnections !== targetConns) {
      simulator.scaleTo(targetConns);
    }

    // Wait for target connections to establish (up to 30s)
    const establishedAtStart = simulator.established;
    process.stdout.write(`  ⏳ Scaling connections from ${establishedAtStart} to ${targetConns}...`);
    await simulator.waitForConnections(targetConns, 30000);
    console.log(` Established: ${simulator.established}/${targetConns}`);

    // Warm-up window: discard samples generated during warmup
    simulator.collectingSamples = false;
    for (let w = 1; w <= config.warmup; w++) {
      process.stdout.write(`\r  🔥 Warm-up (${w}/${config.warmup}s) — discarding transitional samples...`);
      await sleep(1000);
    }
    process.stdout.write(`\r  🔥 Warm-up complete (${config.warmup}s). Resetting steady-state histograms.        \n`);

    // Reset stage T_data histogram to discard any stray warmup ACKs
    simulator.stageTDataHist.reset();
    simulator.collectingSamples = true;

    // Snapshot node metrics at start of steady-state
    const initialNodeMetrics = await Promise.all(
      config.nodeHttpPorts.map(port => fetchJson(`http://127.0.0.1:${port}/metrics`))
    );
    const initialLbMetrics = await fetchJson(`http://${config.lbHost}:${config.lbHttpPort}/metrics`);
    const steadyStartNs = process.hrtime.bigint();

    // Steady-state monitoring window (sample /metrics once per second)
    const periodicSamples = [];

    for (let sec = 1; sec <= config.duration; sec++) {
      await sleep(1000);

      // Poll LB and node metrics
      const [lbMetrics, ...nodeMetrics] = await Promise.all([
        fetchJson(`http://${config.lbHost}:${config.lbHttpPort}/metrics`),
        ...config.nodeHttpPorts.map(port => fetchJson(`http://127.0.0.1:${port}/metrics`))
      ]);

      const validNodes = nodeMetrics.filter(Boolean);
      const totalNodeMps = validNodes.reduce((acc, n) => acc + (n.messagesPerSec || 0), 0);
      const totalNodeActive = validNodes.reduce((acc, n) => acc + (n.activeConnections || 0), 0);
      const currentStagePercentiles = simulator.getStagePercentiles();

      periodicSamples.push({
        second: sec,
        lbMetrics,
        nodeMetrics: validNodes,
        totalNodeMps,
        totalNodeActive
      });

      // Live progress line
      process.stdout.write(
        `\r  ⏱ Hold (${sec}/${config.duration}s) | ` +
        `Active Held: ${simulator.established.toString().padStart(4)} | ` +
        `MPS: ${totalNodeMps.toString().padStart(5)} | ` +
        `T_data P50: ${(currentStagePercentiles.tData.p50 || 0).toFixed(2)}ms | ` +
        `T_data P99: ${(currentStagePercentiles.tData.p99 || 0).toFixed(2)}ms`
      );
    }

    const steadyElapsedSec = Number(process.hrtime.bigint() - steadyStartNs) / 1e9;
    process.stdout.write(`\r  ✔ Steady-state complete (${steadyElapsedSec.toFixed(1)}s). Computing stage metrics...        \n`);

    // Snapshot node metrics at end of steady-state
    const finalNodeMetrics = await Promise.all(
      config.nodeHttpPorts.map(port => fetchJson(`http://127.0.0.1:${port}/metrics`))
    );
    const finalLbMetrics = await fetchJson(`http://${config.lbHost}:${config.lbHttpPort}/metrics`);

    // Calculate per-node and aggregate throughput
    const perNodeStats = {};
    let aggregateDeltaMsgs = 0;
    let aggregateDeltaBytes = 0;

    for (let i = 0; i < config.nodeHttpPorts.length; i++) {
      const port = config.nodeHttpPorts[i];
      const startNode = initialNodeMetrics[i];
      const endNode = finalNodeMetrics[i];
      const nodeId = endNode?.nodeId || startNode?.nodeId || `D${i + 1}`;

      if (startNode && endNode) {
        const deltaMsgs = Math.max(0, (endNode.totalMessages || 0) - (startNode.totalMessages || 0));
        const deltaBytes = Math.max(0, (endNode.totalBytes || 0) - (startNode.totalBytes || 0));
        const mps = +(deltaMsgs / steadyElapsedSec).toFixed(1);
        const mbPerSec = +(deltaBytes / (1024 * 1024 * steadyElapsedSec)).toFixed(3);

        aggregateDeltaMsgs += deltaMsgs;
        aggregateDeltaBytes += deltaBytes;

        // Average active connections during steady-state
        const nodeActiveSamples = periodicSamples
          .map(s => s.nodeMetrics.find(n => n.nodeId === nodeId)?.activeConnections)
          .filter(v => typeof v === 'number');
        const avgActive = nodeActiveSamples.length > 0
          ? +(nodeActiveSamples.reduce((a, b) => a + b, 0) / nodeActiveSamples.length).toFixed(1)
          : endNode.activeConnections || 0;

        perNodeStats[nodeId] = {
          port,
          mps,
          mbPerSec,
          activeConnections: avgActive,
          cpuPercent: endNode.cpuPercent || 0,
          rssMB: endNode.memory?.rssMB || 0
        };
      }
    }

    const aggregateReqPerSec = +(aggregateDeltaMsgs / steadyElapsedSec).toFixed(1);
    const aggregateMBPerSec = +(aggregateDeltaBytes / (1024 * 1024 * steadyElapsedSec)).toFixed(2);

    // Sum of backend active connections
    const sumBackendActive = Object.values(perNodeStats).reduce((acc, n) => acc + n.activeConnections, 0);

    // Sanity check: sum of per-node active connections vs target connection count
    const divergence = Math.abs(sumBackendActive - targetConns);
    const divergenceRatio = divergence / targetConns;
    if (divergenceRatio > 0.15) {
      console.warn(
        `  ⚠️  Sanity Warning: Sum of backend active connections (${sumBackendActive}) ` +
        `diverges from target (${targetConns}) by ${(divergenceRatio * 100).toFixed(1)}%`
      );
    }

    const stagePercentiles = simulator.getStagePercentiles();
    const stageErrors = simulator.getErrorCounts();

    const stageResult = {
      stage: sIdx + 1,
      connections: targetConns,
      activeHeld: simulator.established,
      sumBackendActive: Math.round(sumBackendActive),
      durationSec: +steadyElapsedSec.toFixed(1),
      aggregateReqPerSec,
      aggregateMBPerSec,
      perNodeThroughput: perNodeStats,
      tDataLatencyMs: stagePercentiles.tData,
      tConnLatencyMs: stagePercentiles.tConn,
      errors: stageErrors,
      sanityPassed: divergenceRatio <= 0.15
    };

    stageResults.push(stageResult);
    console.log(`  Stage summary: Req/s: ${aggregateReqPerSec} | MB/s: ${aggregateMBPerSec} | T_data P50: ${stagePercentiles.tData.p50}ms | T_conn P50: ${stagePercentiles.tConn.p50}ms\n`);
  }

  // Teardown simulator
  simulator.stop();

  // -------------------------------------------------------------
  // 4. Print Terminal Summary Tables
  // -------------------------------------------------------------
  console.log(`\n========================================================================================================================`);
  console.log(`📊 BENCHMARK SUMMARY TABLE: APPLICATION RTT (T_data) & THROUGHPUT`);
  console.log(`========================================================================================================================`);
  console.log(`Stage (Connections) | Req/s      | MB/s    | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) | P99.9 (ms) | P99.99 (ms) | Errors/Timeouts`);
  console.log(`------------------------------------------------------------------------------------------------------------------------`);

  for (const res of stageResults) {
    const colStage = `${res.connections} conns`.padEnd(19);
    const colReq = `${res.aggregateReqPerSec}`.padEnd(10);
    const colMB = `${res.aggregateMBPerSec}`.padEnd(7);
    const colP50 = `${res.tDataLatencyMs.p50}`.padEnd(8);
    const colP90 = `${res.tDataLatencyMs.p90}`.padEnd(8);
    const colP95 = `${res.tDataLatencyMs.p95}`.padEnd(8);
    const colP99 = `${res.tDataLatencyMs.p99}`.padEnd(8);
    const colP999 = `${res.tDataLatencyMs.p99_9}`.padEnd(10);
    const colP9999 = `${res.tDataLatencyMs.p99_99}`.padEnd(11);
    const colErrors = `${res.errors.totalErrors} (RST:${res.errors.ECONNRESET}, PIPE:${res.errors.EPIPE}, TIMEOUT:${res.errors.ETIMEDOUT})`;

    console.log(`${colStage} | ${colReq} | ${colMB} | ${colP50} | ${colP90} | ${colP95} | ${colP99} | ${colP999} | ${colP9999} | ${colErrors}`);
  }
  console.log(`========================================================================================================================\n`);

  console.log(`========================================================================================================================`);
  console.log(`🔌 BENCHMARK SUMMARY TABLE: TCP HANDSHAKE & ROUTING LATENCY (T_conn)`);
  console.log(`========================================================================================================================`);
  console.log(`Stage (Connections) | Samples | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) | P99.9 (ms) | P99.99 (ms) | Active Sockets Held`);
  console.log(`------------------------------------------------------------------------------------------------------------------------`);

  for (const res of stageResults) {
    const colStage = `${res.connections} conns`.padEnd(19);
    const colCount = `${res.tConnLatencyMs.count}`.padEnd(7);
    const colP50 = `${res.tConnLatencyMs.p50}`.padEnd(8);
    const colP90 = `${res.tConnLatencyMs.p90}`.padEnd(8);
    const colP95 = `${res.tConnLatencyMs.p95}`.padEnd(8);
    const colP99 = `${res.tConnLatencyMs.p99}`.padEnd(8);
    const colP999 = `${res.tConnLatencyMs.p99_9}`.padEnd(10);
    const colP9999 = `${res.tConnLatencyMs.p99_99}`.padEnd(11);
    const colActive = `${res.activeHeld}/${res.connections}`;

    console.log(`${colStage} | ${colCount} | ${colP50} | ${colP90} | ${colP95} | ${colP99} | ${colP999} | ${colP9999} | ${colActive}`);
  }
  console.log(`========================================================================================================================\n`);

  // -------------------------------------------------------------
  // 5. Save Raw Results to JSON
  // -------------------------------------------------------------
  const resultsDir = path.resolve(__dirname, '..', 'benchmark-results');
  if (!fs.existsSync(resultsDir)) {
    fs.mkdirSync(resultsDir, { recursive: true });
  }

  const defaultFileName = `${startTimeIso.replace(/:/g, '-')}-latency.json`;
  const outputPath = config.output
    ? path.resolve(process.cwd(), config.output)
    : path.join(resultsDir, defaultFileName);

  const globalPercentiles = simulator.getPercentiles();
  const fullReport = {
    timestamp: startTimeIso,
    metadata: {
      lbEndpoint: `${config.lbHost}:${config.lbPort}`,
      stages: config.stages,
      targetRatePerDevice: config.rate,
      intervalMs: config.interval,
      warmupSec: config.warmup,
      holdDurationSec: config.duration,
      nodesMonitored: config.nodeHttpPorts.map((p, i) => `D${i + 1} (:700${i + 1} / :${p})`)
    },
    stages: stageResults,
    global: {
      tDataLatencyMs: globalPercentiles.tData,
      tConnLatencyMs: globalPercentiles.tConn,
      errors: simulator.getErrorCounts()
    }
  };

  fs.writeFileSync(outputPath, JSON.stringify(fullReport, null, 2), 'utf8');
  console.log(`💾 Raw benchmark telemetry successfully written to:\n   ${outputPath}\n`);

  return fullReport;
}

if (require.main === module) {
  runBenchmark().catch(err => {
    console.error('Benchmark runner failed with error:', err);
    process.exit(1);
  });
}

module.exports = { runBenchmark };
