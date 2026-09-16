const path = require('path');

module.exports = {
  apps: [
    {
      name: 'ingestion-d1',
      script: path.resolve(__dirname, 'ingestion-node', 'node.js'),
      cwd: __dirname,
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      args: '--node-id D1 --tcp-port 7001 --http-port 8001',
      env: {
        NODE_ENV: 'production',
        NODE_ID: 'D1',
        TCP_PORT: 7001,
        HTTP_PORT: 8001
      }
    },
    {
      name: 'ingestion-d2',
      script: path.resolve(__dirname, 'ingestion-node', 'node.js'),
      cwd: __dirname,
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      args: '--node-id D2 --tcp-port 7002 --http-port 8002',
      env: {
        NODE_ENV: 'production',
        NODE_ID: 'D2',
        TCP_PORT: 7002,
        HTTP_PORT: 8002
      }
    },
    {
      name: 'ingestion-d3',
      script: path.resolve(__dirname, 'ingestion-node', 'node.js'),
      cwd: __dirname,
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      args: '--node-id D3 --tcp-port 7003 --http-port 8003',
      env: {
        NODE_ENV: 'production',
        NODE_ID: 'D3',
        TCP_PORT: 7003,
        HTTP_PORT: 8003
      }
    },
    {
      name: 'ingestion-d4',
      script: path.resolve(__dirname, 'ingestion-node', 'node.js'),
      cwd: __dirname,
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      args: '--node-id D4 --tcp-port 7004 --http-port 8004',
      env: {
        NODE_ENV: 'production',
        NODE_ID: 'D4',
        TCP_PORT: 7004,
        HTTP_PORT: 8004
      }
    }
  ]
};

