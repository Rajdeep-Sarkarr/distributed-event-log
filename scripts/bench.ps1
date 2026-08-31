$ErrorActionPreference = "Stop"

Write-Host "Running Go in-process benchmarks..." -ForegroundColor Cyan
go test -bench=. -benchtime=5s -benchmem distributed-event-log/internal/bench

Write-Host "`nChecking for ghz utility..." -ForegroundColor Cyan
if (-not (Get-Command ghz -ErrorAction SilentlyContinue)) {
    Write-Warning "ghz tool not found in PATH. Install with: go install github.com/bojand/ghz/cmd/ghz@latest"
    exit 1
}

$protoPath = "internal/proto/broker.proto"
if (-not (Test-Path $protoPath)) {
    $protoPath = "proto/broker.proto"
}

$certDir = Join-Path $PSScriptRoot "..\certs"

# Runs with client certificates enabled
Write-Host "Running ghz gRPC load benchmark (10k RPCs, 10 concurrency)..." -ForegroundColor Cyan
ghz `
    --cacert="$certDir\ca.crt" `
    --cert="$certDir\broker-1.crt" `
    --key="$certDir\broker-1.key" `
    --proto=$protoPath `
    --call=broker.BrokerService.Produce `
    -d '{"topic":"bench-load","key":"bXlrZXk=","value":"bXl2YWx1ZQ=="}' `
    -n 10000 `
    -c 10 `
    localhost:9083