$ErrorActionPreference = "Stop"

$TestStartTime = Get-Date
$ProjectRoot = (Get-Location).Path
$CertDir = Join-Path $ProjectRoot "certs"

$BrokerPorts = @{
    "broker-1" = 9081
    "broker-2" = 9082
    "broker-3" = 9083
}

function Require-Path {
    param([string]$Path, [string]$Description)

    if (-not (Test-Path $Path)) {
        throw "FAIL: $Description is missing: $Path"
    }

    Write-Host "PASS: $Description" -ForegroundColor Green
}

function Invoke-Cli {
    param([string[]]$Arguments)

    $output = & go run ./cmd/cli @Arguments 2>&1

    if ($LASTEXITCODE -ne 0) {
        throw "CLI failed:`n$($output -join "`n")"
    }

    return ($output -join "`n").Trim()
}

function Get-LeaderFromLogs {
    param(
        [hashtable]$IpMap,
        [DateTime]$Since
    )

    $logs = & docker compose logs --no-log-prefix broker-1 broker-2 broker-3 2>&1

    $leader = $null

    foreach ($line in $logs) {
        if ($line -match '^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s+') {
            $timestamp = [DateTime]::Parse($Matches[1]).ToUniversalTime()

            if ($timestamp -lt $Since.ToUniversalTime()) {
                continue
            }

            if ($line -match 'raft: entering leader state: leader="Node at ([0-9.]+):\d+') {
                $ip = $Matches[1]

                if ($IpMap.ContainsKey($ip)) {
                    $leader = $IpMap[$ip]
                }
            }
        }
    }

    return $leader
}

function Build-IpMap {
    $ipMap = @{}
    foreach ($n in 1..3) {
        $containerId = (& docker compose ps -q broker-$n).Trim()
        if (-not $containerId) { continue }
        $ip = (& docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $containerId).Trim()
        if ($ip) { $ipMap[$ip] = "broker-$n" }
    }
    return $ipMap
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Distributed Event Log Integration Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Certificates
Require-Path (Join-Path $CertDir "ca.crt") "CA certificate"
foreach ($n in 1..3) {
    Require-Path (Join-Path $CertDir "broker-$n.crt") "broker-$n certificate"
    Require-Path (Join-Path $CertDir "broker-$n.key") "broker-$n private key"
}

# Pre-flight: ensure all brokers are running
foreach ($n in 1..3) {
    $state = (& docker compose ps --format '{{.Service}}|{{.State}}' |
        Where-Object { $_ -match "^broker-$n\|" }) -replace "^broker-$n\|", ""
    if ($state -ne "running") {
        Write-Host "broker-$n is '$state' - recreating..." -ForegroundColor Yellow
        & docker compose up -d broker-$n | Out-Null
        Start-Sleep -Seconds 5
    }
}

# 2. Docker services
Write-Host "[1/7] Checking Docker services..." -ForegroundColor Yellow

$services = & docker compose ps --format '{{.Service}}|{{.Status}}'
foreach ($service in @("broker-1", "broker-2", "broker-3", "prometheus", "grafana")) {
    $running = $services | Where-Object { $_ -match "^$([regex]::Escape($service))\|Up" }
    if (-not $running) {
        throw "FAIL: $service is not running"
    }
    Write-Host "PASS: $service is running" -ForegroundColor Green
}

# 3. Leader election
Write-Host ""
Write-Host "[2/7] Checking Raft leader election..." -ForegroundColor Yellow

$ipMap = Build-IpMap

$leader = $null
$deadline = (Get-Date).AddSeconds(15)
while ((Get-Date) -lt $deadline) {
    $leader = Get-LeaderFromLogs -IpMap $ipMap -Since ([DateTime]::MinValue)
    if ($leader) { break }
    Start-Sleep -Milliseconds 500
}

if (-not $leader) {
    throw "FAIL: No Raft leader elected within 15 seconds"
}

Write-Host "PASS: Raft leader is $leader" -ForegroundColor Green

# 4. gRPC mTLS + produce
Write-Host ""
Write-Host "[3/7] Testing gRPC mTLS + Produce..." -ForegroundColor Yellow

$leaderPort = $BrokerPorts[$leader]
$leaderCert = Join-Path $CertDir "$leader.crt"
$leaderKey = Join-Path $CertDir "$leader.key"
$caCert = Join-Path $CertDir "ca.crt"

$produceOutput = Invoke-Cli @(
    "produce",
    "--addr", "localhost:$leaderPort",
    "--topic", "integration-test",
    "--partition", "0",
    "--msg", "phase3-integration-test",
    "--cert", $leaderCert,
    "--key", $leaderKey,
    "--ca", $caCert
)

[uint64]$producedOffset = 0
if (-not [uint64]::TryParse($produceOutput, [ref]$producedOffset)) {
    throw "FAIL: Produce returned a non-numeric offset: $produceOutput"
}

Write-Host "PASS: mTLS Produce succeeded (offset $producedOffset)" -ForegroundColor Green

# 5. Consume
Write-Host ""
Write-Host "[4/7] Testing partitioned Consume..." -ForegroundColor Yellow

$consumeOutput = Invoke-Cli @(
    "consume",
    "--addr", "localhost:$leaderPort",
    "--topic", "integration-test",
    "--partition", "0",
    "--offset", "$producedOffset",
    "--cert", $leaderCert,
    "--key", $leaderKey,
    "--ca", $caCert
)

if ($consumeOutput -notmatch 'value:\s*phase3-integration-test') {
    throw "FAIL: Consumed value did not match expected message"
}

Write-Host "PASS: Message consumed from partition 0" -ForegroundColor Green

# 6. Prometheus metrics
Write-Host ""
Write-Host "[5/7] Testing Prometheus metrics..." -ForegroundColor Yellow

$metrics = Invoke-WebRequest -Uri "http://localhost:8081/metrics" -UseBasicParsing

if ($metrics.StatusCode -ne 200) {
    throw "FAIL: /metrics returned HTTP $($metrics.StatusCode)"
}

foreach ($metric in @(
    "event_log_messages_produced_total",
    "event_log_messages_consumed_total"
)) {
    if ($metrics.Content -notmatch [regex]::Escape($metric)) {
        throw "FAIL: Metric $metric was not exposed"
    }
    Write-Host "PASS: $metric exposed" -ForegroundColor Green
}

# 7. Consumer group
Write-Host ""
Write-Host "[6/7] Testing consumer-group commit/fetch..." -ForegroundColor Yellow

$GroupId = "integration-test-group-$(Get-Date -Format 'yyyyMMddHHmmss')"

$groupOutput = Invoke-Cli @(
    "consume-group",
    "--addr", "localhost:$leaderPort",
    "--group", $GroupId,
    "--topic", "integration-test",
    "--partition", "0",
    "--cert", $leaderCert,
    "--key", $leaderKey,
    "--ca", $caCert
)

if ($groupOutput -notmatch 'value:\s*phase3-integration-test') {
    throw "FAIL: Consumer group did not consume the expected message"
}

$fetchResponse = Invoke-RestMethod `
    -Uri "http://localhost:8081/fetch-offset?group_id=$GroupId&topic=integration-test&partition=0" `
    -Method Get

if ([uint64]$fetchResponse.offset -ne ($producedOffset + 1)) {
    throw "FAIL: Consumer group committed offset $($fetchResponse.offset), expected $($producedOffset + 1)"
}

Write-Host "PASS: Consumer group committed next offset" -ForegroundColor Green

# 8. Leader failover and recovery
Write-Host ""
Write-Host "[7/7] Testing leader failure and re-election..." -ForegroundColor Yellow

$leaderContainerId = (& docker compose ps -q $leader).Trim()
if (-not $leaderContainerId) {
    throw "FAIL: Could not resolve leader container ID"
}

Write-Host "Stopping $leader..." -ForegroundColor Yellow
docker stop $leaderContainerId | Out-Null

$newLeader = $null
$deadline = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $deadline) {
    $candidate = Get-LeaderFromLogs -IpMap $ipMap -Since $TestStartTime
    if ($candidate -and $candidate -ne $leader) {
        $newLeader = $candidate
        break
    }
    Start-Sleep -Milliseconds 500
}

if (-not $newLeader) {
    throw "FAIL: No new Raft leader elected after stopping $leader"
}

Write-Host "PASS: New Raft leader is $newLeader" -ForegroundColor Green

$newLeaderPort = $BrokerPorts[$newLeader]
$newLeaderCert = Join-Path $CertDir "$newLeader.crt"
$newLeaderKey = Join-Path $CertDir "$newLeader.key"

$recoveryOutput = Invoke-Cli @(
    "produce",
    "--addr", "localhost:$newLeaderPort",
    "--topic", "recovery-test",
    "--partition", "0",
    "--msg", "leader-failover-success",
    "--cert", $newLeaderCert,
    "--key", $newLeaderKey,
    "--ca", $caCert
)

[uint64]$recoveryOffset = 0
if (-not [uint64]::TryParse($recoveryOutput, [ref]$recoveryOffset)) {
    throw "FAIL: Produce after leader failover returned: $recoveryOutput"
}

Write-Host "PASS: Produce succeeded after leader failover (offset $recoveryOffset)" -ForegroundColor Green

Write-Host "Restarting $leader..." -ForegroundColor Yellow
docker start $leaderContainerId | Out-Null

Start-Sleep -Seconds 5

$runningState = (& docker inspect -f "{{.State.Running}}" $leaderContainerId).Trim()
if ($runningState -ne "true") {
    throw "FAIL: $leader did not restart"
}

Write-Host "PASS: $leader restarted successfully" -ForegroundColor Green

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " ALL INTEGRATION TESTS PASSED" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""