# Starts the local distributed event-log Docker cluster.
# Usage: .\start.ps1

$ErrorActionPreference = "Stop"

function Write-Pass {
    param([string]$Message)
    Write-Host "PASS: $Message" -ForegroundColor Green
}

function Write-Fail {
    param([string]$Message)
    Write-Host "FAIL: $Message" -ForegroundColor Red
}

function Test-DockerDesktop {
    try {
        docker info | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Docker CLI could not reach the Docker daemon."
        }
    }
    catch {
        Write-Fail "Docker Desktop is not running. Start Docker Desktop and run this script again."
        exit 1
    }

    Write-Pass "Docker Desktop is running."
}

function Ensure-Certificates {
    $certDir = Join-Path $PSScriptRoot "certs"
    $caCert = Join-Path $certDir "ca.crt"

    if ((Test-Path $certDir) -and (Test-Path $caCert)) {
        Write-Pass "TLS certificates found."
        return
    }

    Write-Host "TLS certificates not found. Generating them..." -ForegroundColor Yellow

    $generator = Join-Path $PSScriptRoot "scripts\gen-certs.ps1"

    if (-not (Test-Path $generator)) {
        Write-Fail "Certificate generator not found: $generator"
        exit 1
    }

    & pwsh -ExecutionPolicy Bypass -File $generator

    if ($LASTEXITCODE -ne 0) {
        Write-Fail "Certificate generation failed."
        exit 1
    }

    if (-not (Test-Path $caCert)) {
        Write-Fail "Certificate generation completed but certs\ca.crt was not created."
        exit 1
    }

    Write-Pass "TLS certificates generated."
}

function Start-Cluster {
    Write-Host "Starting Docker Compose cluster..." -ForegroundColor Yellow

    & docker compose up -d

    if ($LASTEXITCODE -ne 0) {
        Write-Fail "docker compose up -d failed."
        exit 1
    }

    Write-Pass "Docker Compose cluster started."
}

function Wait-ForBrokerHealth {
    $urls = @(
        "http://localhost:8081/metrics",
        "http://localhost:8082/metrics",
        "http://localhost:8083/metrics"
    )

    $deadline = (Get-Date).AddSeconds(30)

    while ((Get-Date) -lt $deadline) {
        $allReady = $true

        foreach ($url in $urls) {
            try {
                $response = Invoke-WebRequest `
                    -Uri $url `
                    -UseBasicParsing `
                    -TimeoutSec 2

                if ($response.StatusCode -ne 200) {
                    $allReady = $false
                    break
                }
            }
            catch {
                $allReady = $false
                break
            }
        }

        if ($allReady) {
            foreach ($url in $urls) {
                Write-Pass "$url is healthy."
            }

            return
        }

        Start-Sleep -Seconds 1
    }

    Write-Fail "Cluster did not become ready within 30 seconds."
    Write-Host "Check broker logs with: docker compose logs --no-log-prefix broker-1 broker-2 broker-3" -ForegroundColor Yellow
    exit 1
}

Test-DockerDesktop
Ensure-Certificates
Start-Cluster
Wait-ForBrokerHealth

Write-Host ""
Write-Host "Cluster ready." -ForegroundColor Green