# Generates the local CA and broker TLS certificates.
# Usage: .\scripts\gen-certs.ps1

$ErrorActionPreference = "Stop"
$env:PATH = "C:\Program Files\Git\usr\bin;" + $env:PATH
$env:OPENSSL_CONF = "C:\Program Files\Git\usr\ssl\openssl.cnf"

function Write-Pass {
    param([string]$Message)
    Write-Host "PASS: $Message" -ForegroundColor Green
}

function Write-Fail {
    param([string]$Message)
    Write-Host "FAIL: $Message" -ForegroundColor Red
}

function Test-OpenSSL {
    try {
        openssl version | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "OpenSSL command failed."
        }
    }
    catch {
        Write-Fail "OpenSSL was not found on PATH. Install Git for Windows or add OpenSSL to PATH."
        exit 1
    }

    Write-Pass "OpenSSL is available."
}

function New-CertificateAuthority {
    param([string]$CertDir)

    $caKey = Join-Path $CertDir "ca.key"
    $caCrt = Join-Path $CertDir "ca.crt"

    Write-Host "Generating self-signed CA..." -ForegroundColor Yellow

    Remove-Item $caKey, $caCrt -Force -ErrorAction SilentlyContinue

    openssl genrsa -out $caKey 4096

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate CA private key."
    }

    openssl req `
        -x509 `
        -new `
        -nodes `
        -key $caKey `
        -sha256 `
        -days 3650 `
        -out $caCrt `
        -subj "/C=IN/O=Distributed Event Log/OU=CA/CN=Distributed Event Log CA"

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate CA certificate."
    }

    Write-Pass "CA generated."
}

function New-BrokerCertificate {
    param(
        [string]$CertDir,
        [string]$BrokerName
    )

    $keyFile = Join-Path $CertDir "$BrokerName.key"
    $csrFile = Join-Path $CertDir "$BrokerName.csr"
    $crtFile = Join-Path $CertDir "$BrokerName.crt"
    $extFile = Join-Path $CertDir "$BrokerName.ext"
    $caCrt = Join-Path $CertDir "ca.crt"
    $caKey = Join-Path $CertDir "ca.key"

    Write-Host "Generating certificate for $BrokerName..." -ForegroundColor Yellow

    Remove-Item $keyFile, $csrFile, $crtFile, $extFile `
        -Force `
        -ErrorAction SilentlyContinue

    openssl genrsa -out $keyFile 2048

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate private key for $BrokerName."
    }

    openssl req `
        -new `
        -key $keyFile `
        -out $csrFile `
        -subj "/C=IN/O=Distributed Event Log/OU=Brokers/CN=$BrokerName"

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate CSR for $BrokerName."
    }

    @"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:$BrokerName,DNS:localhost,IP:127.0.0.1
"@ | Set-Content -Path $extFile -Encoding ascii

    openssl x509 `
        -req `
        -in $csrFile `
        -CA $caCrt `
        -CAkey $caKey `
        -CAcreateserial `
        -out $crtFile `
        -days 730 `
        -sha256 `
        -extfile $extFile

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to sign certificate for $BrokerName."
    }

    Remove-Item $csrFile, $extFile `
        -Force `
        -ErrorAction SilentlyContinue

    Write-Pass "$BrokerName certificate generated."
}

try {
    $certDir = Join-Path $PSScriptRoot "..\certs"
    $certDir = [System.IO.Path]::GetFullPath($certDir)

    if (-not (Test-Path $certDir)) {
        New-Item -ItemType Directory -Path $certDir | Out-Null
    }

    Test-OpenSSL

    New-CertificateAuthority -CertDir $certDir

    New-BrokerCertificate -CertDir $certDir -BrokerName "broker-1"
    New-BrokerCertificate -CertDir $certDir -BrokerName "broker-2"
    New-BrokerCertificate -CertDir $certDir -BrokerName "broker-3"

    Remove-Item (Join-Path $certDir "ca.srl") `
        -Force `
        -ErrorAction SilentlyContinue

    Write-Host ""
    Write-Host "PASS: All TLS certificates generated in $certDir" -ForegroundColor Green
}
catch {
    Write-Fail $_.Exception.Message
    exit 1
}