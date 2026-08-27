#!/usr/bin/env bash

set -euo pipefail

CERT_DIR="certs"

mkdir -p "$CERT_DIR"

echo "Generating self-signed CA..."

openssl genrsa \
    -out "$CERT_DIR/ca.key" \
    4096

openssl req \
    -x509 \
    -new \
    -nodes \
    -key "$CERT_DIR/ca.key" \
    -sha256 \
    -days 3650 \
    -out "$CERT_DIR/ca.crt" \
    -subj "/C=IN/O=Distributed Event Log/OU=CA/CN=Distributed Event Log CA"

generate_broker_cert() {
    local broker="$1"

    echo "Generating certificate for ${broker}..."

    openssl genrsa \
        -out "$CERT_DIR/${broker}.key" \
        2048

    openssl req \
        -new \
        -key "$CERT_DIR/${broker}.key" \
        -out "$CERT_DIR/${broker}.csr" \
        -subj "/C=IN/O=Distributed Event Log/OU=Brokers/CN=${broker}"

    cat > "$CERT_DIR/${broker}.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:broker-1,DNS:broker-2,DNS:broker-3,DNS:localhost,IP:127.0.0.1
EOF

    openssl x509 \
        -req \
        -in "$CERT_DIR/${broker}.csr" \
        -CA "$CERT_DIR/ca.crt" \
        -CAkey "$CERT_DIR/ca.key" \
        -CAcreateserial \
        -out "$CERT_DIR/${broker}.crt" \
        -days 730 \
        -sha256 \
        -extfile "$CERT_DIR/${broker}.ext"

    rm -f \
        "$CERT_DIR/${broker}.csr" \
        "$CERT_DIR/${broker}.ext"
}

generate_broker_cert "broker-1"
generate_broker_cert "broker-2"
generate_broker_cert "broker-3"

rm -f "$CERT_DIR/ca.srl"

echo
echo "TLS certificates generated successfully in ${CERT_DIR}/"
echo
ls -1 "$CERT_DIR"