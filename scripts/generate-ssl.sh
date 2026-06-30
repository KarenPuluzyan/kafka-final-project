#!/usr/bin/env bash
# ============================================================
# Генерация SSL-сертификатов для ЦОД-1 (Москва)
# Только ЦОД-1 использует TLS — ЦОД-2 работает в закрытой сети
# ============================================================
set -euo pipefail

SSL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../ssl/dc1" && pwd)"
PASSWORD="kafka-ssl-password"
VALIDITY=3650

echo "==> Создаём директорию: $SSL_DIR"
mkdir -p "$SSL_DIR"
cd "$SSL_DIR"

echo "==> Генерируем корневой CA для ЦОД-1..."
openssl req -new -x509 -keyout ca-key -out ca-cert -days $VALIDITY \
  -passout pass:$PASSWORD \
  -subj "/C=RU/ST=Moscow/L=Moscow/O=Marketplace/OU=DC1/CN=dc1-kafka-ca"

echo "$PASSWORD" > ssl_credentials

for broker in kafka-1 kafka-2 kafka-3; do
  echo "==> Сертификат для $broker..."
  keytool -genkey -noprompt -alias "$broker" \
    -dname "CN=$broker,OU=DC1,O=Marketplace,L=Moscow,ST=Moscow,C=RU" \
    -keystore "$broker.keystore.jks" -keyalg RSA \
    -storepass $PASSWORD -keypass $PASSWORD -validity $VALIDITY
  keytool -certreq -alias "$broker" \
    -keystore "$broker.keystore.jks" -file "$broker.csr" -storepass $PASSWORD
  openssl x509 -req -CA ca-cert -CAkey ca-key \
    -in "$broker.csr" -out "$broker-signed.crt" \
    -days $VALIDITY -CAcreateserial -passin pass:$PASSWORD
  keytool -import -noprompt -alias ca-root -file ca-cert \
    -keystore "$broker.keystore.jks" -storepass $PASSWORD
  keytool -import -noprompt -alias "$broker" -file "$broker-signed.crt" \
    -keystore "$broker.keystore.jks" -storepass $PASSWORD
  keytool -import -noprompt -alias ca-root -file ca-cert \
    -keystore kafka.truststore.jks -storepass $PASSWORD 2>/dev/null || true
done

for client in kafka-producer kafka-consumer; do
  echo "==> Сертификат для клиента $client..."
  keytool -genkey -noprompt -alias "$client" \
    -dname "CN=$client,OU=DC1,O=Marketplace,L=Moscow,ST=NH,C=RU" \
    -keystore "$client.keystore.jks" -keyalg RSA \
    -storepass $PASSWORD -keypass $PASSWORD -validity $VALIDITY
  keytool -certreq -alias "$client" \
    -keystore "$client.keystore.jks" -file "$client.csr" -storepass $PASSWORD
  openssl x509 -req -CA ca-cert -CAkey ca-key \
    -in "$client.csr" -out "$client-signed.crt" \
    -days $VALIDITY -CAcreateserial -passin pass:$PASSWORD
  keytool -import -noprompt -alias ca-root -file ca-cert \
    -keystore "$client.keystore.jks" -storepass $PASSWORD
  keytool -import -noprompt -alias "$client" -file "$client-signed.crt" \
    -keystore "$client.keystore.jks" -storepass $PASSWORD
  keytool -importkeystore \
    -srckeystore "$client.keystore.jks" -destkeystore "$client.p12" \
    -deststoretype PKCS12 -srcstorepass $PASSWORD -deststorepass $PASSWORD -noprompt 2>/dev/null
  openssl pkcs12 -in "$client.p12" -nocerts -nodes \
    -out "$client.key" -passin pass:$PASSWORD
  keytool -import -noprompt -alias ca-root -file ca-cert \
    -keystore "$client.truststore.jks" -storepass $PASSWORD 2>/dev/null || true
done

echo ""
echo "✅ SSL-сертификаты ЦОД-1 сгенерированы в: $SSL_DIR"
echo "   Пароль: $PASSWORD"
ls -la "$SSL_DIR"
