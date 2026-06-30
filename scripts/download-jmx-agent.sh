#!/usr/bin/env bash
# ============================================================
# Скачивание JMX Prometheus Java Agent
# Необходим для мониторинга Kafka в Prometheus
# ============================================================
set -euo pipefail

AGENT_VERSION="0.20.0"
AGENT_JAR="jmx_prometheus_javaagent-${AGENT_VERSION}.jar"
DOWNLOAD_URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${AGENT_VERSION}/${AGENT_JAR}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$SCRIPT_DIR/$AGENT_JAR"

if [ -f "$TARGET" ]; then
  echo "✅ JMX Prometheus Agent уже скачан: $TARGET"
  exit 0
fi

echo "==> Скачиваем JMX Prometheus Agent $AGENT_VERSION..."
echo "    URL: $DOWNLOAD_URL"
echo "    Назначение: $TARGET"

if command -v wget >/dev/null 2>&1; then
  wget -O "$TARGET" "$DOWNLOAD_URL"
elif command -v curl >/dev/null 2>&1; then
  curl -L -o "$TARGET" "$DOWNLOAD_URL"
else
  echo "❌ Ни wget, ни curl не найдены. Установите один из них."
  exit 1
fi

echo "✅ JMX Prometheus Agent успешно скачан: $TARGET"
ls -lh "$TARGET"
