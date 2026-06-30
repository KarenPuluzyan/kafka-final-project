#!/usr/bin/env bash
# ============================================================
# Регистрация Kafka Connect коннектора (File Sink)
# Шаг 5 финального проекта: хранение отфильтрованных данных
# Базовый вариант: запись в файл для отладки
# ============================================================
set -euo pipefail

CONNECT_URL=${CONNECT_URL:-"http://localhost:8083"}
OUTPUT_FILE=${OUTPUT_FILE:-"/data/filtered-products.json"}

echo "⏳ Ждём готовности Kafka Connect ($CONNECT_URL)..."
until curl -sf "$CONNECT_URL/" > /dev/null 2>&1; do
  echo "   Connect не готов, ожидаем 5 сек..."
  sleep 5
done

echo "✅ Kafka Connect готов!"
echo ""
echo "==> Регистрируем File Sink Connector..."

curl -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "name": "products-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "products-filtered",
    "file": "${OUTPUT_FILE}",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
EOF

echo ""
echo "==> Проверяем статус коннектора..."
sleep 3
curl -s "$CONNECT_URL/connectors/products-file-sink/status" | python3 -m json.tool

echo ""
echo "✅ File Sink Connector зарегистрирован!"
echo "   Данные записываются в: $OUTPUT_FILE"
echo ""
echo "📄 Для просмотра записанных данных:"
echo "   tail -f $OUTPUT_FILE"
