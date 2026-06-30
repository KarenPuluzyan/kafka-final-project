#!/usr/bin/env bash
# ============================================================
# ЦОД-1 (Москва): создание топиков и ACL
# Брокеры: kafka-1:29092, kafka-2:29094, kafka-3:29096
# ZooKeeper: zk1-1:2181, zk1-2:2181, zk1-3:2181
# ============================================================
set -euo pipefail

BS="kafka-1:29092,kafka-2:29094,kafka-3:29096"
RF=3
MIN_ISR=2

echo "══════════════════════════════════════════"
echo "  ЦОД-1 Москва — инициализация топиков"
echo "══════════════════════════════════════════"
echo "⏳ Ожидаем брокеров..."
sleep 15

create_topic() {
  local name=$1 parts=${2:-3} rf=${3:-$RF} min_isr=${4:-$MIN_ISR}
  kafka-topics --bootstrap-server $BS \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor "$rf" \
    --config min.insync.replicas="$min_isr"
  echo "  ✅ $name (partitions=$parts, RF=$rf, min.isr=$min_isr)"
}

echo ""
echo "==> Создаём топики в ЦОД-1..."

create_topic "products"                 3 3 2   # входящие товары от SHOP API
create_topic "products-filtered"        3 3 2   # разрешённые товары (после Faust)
create_topic "client-requests"          3 3 2   # запросы от CLIENT API
create_topic "recommendations"          3 3 2   # персонализированные рекомендации
create_topic "banned-products-commands" 1 3 2   # CLI-команды для управления бан-листом
create_topic "analytics-events"         3 3 2   # аналитические события

echo ""
echo "==> Настраиваем ACL..."

# Продюсеры (SHOP API) могут писать в products и client-requests
kafka-acls --bootstrap-server $BS --add \
  --allow-principal "User:CN=kafka-producer,OU=Kafka,O=Marketplace,L=Moscow,ST=NH,C=RU" \
  --operation Write --operation Describe \
  --topic products
kafka-acls --bootstrap-server $BS --add \
  --allow-principal "User:CN=kafka-producer,OU=Kafka,O=Marketplace,L=Moscow,ST=NH,C=RU" \
  --operation Write --operation Describe \
  --topic client-requests

# Консьюмеры (CLIENT API) могут читать из products-filtered и recommendations
kafka-acls --bootstrap-server $BS --add \
  --allow-principal "User:CN=kafka-consumer,OU=Kafka,O=Marketplace,L=Moscow,ST=NH,C=RU" \
  --operation Read --operation Describe \
  --topic products-filtered
kafka-acls --bootstrap-server $BS --add \
  --allow-principal "User:CN=kafka-consumer,OU=Kafka,O=Marketplace,L=Moscow,ST=NH,C=RU" \
  --operation Read --operation Describe \
  --topic recommendations

echo ""
echo "==> Итоговый список топиков ЦОД-1:"
kafka-topics --bootstrap-server $BS --list
echo ""
echo "✅ ЦОД-1 инициализирован!"
