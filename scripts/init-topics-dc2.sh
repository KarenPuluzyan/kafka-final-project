#!/usr/bin/env bash
# ============================================================
# ЦОД-2 (Санкт-Петербург): создание топиков
# Брокеры: kafka-4:39092, kafka-5:39094, kafka-6:39096
# ZooKeeper: zk2-1:2181, zk2-2:2181, zk2-3:2181
#
# Топики создаются заранее, чтобы MirrorMaker2 мог в них писать.
# RF=3 (все три брокера ЦОД-2), min.isr=2
# ============================================================
set -euo pipefail

BS="kafka-4:39092,kafka-5:39094,kafka-6:39096"
RF=3
MIN_ISR=2

echo "══════════════════════════════════════════"
echo "  ЦОД-2 Санкт-Петербург — инициализация"
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
echo "==> Создаём топики в ЦОД-2..."
echo "    (MirrorMaker2 будет писать в эти топики реплицированные данные)"
echo ""

# MirrorMaker2 добавляет префикс "dc1." к именам топиков-источников
create_topic "dc1.products-filtered"   3 3 2
create_topic "dc1.client-requests"     3 3 2
create_topic "dc1.analytics-events"    3 3 2

# Топик для результатов аналитики (генерируется локально в ЦОД-2)
create_topic "analytics-results"       3 3 2

echo ""
echo "==> Итоговый список топиков ЦОД-2:"
kafka-topics --bootstrap-server $BS --list
echo ""
echo "✅ ЦОД-2 инициализирован!"
