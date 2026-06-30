#!/usr/bin/env bash
# ============================================================
# Скрипт сбора результатов для отчёта по финальному проекту
# Запускать после того как система поработала ~30 минут
#
# Собирает:
#   1. Скриншоты Kafka UI (curl + jq)
#   2. Статус всех контейнеров
#   3. Список топиков и их описание (ЦОД-1 и ЦОД-2)
#   4. ACL-правила
#   5. TLS-проверка
#   6. MirrorMaker2 — статус репликации
#   7. Kafka Connect — статус коннектора
#   8. Содержимое отфильтрованных товаров (File Sink)
#   9. Faust — список запрещённых товаров
#  10. Метрики Prometheus
#  11. Рекомендации (топик recommendations)
#  12. RAM и CPU — потребление
#
# Результаты сохраняются в ~/kafka-project/report/
# ============================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DIR="$PROJECT_DIR/report"
TS=$(date '+%Y%m%d_%H%M%S')
BS1="localhost:19092"    # ЦОД-1 PLAINTEXT внешний
BS2="localhost:29092"    # ЦОД-2 PLAINTEXT внешний

# Цвета
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✅ $1${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $1${NC}"; }
fail() { echo -e "${RED}❌ $1${NC}"; }
section() { echo -e "\n${YELLOW}══════════════════════════════════════${NC}"; echo -e "${YELLOW}  $1${NC}"; echo -e "${YELLOW}══════════════════════════════════════${NC}"; }

mkdir -p "$REPORT_DIR"
cd "$PROJECT_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Сбор результатов для отчёта                        ║"
echo "║   Время: $(date '+%Y-%m-%d %H:%M:%S')                     ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "  Результаты сохраняются в: $REPORT_DIR/"
echo ""

# ──────────────────────────────────────────────────────────
# 1. Статус контейнеров
# ──────────────────────────────────────────────────────────
section "1. Статус контейнеров Docker"

docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" \
  | tee "$REPORT_DIR/01_containers_status.txt"

RUNNING=$(docker compose ps --status running --format "{{.Name}}" | wc -l)
TOTAL=$(docker compose ps --format "{{.Name}}" | grep -v "^$" | wc -l)
ok "Запущено контейнеров: $RUNNING / $TOTAL"
echo "Запущено: $RUNNING / $TOTAL" >> "$REPORT_DIR/01_containers_status.txt"

# ──────────────────────────────────────────────────────────
# 2. Топики ЦОД-1
# ──────────────────────────────────────────────────────────
section "2. Топики Kafka — ЦОД-1 (Москва)"

{
  echo "=== Список топиков ЦОД-1 ==="
  docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics \
    --bootstrap-server kafka-1:29092 \
    --list 2>/dev/null || fail "kafka-1 недоступен"

  echo ""
  echo "=== Описание топиков ЦОД-1 ==="
  for topic in products products-filtered client-requests recommendations banned-products-commands analytics-events; do
    echo "--- $topic ---"
    docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics \
      --bootstrap-server kafka-1:29092 \
      --describe --topic "$topic" 2>/dev/null || echo "  [топик не найден]"
  done
} | tee "$REPORT_DIR/02_dc1_topics.txt"
ok "Топики ЦОД-1 собраны"

# ──────────────────────────────────────────────────────────
# 3. Топики ЦОД-2
# ──────────────────────────────────────────────────────────
section "3. Топики Kafka — ЦОД-2 (Санкт-Петербург)"

{
  echo "=== Список топиков ЦОД-2 (включая реплицированные dc1.*) ==="
  docker exec -e KAFKA_OPTS="" kafka-4 kafka-topics \
    --bootstrap-server kafka-4:39092 \
    --list 2>/dev/null || fail "kafka-4 недоступен"

  echo ""
  echo "=== Описание топиков ЦОД-2 ==="
  for topic in dc1.products-filtered dc1.client-requests analytics-results; do
    echo "--- $topic ---"
    docker exec -e KAFKA_OPTS="" kafka-4 kafka-topics \
      --bootstrap-server kafka-4:39092 \
      --describe --topic "$topic" 2>/dev/null || echo "  [топик не найден]"
  done
} | tee "$REPORT_DIR/03_dc2_topics.txt"
ok "Топики ЦОД-2 собраны"

# ──────────────────────────────────────────────────────────
# 4. ACL-правила
# ──────────────────────────────────────────────────────────
section "4. ACL-правила (ЦОД-1)"

{
  echo "=== ACL для топиков ==="
  docker exec -e KAFKA_OPTS="" kafka-1 kafka-acls \
    --bootstrap-server kafka-1:29092 \
    --list 2>/dev/null || echo "[ACL не настроены или не доступны]"
} | tee "$REPORT_DIR/04_acl_rules.txt"
ok "ACL собраны"

# ──────────────────────────────────────────────────────────
# 5. TLS-проверка
# ──────────────────────────────────────────────────────────
section "5. Проверка TLS (ЦОД-1)"

{
  echo "=== TLS соединение с kafka-1:9093 ==="
  if [ -f ssl/dc1/ca-cert ]; then
    echo "Q" | openssl s_client \
      -connect localhost:19093 \
      -CAfile ssl/dc1/ca-cert \
      -cert ssl/dc1/kafka-producer-signed.crt \
      -key ssl/dc1/kafka-producer.key \
      2>&1 | grep -E "(Verify|Protocol|Cipher|subject|issuer|CONNECTED|error)" \
      || echo "[TLS соединение не установлено]"
  else
    echo "[SSL-сертификаты не найдены: ssl/dc1/ca-cert]"
  fi
} | tee "$REPORT_DIR/05_tls_check.txt"
ok "TLS проверен"

# ──────────────────────────────────────────────────────────
# 6. MirrorMaker2 — статус репликации
# ──────────────────────────────────────────────────────────
section "6. MirrorMaker2 — репликация ЦОД-1 → ЦОД-2"

{
  echo "=== Логи MirrorMaker2 (последние 30 строк) ==="
  docker logs mirrormaker2 --tail 30 2>&1

  echo ""
  echo "=== Heartbeat топики в ЦОД-2 (признак работы MM2) ==="
  docker exec -e KAFKA_OPTS="" kafka-4 kafka-topics \
    --bootstrap-server kafka-4:39092 \
    --list 2>/dev/null | grep -E "(heartbeat|checkpoint|dc1)" || echo "[репликация ещё не началась]"

  echo ""
  echo "=== Сообщения в dc1.products-filtered (ЦОД-2) ==="
  timeout 8 docker exec -e KAFKA_OPTS="" kafka-4 kafka-console-consumer \
    --bootstrap-server kafka-4:39092 \
    --topic dc1.products-filtered \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 5000 2>/dev/null \
    || echo "[нет сообщений или топик не создан]"
} | tee "$REPORT_DIR/06_mirrormaker2_status.txt"
ok "MirrorMaker2 проверен"

# ──────────────────────────────────────────────────────────
# 7. Kafka Connect — File Sink
# ──────────────────────────────────────────────────────────
section "7. Kafka Connect — File Sink Connector"

{
  echo "=== Список коннекторов ==="
  curl -s http://localhost:8083/connectors 2>/dev/null \
    | python3 -m json.tool 2>/dev/null \
    || echo "[Kafka Connect недоступен]"

  echo ""
  echo "=== Статус коннектора products-file-sink ==="
  curl -s http://localhost:8083/connectors/products-file-sink/status 2>/dev/null \
    | python3 -m json.tool 2>/dev/null \
    || echo "[коннектор не зарегистрирован]"
} | tee "$REPORT_DIR/07_connect_status.txt"
ok "Kafka Connect проверен"

# ──────────────────────────────────────────────────────────
# 8. Отфильтрованные товары (File Sink output)
# ──────────────────────────────────────────────────────────
section "8. Отфильтрованные товары (File Sink)"

SINK_FILE="$PROJECT_DIR/data/filtered-products.json"
{
  if [ -f "$SINK_FILE" ]; then
    LINE_COUNT=$(wc -l < "$SINK_FILE")
    echo "Файл: $SINK_FILE"
    echo "Записей: $LINE_COUNT"
    echo ""
    echo "=== Последние 10 записей ==="
    tail -10 "$SINK_FILE"
  else
    echo "[файл $SINK_FILE ещё не создан — коннектор не получил данные]"
  fi
} | tee "$REPORT_DIR/08_filtered_products.txt"
ok "Данные File Sink собраны"

# ──────────────────────────────────────────────────────────
# 9. Faust — запрещённые товары
# ──────────────────────────────────────────────────────────
section "9. Faust — список запрещённых товаров"

{
  echo "=== HTTP API Faust: /banned/list ==="
  curl -s http://localhost:6066/banned/list 2>/dev/null \
    | python3 -m json.tool 2>/dev/null \
    || echo "[Faust не отвечает]"

  echo ""
  echo "=== Логи Faust (последние 20 строк) ==="
  docker logs faust-filter --tail 20 2>&1
} | tee "$REPORT_DIR/09_faust_banned_list.txt"
ok "Faust проверен"

# ──────────────────────────────────────────────────────────
# 10. Рекомендации из топика
# ──────────────────────────────────────────────────────────
section "10. Рекомендации (топик recommendations)"

{
  echo "=== Сообщения из топика recommendations (ЦОД-1) ==="
  timeout 8 docker exec -e KAFKA_OPTS="" kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic recommendations \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 5000 2>/dev/null \
    | python3 -m json.tool 2>/dev/null \
    || echo "[нет рекомендаций или топик пуст]"
} | tee "$REPORT_DIR/10_recommendations.txt"
ok "Рекомендации собраны"

# ──────────────────────────────────────────────────────────
# 11. Метрики Prometheus
# ──────────────────────────────────────────────────────────
section "11. Метрики Prometheus"

{
  echo "=== Брокеры онлайн ==="
  curl -s "http://localhost:9090/api/v1/query?query=up{job=~'dc.-kafka-.'}" 2>/dev/null \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
for r in results:
    m = r.get('metric', {})
    v = r.get('value', [None, '0'])[1]
    status = '✅ UP' if v == '1' else '❌ DOWN'
    print(f\"  {status}  {m.get('job','?')} (broker={m.get('broker','?')})\")
" 2>/dev/null || echo "[Prometheus недоступен]"

  echo ""
  echo "=== Топ метрики Kafka (messages in/sec) ==="
  curl -s "http://localhost:9090/api/v1/query?query=rate(kafka_server_BrokerTopicMetrics_MessagesInPerSec_rate1m[1m])" \
    2>/dev/null \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
for r in results:
    m = r.get('metric', {})
    v = float(r.get('value', [None, 0])[1])
    if v > 0:
        print(f\"  {m.get('broker','?')}: {v:.2f} msg/sec\")
" 2>/dev/null || echo "[метрики недоступны]"

  echo ""
  echo "=== Alertmanager — активные алерты ==="
  curl -s "http://localhost:9093/api/v2/alerts" 2>/dev/null \
    | python3 -c "
import sys, json
alerts = json.load(sys.stdin)
if not alerts:
    print('  ✅ Активных алертов нет')
else:
    for a in alerts:
        lbl = a.get('labels', {})
        print(f\"  ⚠️  {lbl.get('alertname','?')} [{lbl.get('severity','?')}]\")
" 2>/dev/null || echo "[Alertmanager недоступен]"
} | tee "$REPORT_DIR/11_prometheus_metrics.txt"
ok "Метрики Prometheus собраны"

# ──────────────────────────────────────────────────────────
# 12. Потребление ресурсов VM
# ──────────────────────────────────────────────────────────
section "12. Потребление ресурсов VM"

{
  echo "=== Память ==="
  free -h

  echo ""
  echo "=== CPU ==="
  top -bn1 | grep "Cpu(s)" | head -3

  echo ""
  echo "=== Диск ==="
  df -h /

  echo ""
  echo "=== Потребление по контейнерам (top-10 по RAM) ==="
  docker stats --no-stream --format \
    "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" \
    | sort -t$'\t' -k3 -hr | head -12
} | tee "$REPORT_DIR/12_resource_usage.txt"
ok "Ресурсы VM собраны"

# ──────────────────────────────────────────────────────────
# 13. Сводный отчёт
# ──────────────────────────────────────────────────────────
section "13. Формирование сводного отчёта"

SUMMARY="$REPORT_DIR/00_SUMMARY_${TS}.txt"
{
cat << SUMMARY_HEADER
════════════════════════════════════════════════════════════════
  ФИНАЛЬНЫЙ ПРОЕКТ — Аналитическая платформа «Покупай выгодно»
  Яндекс Практикум — Apache Kafka
  Автор: Karen Puluzyan (KarenPuluzyan)
════════════════════════════════════════════════════════════════
  Дата и время сбора: $(date '+%Y-%m-%d %H:%M:%S')
  VM: Yandex Cloud, Intel Ice Lake, 16 vCPU / 64 ГБ RAM / 100 ГБ SSD
════════════════════════════════════════════════════════════════

АРХИТЕКТУРА: 2 изолированных ЦОД (Docker-сети)
  ЦОД-1 Москва:          zk1-1/2/3 + kafka-1/2/3 (TLS + ACL)
  ЦОД-2 Санкт-Петербург: zk2-1/2/3 + kafka-4/5/6
  Межцодовый канал:       MirrorMaker2 (dc1 → dc2)

SUMMARY_HEADER

echo "СТАТУС КОНТЕЙНЕРОВ:"
docker compose ps --format "  {{.Name}}: {{.Status}}" 2>/dev/null | sort

echo ""
echo "ТОПИКИ ЦОД-1:"
docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list 2>/dev/null \
  | sed 's/^/  /' || echo "  [недоступно]"

echo ""
echo "ТОПИКИ ЦОД-2 (включая реплицированные):"
docker exec -e KAFKA_OPTS="" kafka-4 kafka-topics --bootstrap-server kafka-4:39092 --list 2>/dev/null \
  | sed 's/^/  /' || echo "  [недоступно]"

echo ""
echo "ЗАПРЕЩЁННЫЕ ТОВАРЫ (Faust):"
curl -s http://localhost:6066/banned/list 2>/dev/null \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  Всего запрещённых: {d.get('banned_count',0)}\")
for pid, reason in d.get('banned_products',{}).items():
    print(f'  • {pid}: {reason}')
" 2>/dev/null || echo "  [Faust недоступен]"

echo ""
echo "FILE SINK (отфильтрованные товары):"
if [ -f "$PROJECT_DIR/data/filtered-products.json" ]; then
  echo "  Файл: data/filtered-products.json"
  echo "  Записей: $(wc -l < "$PROJECT_DIR/data/filtered-products.json")"
else
  echo "  [файл не создан]"
fi

echo ""
echo "ИСПОЛЬЗОВАНИЕ РЕСУРСОВ:"
free -h | grep Mem | awk '{printf "  RAM: %s использовано из %s (свободно: %s)\n", $3, $2, $4}'
df -h / | tail -1 | awk '{printf "  Диск: %s использовано из %s (%s)\n", $3, $2, $5}'

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  Ссылки на интерфейсы:"
PUBLIC_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<IP>")
echo "  Kafka UI DC1:  http://$PUBLIC_IP:8080"
echo "  Kafka UI DC2:  http://$PUBLIC_IP:8081"
echo "  Grafana:       http://$PUBLIC_IP:3000  (admin/admin)"
echo "  Prometheus:    http://$PUBLIC_IP:9090"
echo "════════════════════════════════════════════════════════════════"

} | tee "$SUMMARY"

# ──────────────────────────────────────────────────────────
# 14. Генерация финального Markdown-отчёта с реальными данными
# ──────────────────────────────────────────────────────────
section "14. Генерация CRITERIA_REPORT.md"

PUBLIC_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "N/A")
CRITERIA_REPORT="$REPORT_DIR/CRITERIA_REPORT_${TS}.md"

{
cat << MD_HEADER
# Отчёт по критериям — финальный проект «Покупай выгодно»

**Автор:** Karen Puluzyan [@KarenPuluzyan](https://github.com/KarenPuluzyan)  
**Дата сбора:** $(date '+%Y-%m-%d %H:%M:%S')  
**VM:** Yandex Cloud, Intel Ice Lake, 16 vCPU / 64 ГБ RAM / 100 ГБ SSD  
**Публичный IP:** $PUBLIC_IP

> Этот файл сгенерирован автоматически скриптом \`scripts/collect-report.sh\`.  
> Полные логи по каждому пункту — в файлах \`report/NN_*.txt\`.

---

## Критерий 1: Kafka передаёт данные между сервисами

**Статус контейнеров:**

\`\`\`
MD_HEADER

docker compose ps --format "{{.Name}}: {{.Status}}" 2>/dev/null | sort
echo '```'

echo ""
echo "**Топики ЦОД-1 (kafka-1/2/3):**"
echo '```'
docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list 2>/dev/null \
  || echo "[недоступно]"
echo '```'

echo ""
echo "**Сообщения в топике \`products-filtered\` (последние 3):**"
echo '```json'
timeout 8 docker exec -e KAFKA_OPTS="" kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic products-filtered \
  --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null \
  | head -3 \
  || echo "[пусто или топик не создан]"
echo '```'

cat << 'MD2'

**Файлы доказательств:** `report/01_containers_status.txt`, `report/02_dc1_topics.txt`

---

## Критерий 2: Защита TLS и ACL

**Результат TLS-рукопожатия с kafka-1:9093:**

```
MD2

if [ -f "$PROJECT_DIR/ssl/dc1/ca-cert" ]; then
  echo "Q" | openssl s_client \
    -connect localhost:19093 \
    -CAfile "$PROJECT_DIR/ssl/dc1/ca-cert" \
    -cert "$PROJECT_DIR/ssl/dc1/kafka-producer-signed.crt" \
    -key "$PROJECT_DIR/ssl/dc1/kafka-producer.key" \
    2>&1 | grep -E "(CONNECTED|Protocol|Cipher|subject|issuer|Verification|Verify)" \
    || echo "[TLS недоступен]"
else
  echo "[SSL-сертификаты не найдены]"
fi
echo '```'

echo ""
echo "**ACL-правила (kafka-acls --list):**"
echo '```'
docker exec -e KAFKA_OPTS="" kafka-1 kafka-acls \
  --bootstrap-server kafka-1:29092 \
  --list 2>/dev/null \
  || echo "[ACL недоступны]"
echo '```'

echo ""
echo "**Файлы доказательств:** \`report/04_acl_rules.txt\`, \`report/05_tls_check.txt\`"
echo ""
echo "---"
echo ""

cat << 'MD3'
## Критерий 3: Репликация топиков и минимальное число реплик

**Describe топика `products` (ЦОД-1):**

```
MD3

docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe --topic products 2>/dev/null \
  || echo "[топик не найден]"
echo '```'

echo ""
echo "**Describe топика \`dc1.products-filtered\` (ЦОД-2):**"
echo '```'
docker exec -e KAFKA_OPTS="" kafka-4 kafka-topics \
  --bootstrap-server kafka-4:39092 \
  --describe --topic dc1.products-filtered 2>/dev/null \
  || echo "[топик не найден]"
echo '```'

echo ""
echo "**Файлы доказательств:** \`report/02_dc1_topics.txt\`, \`report/03_dc2_topics.txt\`"
echo ""
echo "---"
echo ""

cat << 'MD4'
## Критерий 4: Дублирование данных во второй кластер (MirrorMaker2)

**Топики в ЦОД-2 (dc1.* — реплицированные из ЦОД-1):**

```
MD4

docker exec -e KAFKA_OPTS="" kafka-4 kafka-topics \
  --bootstrap-server kafka-4:39092 \
  --list 2>/dev/null | grep -E "(dc1\.|heartbeat|checkpoint)" \
  || echo "[реплицированных топиков нет]"
echo '```'

echo ""
echo "**Последние строки лога MirrorMaker2:**"
echo '```'
docker logs mirrormaker2 --tail 10 2>&1 \
  | grep -v "^$" | head -10
echo '```'

echo ""
echo "**Файлы доказательств:** \`report/06_mirrormaker2_status.txt\`, \`report/03_dc2_topics.txt\`"
echo ""
echo "---"
echo ""

cat << 'MD5'
## Критерий 5: Фильтрация запрещённых товаров (Faust)

**Список запрещённых товаров:**

```json
MD5

curl -s http://localhost:6066/banned/list 2>/dev/null \
  | python3 -m json.tool 2>/dev/null \
  || echo '{"error": "Faust недоступен"}'
echo '```'

echo ""
echo "**Лог Faust (записи BANNED/ALLOWED):**"
echo '```'
docker logs faust-filter --tail 30 2>&1 \
  | grep -E "\[(BANNED|ALLOWED|BAN ADD)\]" | head -10 \
  || echo "[логи не найдены]"
echo '```'

echo ""
echo "**Файлы доказательств:** \`report/09_faust_banned_list.txt\`"
echo ""
echo "---"
echo ""

cat << 'MD6'
## Критерий 6: Данные после фильтрации записываются в хранилище

**Статус Kafka Connect File Sink Connector:**

```json
MD6

curl -s http://localhost:8083/connectors/products-file-sink/status 2>/dev/null \
  | python3 -m json.tool 2>/dev/null \
  || echo '{"error": "Connect недоступен"}'
echo '```'

echo ""
SINK_FILE="$PROJECT_DIR/data/filtered-products.json"
if [ -f "$SINK_FILE" ]; then
  LINE_COUNT=$(wc -l < "$SINK_FILE")
  echo "**Файл** \`data/filtered-products.json\` — **записей: $LINE_COUNT**"
  echo ""
  echo "Последние 3 записи:"
  echo '```json'
  tail -3 "$SINK_FILE" | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if line:
        try:
            obj = json.loads(line)
            print(json.dumps({'product_id': obj.get('product_id'), 'name': obj.get('name'), 'category': obj.get('category')}, ensure_ascii=False, indent=2))
        except:
            print(line[:120])
" 2>/dev/null || tail -3 "$SINK_FILE"
  echo '```'
else
  echo "**Файл** \`data/filtered-products.json\` — не создан (коннектор ещё не получил данные)"
fi

echo ""
echo "**Файлы доказательств:** \`report/07_connect_status.txt\`, \`report/08_filtered_products.txt\`"
echo ""
echo "---"
echo ""

cat << 'MD7'
## Критерий 7 и 8: Аналитика и рекомендации в отдельном топике

**Сообщения из топика `recommendations` (ЦОД-1):**

```json
MD7

timeout 8 docker exec -e KAFKA_OPTS="" kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic recommendations \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000 2>/dev/null \
  | head -3 \
  | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if line:
        try:
            obj = json.loads(line)
            print(json.dumps({
              'user_id': obj.get('user_id'),
              'source_dc': obj.get('source_dc'),
              'generated_at': obj.get('generated_at'),
              'recommendations_count': len(obj.get('recommendations', [])),
              'top_recommendation': obj.get('recommendations', [{}])[0].get('name', '-')
            }, ensure_ascii=False, indent=2))
        except:
            print(line[:200])
" 2>/dev/null \
  || echo "[нет сообщений — аналитика ещё не успела сгенерировать рекомендации]"
echo '```'

echo ""
echo "**Файлы доказательств:** \`report/10_recommendations.txt\`"
echo ""
echo "---"
echo ""

cat << 'MD8'
## Критерий 9: Мониторинг (Prometheus, Grafana, Alertmanager)

**Статус брокеров в Prometheus:**

```
MD8

curl -s "http://localhost:9090/api/v1/query?query=up{job=~'dc.-kafka-.'}" 2>/dev/null \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
if not results:
    print('[нет данных — JMX Exporter ещё не собрал метрики]')
for r in results:
    m = r.get('metric', {})
    v = r.get('value', [None, '0'])[1]
    status = 'UP  ✓' if v == '1' else 'DOWN ✗'
    print(f\"{status}  {m.get('job','?')} | broker={m.get('broker','?')} | dc={m.get('dc','?')}\")
" 2>/dev/null || echo "[Prometheus недоступен на localhost:9090]"
echo '```'

echo ""
echo "**Активные алерты Alertmanager:**"
echo '```'
curl -s "http://localhost:9093/api/v2/alerts" 2>/dev/null \
  | python3 -c "
import sys, json
alerts = json.load(sys.stdin)
if not alerts:
    print('Активных алертов нет — все брокеры работают нормально ✓')
else:
    for a in alerts:
        lbl = a.get('labels', {})
        ann = a.get('annotations', {})
        print(f\"⚠  {lbl.get('alertname','?')} [{lbl.get('severity','?')}]: {ann.get('summary','')}\")
" 2>/dev/null || echo "[Alertmanager недоступен на localhost:9093]"
echo '```'

echo ""
echo "**Конфигурация scrape-jobs (Prometheus собирает метрики с 6 брокеров + Connect):**"
echo '```'
grep "job_name:" "$PROJECT_DIR/config/prometheus.yml" | sed 's/.*job_name:/  job:/'
echo '```'

cat << MD9

**Grafana:** http://${PUBLIC_IP}:3000 (admin/admin)  
Дашборд «Kafka Marketplace Monitoring» провизионируется автоматически из  
\`config/grafana/dashboards/kafka-dashboard.json\`.

**Файлы доказательств:** \`report/11_prometheus_metrics.txt\`

---

## Критерий 10: Документация

| Документ | Путь | Содержимое |
|---|---|---|
| README.md | \`README.md\` | Инструкция по запуску, VM-конфигурация, стоимость, план на 1 день |
| Отчёт по проекту | \`report/PROJECT_REPORT.md\` | Архитектура, технологии, логика, критерии |
| Этот документ | \`report/CRITERIA_REPORT_<ts>.md\` | Данные с работающей системы по каждому критерию |

**Технологии проекта:** Apache Kafka, ZooKeeper, MirrorMaker2, Kafka Connect,  
Faust-streaming, Prometheus, Grafana, Alertmanager, JMX Exporter,  
Python 3.11, confluent-kafka, Docker Compose, Terraform, Yandex Cloud, TLS/ACL.

MD9

} | tee "$CRITERIA_REPORT"
ok "CRITERIA_REPORT_${TS}.md сгенерирован"

# ──────────────────────────────────────────────────────────
# Итог
# ──────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   ✅ Сбор результатов завершён!                       ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Файлы в report/:                                    ║"
ls -1 "$REPORT_DIR/" | sed 's/^/║    /'
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Скачать с VM:                                       ║"
echo "║  scp -r ubuntu@${PUBLIC_IP}:~/kafka-project/report/ ./"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
