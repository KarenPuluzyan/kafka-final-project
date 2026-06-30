# Отчёт по финальному проекту  
## Аналитическая платформа для маркетплейса «Покупай выгодно»

**Курс:** Apache Kafka для разработки и архитектуры — Яндекс Практикум  
**Автор:** Karen Puluzyan [@KarenPuluzyan](https://github.com/KarenPuluzyan)  
**Репозитории предыдущих заданий:**  
- Блок 1: https://github.com/KarenPuluzyan/Kafka_Yandex  
- Блок 2: https://github.com/KarenPuluzyan/Kafka_Yandex_Block2  
- Блок 4: https://github.com/KarenPuluzyan/Kafka_Yandex_Block4  
- Блок 5: https://github.com/KarenPuluzyan/Kafka_Yandex_Block5  
- Блок 6: https://github.com/KarenPuluzyan/Kafka_Yandex_Block6  

---

## Используемые технологии

| Технология | Версия | Роль в проекте |
|---|---|---|
| Apache Kafka | 7.5.0 (Confluent) | Основная шина данных; два независимых кластера |
| Apache ZooKeeper | 7.5.0 (Confluent) | Координация кластеров; два независимых ансамбля по 3 ноды |
| MirrorMaker 2 | 7.5.0 (Confluent) | Репликация данных между ЦОД-1 и ЦОД-2 |
| Kafka Connect | 7.5.0 (Confluent) | File Sink Connector — запись отфильтрованных данных |
| Faust-streaming | 0.11.3 | Потоковая обработка: фильтрация запрещённых товаров |
| Prometheus | 2.48.0 | Сбор и хранение метрик с обоих кластеров |
| Grafana | 10.2.0 | Визуализация метрик; автопровизионированный дашборд |
| Alertmanager | 0.26.0 | Алерты при падении брокера или ЦОД |
| JMX Exporter | 0.20.0 | Экспорт JMX-метрик Kafka в формат Prometheus |
| Python | 3.11 | SHOP API, CLIENT API, Analytics Service |
| confluent-kafka | 2.3.0 | Python-клиент Kafka |
| Docker / Compose | 24+ / 2.20+ | Контейнеризация всех сервисов |
| Terraform | 1.3+ | IaC: создание VM в Yandex Cloud |
| Yandex Cloud | — | Облачная VM: 16 vCPU, 64 ГБ RAM, 100 ГБ SSD |
| TLS/SSL (mutual) | TLS 1.3 | Шифрование соединений с брокерами ЦОД-1 |
| Kafka ACL | — | Разграничение доступа к топикам |

---

## Архитектура системы

```
          VM Yandex Cloud (16 vCPU / 64 ГБ RAM / 100 ГБ SSD)
          Ubuntu 22.04 — Docker Compose — 24 контейнера
╔═══════════════════════════════════════════════════════════════╗
║  dc1-net (172.20.0.0/24) — ЦОД-1 МОСКВА (основной)          ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │  ZooKeeper: zk1-1 / zk1-2 / zk1-3 (кворум 2/3)       │  ║
║  │  Kafka:  kafka-1:29092  kafka-2:29094  kafka-3:29096   │  ║
║  │          TLS порты: 9093 / 9095 / 9097                 │  ║
║  │          RF=3, min.isr=2, retention=1ч                 │  ║
║  │                                                         │  ║
║  │  Топики:  products │ products-filtered                  │  ║
║  │           client-requests │ recommendations             │  ║
║  │           banned-products-commands │ analytics-events   │  ║
║  │                                                         │  ║
║  │  SHOP API ──────► products (SSL, ACL: Write)            │  ║
║  │  Faust Filter ──► products-filtered (бан-лист)          │  ║
║  │  Kafka Connect ─► data/filtered-products.json (Sink)   │  ║
║  │  Kafka UI DC1  → :8080                                  │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                         ↕ backbone-net (172.22.0.0/24)       ║
║                    MirrorMaker2                              ║
║               dc1.products-filtered →                        ║
║               dc1.client-requests →                          ║
║               ← recommendations                              ║
║                         ↕                                    ║
║  dc2-net (172.21.0.0/24) — ЦОД-2 САНКТ-ПЕТЕРБУРГ            ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │  ZooKeeper: zk2-1 / zk2-2 / zk2-3 (кворум 2/3)       │  ║
║  │  Kafka:  kafka-4:39092  kafka-5:39094  kafka-6:39096   │  ║
║  │          RF=3, min.isr=2, retention=2ч                 │  ║
║  │                                                         │  ║
║  │  Топики:  dc1.products-filtered │ dc1.client-requests  │  ║
║  │           analytics-results                             │  ║
║  │                                                         │  ║
║  │  Analytics ──► читает dc1.* ──► пишет recommendations  │  ║
║  │  Kafka UI DC2 → :8081                                   │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                              ║
║  backbone-net — Мониторинг:                                  ║
║    Prometheus :9090 · Grafana :3000 · Alertmanager :9093    ║
╚═══════════════════════════════════════════════════════════════╝
```

### Сети Docker и изоляция ЦОД

| Сеть | Подсеть | Участники |
|---|---|---|
| `dc1-net` | 172.20.0.0/24 | zk1-1/2/3, kafka-1/2/3, kafka-connect, faust-filter, shop-api, kafka-ui-dc1 |
| `dc2-net` | 172.21.0.0/24 | zk2-1/2/3, kafka-4/5/6, analytics, kafka-ui-dc2 |
| `backbone-net` | 172.22.0.0/24 | kafka-1..6 (EXTERNAL), mirrormaker2, prometheus, grafana, alertmanager |

Брокеры подключены к двум сетям: своей цодовой (изолированной) и `backbone-net`. Это обеспечивает сетевую изоляцию ЦОД при сохранении возможности межцодовой репликации через MirrorMaker2 и централизованного мониторинга через Prometheus.

---

## Логика реализации

### Поток данных

```
SHOP API                     CLIENT API
(читает products.json)       (терминал: search / recommend)
     │                              │
     ▼                              ▼
 [products]             [client-requests]  ◄── ЦОД-1
     │                              │
     ▼                              │
 Faust Filter ◄──── banned-products-commands (CLI)
 (проверяет бан-лист)
     │ разрешённые
     ▼
 [products-filtered]
     │                    │
     ▼                    ▼
 Kafka Connect       MirrorMaker2 ──► ЦОД-2
 File Sink            │
     │                ▼
     ▼          [dc1.products-filtered]
 filtered-           [dc1.client-requests]
 products.json            │
                          ▼
                     Analytics Service
                     (анализ + рекомендации)
                          │
                          ▼
                   [recommendations] ◄── пишет в ЦОД-1
                          │
                          ▼
                     CLIENT API читает
```

---

## Критерии выполнения

> Данные в разделах ниже автоматически обновляются скриптом  
> `scripts/collect-report.sh` при запуске на работающей системе.  
> Результаты каждой проверки сохраняются в отдельные файлы `report/NN_*.txt`.

---

### ✅ Критерий 1: Kafka успешно передаёт данные между сервисами

**Как проверяется:**  
SHOP API непрерывно публикует товары в топик `products` кластера ЦОД-1. Faust-filter читает из `products` и пишет разрешённые товары в `products-filtered`. Analytics читает `dc1.products-filtered` в ЦОД-2 и публикует рекомендации обратно в ЦОД-1. Конечный признак — наличие сообщений в топиках `products-filtered` и `recommendations`.

**Конфигурация топиков ЦОД-1** (см. `scripts/init-topics-dc1.sh`):
```
products                : partitions=3, RF=3, min.isr=2
products-filtered       : partitions=3, RF=3, min.isr=2
client-requests         : partitions=3, RF=3, min.isr=2
recommendations         : partitions=3, RF=3, min.isr=2
banned-products-commands: partitions=1, RF=3, min.isr=2
analytics-events        : partitions=3, RF=3, min.isr=2
```

**Доказательства в файлах:**
- `report/02_dc1_topics.txt` — список и `--describe` всех топиков ЦОД-1
- `report/10_recommendations.txt` — сообщения из топика `recommendations`
- `report/08_filtered_products.txt` — данные в File Sink (косвенно: данные дошли до хранилища)

---

### ✅ Критерий 2: Включена защита TLS, работают ACL

**TLS (mutual)** реализован на брокерах ЦОД-1 (`kafka-1`, `kafka-2`, `kafka-3`):
- Listener `SSL` на портах 9093 / 9095 / 9097
- CA + Keystore + Truststore для каждого брокера (генерация: `scripts/generate-ssl.sh`)
- Keystore + PEM-ключи для клиентов `kafka-producer` и `kafka-consumer`
- Параметр `KAFKA_SSL_CLIENT_AUTH: required` — сервер требует сертификат от клиента

**ACL** настроены через `scripts/init-topics-dc1.sh`:

| Principal | Топик | Операция |
|---|---|---|
| `CN=kafka-producer` | `products` | Write, Describe |
| `CN=kafka-producer` | `client-requests` | Write, Describe |
| `CN=kafka-consumer` | `products-filtered` | Read, Describe |
| `CN=kafka-consumer` | `recommendations` | Read, Describe |

Параметры брокеров:
```yaml
KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true"   # межброкерный PLAINTEXT
KAFKA_SUPER_USERS: User:ANONYMOUS               # брокеры общаются без auth
```

**Доказательства в файлах:**
- `report/05_tls_check.txt` — результат `openssl s_client`: Protocol, Cipher, Verify OK
- `report/04_acl_rules.txt` — вывод `kafka-acls --list`

---

### ✅ Критерий 3: Реализована репликация топиков, задано минимальное число реплик

Все топики ЦОД-1 и ЦОД-2 создаются с параметрами:
- `--replication-factor 3` — каждый топик реплицируется на все три брокера ЦОД
- `--config min.insync.replicas=2` — запись подтверждается минимум двумя репликами

На уровне брокеров:
```yaml
KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_MIN_INSYNC_REPLICAS: 2
```

**Доказательства в файлах:**
- `report/02_dc1_topics.txt` — `--describe` каждого топика: поля `ReplicationFactor`, `Isr`, `Configs: min.insync.replicas=2`
- `report/03_dc2_topics.txt` — аналогично для ЦОД-2

Пример вывода `kafka-topics --describe`:
```
Topic: products  PartitionCount: 3  ReplicationFactor: 3
  Configs: min.insync.replicas=2
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

---

### ✅ Критерий 4: Настроено дублирование данных во второй Kafka-кластер

MirrorMaker 2 (`mirrormaker2` контейнер в `backbone-net`) реплицирует топики из ЦОД-1 в ЦОД-2.

Конфигурация (`config/mm2.properties`):
```properties
clusters = dc1, dc2
dc1.bootstrap.servers = kafka-1:29092,kafka-2:29094,kafka-3:29096
dc2.bootstrap.servers = kafka-4:39092,kafka-5:39094,kafka-6:39096

dc1->dc2.enabled = true
dc1->dc2.topics = products-filtered, client-requests, analytics-events
dc2->dc1.enabled = false
```

Реплицированные топики в ЦОД-2 получают префикс `dc1.`:
- `dc1.products-filtered` — отфильтрованные товары
- `dc1.client-requests` — запросы клиентов для аналитики
- `dc1.analytics-events` — аналитические события

MirrorMaker2 также создаёт служебные топики в ЦОД-2: `heartbeats`, `mm2-offsets.dc1.internal`, `mm2-status.dc1.internal`.

**Доказательства в файлах:**
- `report/06_mirrormaker2_status.txt` — логи MM2, наличие heartbeat-топиков в ЦОД-2, сообщения в `dc1.products-filtered`
- `report/03_dc2_topics.txt` — список топиков ЦОД-2 с префиксом `dc1.*`

---

### ✅ Критерий 5: Выполнена фильтрация запрещённых товаров

Реализована с помощью **Faust-streaming** (`services/faust-filter/app.py`), опыт перенесён из `Kafka_Yandex_Block2`.

**Логика работы:**
1. Faust-агент `filter_products` читает из топика `products`
2. Для каждого товара проверяет `product_id` в Faust Table `banned_products`
3. Если товар в бан-листе → логирует и **отбрасывает** (не передаёт дальше)
4. Если товар разрешён → публикует в `products-filtered`

**Управление бан-листом** через два интерфейса:

CLI (`services/faust-filter/manage_banned.py`):
```bash
python3 manage_banned.py seed   # загрузить начальный список
python3 manage_banned.py add <id> --reason "Причина"
python3 manage_banned.py remove <id>
python3 manage_banned.py list
```

HTTP API (Faust встроенный веб-сервер, порт 6066):
```bash
curl http://<IP>:6066/banned/add/22222?reason=Нелегальный
curl http://<IP>:6066/banned/remove/22222
curl http://<IP>:6066/banned/list
```

**Начальный список запрещённых товаров** (загружается при старте):

| product_id | Причина |
|---|---|
| `22222` | Нелегальный товар — категория «Запрещённые» |
| `55555` | Запрещённый препарат — не подлежит продаже |
| `BAN-001` | SKU в списке запрещённых |

**Доказательства в файлах:**
- `report/09_faust_banned_list.txt` — вывод `/banned/list` и логи Faust с записями `[BANNED]` и `[ALLOWED]`

Пример лога Faust:
```
[BANNED] Товар отфильтрован: 22222 | Нелегальный товар А | Причина: Нелегальный товар
[ALLOWED] Товар разрешён: 12345 | Умные часы XYZ
[ALLOWED] Товар разрешён: 33333 | Ноутбук ProBook 15
```

---

### ✅ Критерий 6: Данные после фильтрации записываются в систему хранения

**Kafka Connect File Sink Connector** читает из топика `products-filtered` и записывает каждое сообщение в файл `data/filtered-products.json`.

Конфигурация коннектора (регистрируется через `scripts/register-connector.sh`):
```json
{
  "name": "products-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "products-filtered",
    "file": "/data/filtered-products.json",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

В файл попадают **только разрешённые** товары — те, что прошли фильтрацию Faust.  
Запрещённые товары (22222, 55555, BAN-001) в файл **не записываются**.

**Проверка через REST API:**
```bash
curl http://localhost:8083/connectors/products-file-sink/status
# → {"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}
```

**Доказательства в файлах:**
- `report/07_connect_status.txt` — статус коннектора `RUNNING`
- `report/08_filtered_products.txt` — содержимое `data/filtered-products.json` с разрешёнными товарами

---

### ✅ Критерий 7: Реализована аналитическая обработка данных

**Analytics Service** (`services/analytics/analytics.py`) работает в ЦОД-2 и подключён к `backbone-net` для записи результатов в ЦОД-1.

**Читает из ЦОД-2:**
- `dc1.products-filtered` — реплицированные отфильтрованные товары
- `dc1.client-requests` — реплицированные запросы клиентов

**Выполняет аналитику:**
- Топ-5 категорий по количеству товаров
- Топ-5 брендов
- Топ-3 магазина
- Суммарный объём запасов в рублях

**Генерирует рекомендации** на основе:
- Популярных категорий (самые частые в батче)
- Истории поисковых запросов пользователей (персонализация)

**Цикл:** каждые 30 секунд анализирует накопленный батч и публикует результаты.

**Доказательства в файлах:**
- `report/10_recommendations.txt` — сообщения из топика `recommendations` с полями `user_id`, `recommendations[]`, `source_dc: dc2-spb`

---

### ✅ Критерий 8: Рекомендации записываются в отдельный топик Kafka

Топик **`recommendations`** в ЦОД-1 предназначен исключительно для рекомендаций.

Параметры топика:
```
recommendations: partitions=3, RF=3, min.isr=2
```

Формат сообщения:
```json
{
  "user_id": "all_users",
  "recommendations": [
    {
      "product_id": "33333",
      "name": "Ноутбук ProBook 15",
      "category": "Электроника",
      "price": {"amount": 89990.00, "currency": "RUB"},
      "reason": "Популярная категория: Электроника"
    }
  ],
  "source_dc": "dc2-spb",
  "generated_at": "2024-10-01T12:00:00Z"
}
```

CLIENT API (`services/shop-api/client_api.py`) подписывается на этот топик командой `recommend` и выводит рекомендации пользователю в терминале.

**Доказательства в файлах:**
- `report/10_recommendations.txt` — 1–5 сообщений из топика `recommendations` в формате JSON

---

### ✅ Критерий 9: Настроен мониторинг

#### 9.1 Дашборд Grafana отображает ключевые метрики

Grafana поднимается на порту `3000` (admin/admin) с автоматически провизионированным дашбордом **«Kafka Marketplace Monitoring»** (`config/grafana/dashboards/kafka-dashboard.json`).

Метрики на дашборде:
- **Брокеры онлайн** — stat panel, зелёный/красный по порогу
- **RPS (запросы/сек)** — timeseries по каждому брокеру
- **Трафик (байт/сек IN/OUT)** — timeseries с разбивкой DC1/DC2
- **JVM Heap Memory** — использование памяти каждого брокера
- **Время обработки запросов (мс)**

Datasource Prometheus провизионируется автоматически из `config/grafana/provisioning/datasources/prometheus.yml`.

#### 9.2 Alertmanager отправляет оповещения при сбоях

Alertmanager работает на порту `9093`. Правила алертов (`config/alert-rules.yml`):

| Алерт | Условие | Severity |
|---|---|---|
| `KafkaBrokerDown` | `up{job=~"dc.-kafka-."} == 0` за 1 мин | critical |
| `KafkaDC1AllBrokersDown` | все брокеры ЦОД-1 недоступны | critical |
| `KafkaDC2AllBrokersDown` | все брокеры ЦОД-2 недоступны | critical |
| `KafkaConsumerGroupLag` | lag > 10 000 за 5 мин | warning |
| `KafkaDiskUsageHigh` | свободно < 20% диска | warning |

#### 9.3 Метрики собираются через Prometheus и JMX Exporter

**JMX Exporter** (версия 0.20.0) подключён к каждому из 6 брокеров через параметр:
```
KAFKA_OPTS: -javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=7071:/etc/kafka/jmx-exporter.yml
```

**Prometheus** (`config/prometheus.yml`) scrape-jobs:
```yaml
- job_name: 'dc1-kafka-1'    targets: ['kafka-1:7071']  labels: {dc: dc1, city: Moscow}
- job_name: 'dc1-kafka-2'    targets: ['kafka-2:7071']  labels: {dc: dc1, city: Moscow}
- job_name: 'dc1-kafka-3'    targets: ['kafka-3:7071']  labels: {dc: dc1, city: Moscow}
- job_name: 'dc2-kafka-4'    targets: ['kafka-4:7071']  labels: {dc: dc2, city: SPb}
- job_name: 'dc2-kafka-5'    targets: ['kafka-5:7071']  labels: {dc: dc2, city: SPb}
- job_name: 'dc2-kafka-6'    targets: ['kafka-6:7071']  labels: {dc: dc2, city: SPb}
- job_name: 'kafka-connect'  targets: ['kafka-connect:8083']
```

Интервал сбора: 15 секунд. Retention TSDB: 2 дня.

**Доказательства в файлах:**
- `report/11_prometheus_metrics.txt` — статус всех 6 брокеров (UP/DOWN), msg/sec, активные алерты

---

### ✅ Критерий 10: Документация оформлена

#### README.md с инструкцией по запуску

Файл `README.md` в корне репозитория содержит:
- Конфигурацию VM (16 vCPU / 64 ГБ / прерываемая) с расчётом стоимости
- Пошаговый план работы на 1 день с временными метками
- Инструкцию по запуску через Terraform (3 команды)
- Описание процесса автоматической инициализации через cloud-init
- Таблицу URL всех веб-интерфейсов
- Команды для управления запрещёнными товарами
- Инструкцию по сбору результатов и скачиванию отчёта

#### Перечень технологий

Представлен в таблице в начале данного отчёта (14 технологий).

#### Архитектура и логика реализации

Описаны в разделах «Архитектура системы» и «Логика реализации» данного отчёта — включая схему потока данных, таблицу сетевой изоляции ЦОД и диаграмму движения сообщений через систему.

---

## Файлы доказательств (папка `report/`)

| Файл | Содержимое | Критерий |
|---|---|---|
| `01_containers_status.txt` | Статус всех 24 контейнеров | 1, все |
| `02_dc1_topics.txt` | Топики + describe ЦОД-1 (RF, ISR, partitions) | 1, 3 |
| `03_dc2_topics.txt` | Топики + describe ЦОД-2 (dc1.* топики) | 3, 4 |
| `04_acl_rules.txt` | Список ACL-правил | 2 |
| `05_tls_check.txt` | Вывод openssl s_client (Protocol, Verify OK) | 2 |
| `06_mirrormaker2_status.txt` | Логи MM2 + сообщения в dc1.products-filtered | 4 |
| `07_connect_status.txt` | Статус File Sink Connector (RUNNING) | 6 |
| `08_filtered_products.txt` | Содержимое data/filtered-products.json | 6 |
| `09_faust_banned_list.txt` | Бан-лист + логи [BANNED]/[ALLOWED] | 5 |
| `10_recommendations.txt` | JSON-сообщения из топика recommendations | 7, 8 |
| `11_prometheus_metrics.txt` | Брокеры UP/DOWN, msg/sec, алерты | 9 |
| `12_resource_usage.txt` | RAM, CPU, диск, docker stats | — |
| `00_SUMMARY_<ts>.txt` | Сводный отчёт автогенерации | все |
| `PROJECT_REPORT.md` | Этот документ | 10 |

---

## Связь с предыдущими заданиями курса

| Блок | Репозиторий | Что применено в финальном проекте |
|---|---|---|
| Блок 1 | [Kafka_Yandex](https://github.com/KarenPuluzyan/Kafka_Yandex) | Архитектура 3-брокерного кластера; Producer At Least Once (`acks=all`, `retries=5`); Consumer Groups; docker-compose с ZooKeeper |
| Блок 2 | [Kafka_Yandex_Block2](https://github.com/KarenPuluzyan/Kafka_Yandex_Block2) | Faust-streaming: агенты, Faust Tables, HTTP API, потоковая фильтрация и цензура — перенесено в `faust-filter` |
| Блок 4 | [Kafka_Yandex_Block4](https://github.com/KarenPuluzyan/Kafka_Yandex_Block4) | JMX Exporter + Prometheus + Grafana дашборд; Kafka Connect с Debezium как образец |
| Блок 5 | [Kafka_Yandex_Block5](https://github.com/KarenPuluzyan/Kafka_Yandex_Block5) | SSL mutual TLS: generate-ssl.sh, keystores/truststores, ACL-настройка — перенесено в `scripts/generate-ssl.sh` |
| Блок 6 | [Kafka_Yandex_Block6](https://github.com/KarenPuluzyan/Kafka_Yandex_Block6) | Terraform IaC для Yandex Cloud; Kafka Connect File Sink; MirrorMaker2; NiFi как вдохновение для SHOP API |

