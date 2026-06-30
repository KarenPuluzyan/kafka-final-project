# Финальный проект: Аналитическая платформа «Покупай выгодно»

Яндекс Практикум — Apache Kafka для разработки и архитектуры  
Автор: **Karen Puluzyan** [@KarenPuluzyan](https://github.com/KarenPuluzyan)

---

## Инфраструктура: прерываемая VM в Yandex Cloud на 1 день

| Параметр | Значение |
|---|---|
| Платформа | Intel Ice Lake (`standard-v3`) |
| vCPU | 16 × 100% |
| RAM | 64 ГБ |
| Диск | 100 ГБ network-SSD |
| ОС | Ubuntu 22.04 LTS |
| Тип VM | **Прерываемая (preemptible)** |
| Зона | `ru-central1-a` |

### Стоимость за 1 день

| Ресурс | Цена | За 24 ч |
|---|---|---|
| 16 vCPU (прерываемая) × ~0.35 ₽/ч | 5.6 ₽/ч | ~134 ₽ |
| 64 ГБ RAM (прерываемая) × ~0.092 ₽/ч | 5.9 ₽/ч | ~142 ₽ |
| 100 ГБ SSD × 0.012 ₽/ч | 1.2 ₽/ч | ~29 ₽ |
| Статический IP | 0.21 ₽/ч | ~5 ₽ |
| **Итого без НДС** | ~13 ₽/ч | **~310 ₽/день** |
| **Итого с НДС 20%** | | **~372 ₽/день** |

> Прерываемая VM в ~3 раза дешевле обычной. YC гарантирует работу
> минимум 1 час, после 24 часов VM будет остановлена автоматически.
> Данные на диске при этом **сохраняются**.

---

## Пошаговый план на 1 день

```
09:00  terraform apply           → VM создана (~2 мин)
09:07  cloud-init завершён       → Docker + проект готовы (~5-7 мин)
09:10  Проверяем :8080 и :3000   → Kafka UI и Grafana открываются
09:30  bash scripts/collect-report.sh → Первый сбор результатов
       (система работает, данные накапливаются)
14:00  bash scripts/collect-report.sh → Промежуточный сбор
17:00  bash scripts/collect-report.sh → Финальный сбор для отчёта
17:10  scp -r ubuntu@<IP>:~/kafka-project/report/ ./  → Скачиваем
17:15  terraform destroy          → Удаляем VM, списание прекращается
```

---

## Запуск

### 1. Подготовка (на локальной машине)

```bash
# Создаём SSH-ключ (если ещё нет)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""

# Устанавливаем yc CLI
curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
source ~/.bashrc
yc init   # авторизация через браузер

# Переходим в папку terraform
cd ~/kafkafinalproject/terraform

# Заполняем переменные
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars
```

```hcl
# terraform.tfvars — вставить реальные значения:
yc_token            = "..."   # yc iam create-token
yc_cloud_id         = "..."   # yc config get cloud-id
yc_folder_id        = "..."   # yc config get folder-id
yc_zone             = "ru-central1-a"
ssh_public_key_path = "~/.ssh/id_rsa.pub"
repo_url            = ""
```

### 2. Создаём VM через Terraform

```bash
cd ~/kafkafinalproject/terraform
terraform init
terraform apply   # ~2 минуты, введи "yes"
```

Terraform выведет публичный IP и URL всех сервисов.

### 3. Копируем проект на облачную VM

```bash
# Копируем проект (вместо git clone — работаем без GitHub)
scp -r ~/kafkafinalproject ubuntu@<IP>:/home/ubuntu/kafka-project

# Если папка попала вложенной — исправляем:
ssh ubuntu@<IP> "mv ~/kafka-project/kafkafinalproject/* ~/kafka-project/ && \
  rmdir ~/kafka-project/kafkafinalproject"
```

### 4. Инициализируем проект на облачной VM

```bash
ssh ubuntu@<IP>
cd ~/kafka-project

# Скачиваем JMX агент
bash scripts/download-jmx-agent.sh

# Генерируем SSL-сертификаты для ЦОД-1
bash scripts/generate-ssl.sh

# Запускаем все контейнеры
docker compose up -d
```

### 5. Устанавливаем File Sink Connector

> ⚠️ Образ `cp-kafka-connect` не содержит `FileStreamSinkConnector`.
> Устанавливаем JAR вручную после первого запуска.

```bash
# Скачиваем JAR (выполнять на облачной VM)
wget -q https://repo1.maven.org/maven2/org/apache/kafka/connect-file/3.5.1/connect-file-3.5.1.jar \
  -O /tmp/connect-file-3.5.1.jar

# Копируем в контейнер
docker cp /tmp/connect-file-3.5.1.jar \
  kafka-connect:/usr/share/java/kafka/connect-file-3.5.1.jar

# Перезапускаем Kafka Connect для загрузки плагина
docker restart kafka-connect
sleep 30

# Проверяем что плагин появился
curl -s http://localhost:8083/connector-plugins | python3 -m json.tool | grep -i file
# Должно показать: FileStreamSinkConnector и FileStreamSourceConnector
```

### 6. Инициализируем Kafka топики и сервисы

```bash
# Ждём готовности всех брокеров (~2-3 мин после docker compose up)
docker compose ps

# Исправляем min.isr для Faust changelog топика (необходимо для работы фильтрации)
docker exec -e KAFKA_OPTS="" kafka-1 kafka-configs \
  --bootstrap-server kafka-1:29092 --alter \
  --entity-type topics \
  --entity-name marketplace-filter-banned_products-changelog \
  --add-config min.insync.replicas=1

docker exec -e KAFKA_OPTS="" kafka-1 kafka-configs \
  --bootstrap-server kafka-1:29092 --alter \
  --entity-type topics \
  --entity-name marketplace-filter-__assignor-__leader \
  --add-config min.insync.replicas=1

# Перезапускаем Faust после исправления
docker restart faust-filter

# Регистрируем File Sink Connector
bash scripts/register-connector.sh

# Загружаем список запрещённых товаров
pip3 install confluent-kafka --quiet
KAFKA_BROKERS=localhost:19092 python3 services/faust-filter/manage_banned.py seed
```

### 7. Проверяем готовность

```bash
ssh ubuntu@<IP>
docker compose ps   # все контейнеры должны быть Up
```

Ожидаемый вывод через 7–10 минут после создания VM:
```
NAME              STATUS        PORTS
analytics         Up
alertmanager      Up            0.0.0.0:9093->9093/tcp
faust-filter      Up            0.0.0.0:6066->6066/tcp
grafana           Up            0.0.0.0:3000->3000/tcp
kafka-1           Up (healthy)  0.0.0.0:19092-19093->...
kafka-2           Up (healthy)
kafka-3           Up (healthy)
kafka-4           Up (healthy)
kafka-5           Up (healthy)
kafka-6           Up (healthy)
kafka-connect     Up (healthy)  0.0.0.0:8083->8083/tcp
kafka-ui-dc1      Up            0.0.0.0:8080->8080/tcp
kafka-ui-dc2      Up            0.0.0.0:8081->8080/tcp
mirrormaker2      Up
prometheus        Up            0.0.0.0:9090->9090/tcp
shop-api          Up
zk1-1/2/3        Up (healthy)
zk2-1/2/3        Up (healthy)
```

### 8. Веб-интерфейсы

| Сервис | URL | Доступ |
|--------|-----|--------|
| Kafka UI — ЦОД-1 | `http://<IP>:8080` | без пароля |
| Kafka UI — ЦОД-2 | `http://<IP>:8081` | без пароля |
| Grafana | `http://<IP>:3000` | admin / admin |
| Prometheus | `http://<IP>:9090` | без пароля |
| Alertmanager | `http://<IP>:9093` | без пароля |
| Faust API | `http://<IP>:6066/banned/list` | без пароля |

### 9. Сбор результатов для отчёта

```bash
# Подождать 30 минут после запуска, затем:
bash ~/kafka-project/scripts/collect-report.sh
```

Скрипт собирает в `~/kafka-project/report/`:
```
00_SUMMARY_<timestamp>.txt    ← сводный отчёт
01_containers_status.txt      ← статус 24 контейнеров
02_dc1_topics.txt             ← топики + describe ЦОД-1
03_dc2_topics.txt             ← топики + describe ЦОД-2
04_acl_rules.txt              ← ACL-правила
05_tls_check.txt              ← результат TLS-рукопожатия
06_mirrormaker2_status.txt    ← статус репликации DC1→DC2
07_connect_status.txt         ← File Sink коннектор
08_filtered_products.txt      ← содержимое File Sink
09_faust_banned_list.txt      ← список запрещённых товаров
10_recommendations.txt        ← рекомендации из топика
11_prometheus_metrics.txt     ← метрики: брокеры online, msg/sec
12_resource_usage.txt         ← RAM, CPU, диск, docker stats
```

### 10. Скачиваем результаты

```bash
# С локальной машины:
scp -r ubuntu@<IP>:~/kafka-project/report/ ./report/
```

### 11. Управление запрещёнными товарами

```bash
# На VM:
cd ~/kafka-project/services/faust-filter
KAFKA_BROKERS=localhost:19092 python3 manage_banned.py list
KAFKA_BROKERS=localhost:19092 python3 manage_banned.py add 12345 --reason "Подделка"
KAFKA_BROKERS=localhost:19092 python3 manage_banned.py remove 12345

# Через HTTP (с любой машины):
curl http://<IP>:6066/banned/list
curl "http://<IP>:6066/banned/add/12345?reason=Подделка"
```

### 12. Завершение работы

```bash
# После скачивания результатов — удаляем VM:
cd ~/kafkafinalproject/terraform
terraform destroy   # прекращает списание денег
```

---

## Известные особенности при развёртывании

**ZooKeeper healthcheck** — образ Confluent по умолчанию разрешает только команду `srvr`, не `ruok`. В `docker-compose.yml` уже исправлено на `echo srvr | nc localhost 2181 | grep Mode`.

**Kafka healthcheck** — при вызове `kafka-topics` внутри контейнера наследуется `KAFKA_OPTS` с JMX агентом, что вызывает конфликт порта. Исправлено: healthcheck использует `KAFKA_OPTS= kafka-topics ...`. Аналогично при ручных вызовах `docker exec` всегда добавляй `-e KAFKA_OPTS=""`.

**FileStreamSinkConnector** — отсутствует в образе `cp-kafka-connect:7.5.0`. Необходимо вручную скачать JAR и скопировать в контейнер (Шаг 5 выше).

**Faust changelog топик** — создаётся с RF=3 и min.isr=2 по умолчанию кластера. При первом запуске нужно снизить min.isr=1 для топиков `marketplace-filter-banned_products-changelog` и `marketplace-filter-__assignor-__leader` (Шаг 6 выше).

---

## Структура проекта

```
kafkafinalproject/
├── terraform/
│   ├── main.tf                    # 16 vCPU / 64 ГБ / прерываемая VM
│   ├── variables.tf
│   ├── outputs.tf                 # IP, URL, стоимость
│   ├── cloud-init.yaml            # Установка Docker на VM
│   └── terraform.tfvars.example
├── docker-compose.yml             # 24 контейнера, heap для 64 ГБ
│                                  # healthcheck: srvr + KAFKA_OPTS=""
│                                  # ssl→secrets, retention 1ч/2ч
├── jmx_prometheus_javaagent-0.20.0.jar  # скачивается скриптом
├── scripts/
│   ├── collect-report.sh          # ← ГЛАВНЫЙ СКРИПТ для отчёта
│   ├── generate-ssl.sh            # TLS сертификаты для ЦОД-1
│   ├── download-jmx-agent.sh      # JMX Prometheus Agent
│   ├── init-topics-dc1.sh         # Топики + ACL в ЦОД-1
│   ├── init-topics-dc2.sh         # Топики в ЦОД-2
│   └── register-connector.sh      # File Sink Connector
├── config/                        # prometheus, grafana, mm2, alerts
├── services/
│   ├── shop-api/                  # SHOP API + CLIENT API
│   ├── faust-filter/              # Фильтрация бан-листа
│   └── analytics/                 # Аналитика + рекомендации
├── ssl/dc1/                       # TLS сертификаты (генерируются)
├── data/products.json             # Тестовые товары (6 шт.)
└── report/                        # Результаты отчёта
    ├── PROJECT_REPORT.md
    ├── CRITERIA_REPORT_<ts>.md
    └── NN_*.txt
```
