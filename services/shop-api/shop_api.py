#!/usr/bin/env python3
"""
SHOP API — эмулятор отправки товаров в Apache Kafka.
Шаг 1 финального проекта: SHOP API.

Читает список товаров из файла products.json
и публикует каждый товар в топик Kafka 'products'.

Использует опыт из Kafka_Yandex_Block1 (Producer_log.py):
- Гарантия доставки At Least Once: acks="all", retries=5
- Логирование ошибок
"""

import json
import logging
import os
import sys
import time

from confluent_kafka import Producer, KafkaException

# ─── Настройка логирования ───────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SHOP-API] %(levelname)s %(message)s",
)
log = logging.getLogger("shop-api")

# ─── Конфигурация ─────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:29092,kafka-2:29094,kafka-3:29096")
PRODUCTS_FILE = os.getenv("PRODUCTS_FILE", "/app/products.json")
TOPIC = "products"
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "5"))   # секунды между отправками


def delivery_callback(err, msg):
    """Callback подтверждения доставки."""
    if err:
        log.error(f"❌ Ошибка доставки: {err}")
    else:
        log.info(
            f"✅ Доставлено: topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def create_producer() -> Producer:
    """Создаёт Kafka-продюсер с гарантией At Least Once."""
    config = {
        "bootstrap.servers": KAFKA_BROKERS,
        "acks": "all",           # ждём подтверждения всех ISR реплик
        "retries": 5,
        "retry.backoff.ms": 1000,
        "enable.idempotence": True,
    }
    return Producer(config)


def load_products(filepath: str) -> list:
    """Загружает список товаров из JSON файла."""
    with open(filepath, "r", encoding="utf-8") as f:
        products = json.load(f)
    log.info(f"📦 Загружено товаров: {len(products)} из {filepath}")
    return products


def send_products(producer: Producer, products: list):
    """Отправляет все товары в Kafka топик."""
    for product in products:
        product_id = product.get("product_id", "unknown")
        try:
            producer.produce(
                topic=TOPIC,
                key=product_id,
                value=json.dumps(product, ensure_ascii=False),
                callback=delivery_callback,
            )
            log.info(f"📤 Отправляем товар: {product_id} — {product.get('name')}")
            producer.poll(0)  # неблокирующий poll
            time.sleep(0.1)   # небольшая задержка между сообщениями
        except KafkaException as e:
            log.error(f"❌ Ошибка Kafka для товара {product_id}: {e}")

    # Ждём подтверждений всех сообщений
    producer.flush(timeout=30)
    log.info("🏁 Все товары отправлены и подтверждены.")


def main():
    log.info("🚀 SHOP API запущен")
    log.info(f"   Kafka brokers: {KAFKA_BROKERS}")
    log.info(f"   Топик: {TOPIC}")
    log.info(f"   Файл товаров: {PRODUCTS_FILE}")

    # Ждём готовности Kafka
    log.info("⏳ Ожидаем готовности Kafka (30 сек)...")
    time.sleep(30)

    try:
        products = load_products(PRODUCTS_FILE)
        producer = create_producer()

        # Непрерывная отправка (цикл для демонстрации)
        cycle = 1
        while True:
            log.info(f"\n{'='*50}")
            log.info(f"🔄 Цикл отправки #{cycle}")
            send_products(producer, products)
            log.info(f"⏳ Следующий цикл через {SEND_INTERVAL} сек...")
            time.sleep(SEND_INTERVAL)
            cycle += 1

    except FileNotFoundError:
        log.error(f"❌ Файл товаров не найден: {PRODUCTS_FILE}")
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("🛑 SHOP API остановлен пользователем")


if __name__ == "__main__":
    main()
