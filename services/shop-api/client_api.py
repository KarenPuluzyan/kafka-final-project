#!/usr/bin/env python3
"""
CLIENT API — терминальный клиент для маркетплейса.
Шаг 1 финального проекта: CLIENT API.

Поддерживает команды:
  search <name>       — поиск товара по имени
  recommend           — получить персонализированные рекомендации
  quit                — выход

Запросы публикуются:
  1. В Kafka топик 'client-requests' (для аналитики)
  2. В локальное хранилище (simple JSON DB) для поиска

Использует опыт из Kafka_Yandex (Producer) и Block2 (Faust agents).
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

# ─── Настройка ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLIENT-API] %(message)s",
)
log = logging.getLogger("client-api")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092,localhost:9094,localhost:9096")
REQUEST_TOPIC = "client-requests"
RECOMMENDATIONS_TOPIC = "recommendations"
USER_ID = os.getenv("USER_ID", f"user_{uuid.uuid4().hex[:8]}")


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "acks": "all",
        "retries": 3,
    })


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": f"client-api-{USER_ID}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def send_request(producer: Producer, action: str, payload: dict):
    """Отправляет запрос клиента в Kafka для аналитики."""
    event = {
        "request_id": uuid.uuid4().hex,
        "user_id": USER_ID,
        "action": action,
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    producer.produce(
        topic=REQUEST_TOPIC,
        key=USER_ID,
        value=json.dumps(event, ensure_ascii=False),
    )
    producer.flush(timeout=5)


def search_products(producer: Producer, name: str):
    """Ищет товары по имени (локально в файле products.json)."""
    products_file = os.path.join(os.path.dirname(__file__), "../../data/products.json")

    # Отправляем событие поиска в Kafka
    send_request(producer, "search", {"query": name})

    # Локальный поиск
    try:
        with open(products_file, "r", encoding="utf-8") as f:
            products = json.load(f)

        results = [
            p for p in products
            if name.lower() in p.get("name", "").lower()
            or name.lower() in p.get("description", "").lower()
        ]

        if results:
            print(f"\n🔍 Найдено товаров: {len(results)}")
            for p in results:
                price = p.get("price", {})
                print(f"\n  📦 {p['name']} (ID: {p['product_id']})")
                print(f"     Категория: {p.get('category', 'N/A')}")
                print(f"     Цена: {price.get('amount')} {price.get('currency')}")
                print(f"     В наличии: {p.get('stock', {}).get('available', 0)} шт.")
        else:
            print(f"\n❌ Товары по запросу '{name}' не найдены.")

    except FileNotFoundError:
        print("⚠️  Файл базы данных товаров не найден.")


def get_recommendations(producer: Producer, consumer: Consumer):
    """Получает персонализированные рекомендации."""
    print(f"\n🎯 Запрашиваем рекомендации для пользователя {USER_ID}...")

    # Отправляем запрос в Kafka
    send_request(producer, "get_recommendations", {"user_id": USER_ID})

    # Подписываемся на топик рекомендаций
    consumer.subscribe([RECOMMENDATIONS_TOPIC])

    print("⏳ Ожидаем рекомендации (10 сек)...")
    deadline = time.time() + 10
    found = False

    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"❌ Ошибка Kafka: {msg.error()}")
            break

        try:
            rec = json.loads(msg.value().decode("utf-8"))
            if rec.get("user_id") == USER_ID or not rec.get("user_id"):
                print("\n🌟 Персонализированные рекомендации:")
                for item in rec.get("recommendations", []):
                    print(f"   • {item.get('name')} — {item.get('reason', '')}")
                found = True
                break
        except Exception:
            pass

    if not found:
        print("ℹ️  Рекомендации ещё формируются. Повторите запрос позже.")


def print_help():
    print("""
╔══════════════════════════════════════════════════════╗
║        CLIENT API — Маркетплейс «Покупай выгодно»   ║
╚══════════════════════════════════════════════════════╝

Доступные команды:
  search <название>   — поиск товара по названию
  recommend           — получить персонализированные рекомендации
  help                — показать эту справку
  quit / exit         — выйти
""")


def main():
    print(f"👤 Вы подключились как пользователь: {USER_ID}")
    print(f"🌐 Kafka brokers: {KAFKA_BROKERS}")
    print_help()

    try:
        producer = create_producer()
        consumer = create_consumer()
    except KafkaException as e:
        log.error(f"Ошибка подключения к Kafka: {e}")
        sys.exit(1)

    try:
        while True:
            try:
                cmd_line = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n👋 До свидания!")
                break

            if not cmd_line:
                continue

            parts = cmd_line.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""

            if cmd in ("quit", "exit"):
                print("👋 До свидания!")
                break
            elif cmd == "help":
                print_help()
            elif cmd == "search":
                if not arg:
                    print("❗ Укажите название товара: search <название>")
                else:
                    search_products(producer, arg)
            elif cmd == "recommend":
                get_recommendations(producer, consumer)
            else:
                print(f"❓ Неизвестная команда: {cmd}. Введите 'help' для справки.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
