#!/usr/bin/env python3
"""
Аналитическая система — ЦОД-2 (Санкт-Петербург).
Шаг 3 финального проекта.

Читает реплицированные данные из ЦОД-2 (топики с префиксом dc1.),
проводит аналитику и записывает рекомендации обратно в ЦОД-1.

Поток данных:
  ЦОД-1 products-filtered → MirrorMaker2 → ЦОД-2 dc1.products-filtered
  ЦОД-1 client-requests   → MirrorMaker2 → ЦОД-2 dc1.client-requests
                                   ↓
                            Analytics (ЦОД-2)
                                   ↓
  ЦОД-1 recommendations  ← Analytics пишет обратно через backbone-net
"""

import json
import logging
import os
import time
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ANALYTICS/DC2] %(levelname)s %(message)s",
)
log = logging.getLogger("analytics")

# Читаем из ЦОД-2 (реплицированные топики с префиксом dc1.)
KAFKA_DC2 = os.getenv("KAFKA_BROKERS_DC2", "kafka-4:39092,kafka-5:39094,kafka-6:39096")
# Пишем рекомендации обратно в ЦОД-1
KAFKA_DC1 = os.getenv("KAFKA_BROKERS_DC1", "kafka-1:29092,kafka-2:29094,kafka-3:29096")

SOURCE_PRODUCTS   = "dc1.products-filtered"
SOURCE_REQUESTS   = "dc1.client-requests"
DEST_RECOMMENDATIONS = "recommendations"   # в ЦОД-1
ANALYTICS_WINDOW  = int(os.getenv("ANALYTICS_WINDOW", "30"))


def make_consumer(brokers: str, group: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": brokers,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })


def make_producer(brokers: str) -> Producer:
    return Producer({
        "bootstrap.servers": brokers,
        "acks": "all",
        "retries": 5,
    })


def analyze(products: list) -> dict:
    if not products:
        return {}
    categories = Counter(p.get("category", "?") for p in products)
    brands     = Counter(p.get("brand",    "?") for p in products)
    stores     = Counter(p.get("store_id", "?") for p in products)
    inv_value  = sum(
        float(p.get("price", {}).get("amount", 0))
        * int(p.get("stock", {}).get("available", 0))
        for p in products
    )
    return {
        "total_products": len(products),
        "top_categories": dict(categories.most_common(5)),
        "top_brands":     dict(brands.most_common(5)),
        "top_stores":     dict(stores.most_common(3)),
        "inventory_value_rub": round(inv_value, 2),
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }


def generate_recommendations(products: list, requests: list) -> list:
    if not products:
        return []
    searched = [
        r.get("payload", {}).get("query", "").lower()
        for r in requests if r.get("action") == "search"
    ]
    top_cats = [c for c, _ in Counter(p.get("category", "") for p in products).most_common(3)]
    recs = []
    for p in products:
        name = p.get("name", "")
        cat  = p.get("category", "")
        reason = f"Популярная категория: {cat}"
        for term in searched:
            if term and term in name.lower():
                reason = f"Соответствует поиску: «{term}»"
                break
        if cat in top_cats:
            recs.append({
                "product_id": p.get("product_id"),
                "name":       name,
                "category":   cat,
                "price":      p.get("price", {}),
                "reason":     reason,
            })
    return recs[:5]


def send_recommendations(producer: Producer, user_id: str, recs: list):
    if not recs:
        return
    event = {
        "user_id":         user_id,
        "recommendations": recs,
        "source_dc":       "dc2-spb",
        "generated_at":    datetime.utcnow().isoformat() + "Z",
    }
    producer.produce(
        topic=DEST_RECOMMENDATIONS,
        key=user_id,
        value=json.dumps(event, ensure_ascii=False),
    )
    producer.flush(timeout=10)
    log.info(f"📧 Рекомендации → ЦОД-1 для {user_id}: {len(recs)} шт.")


def main():
    log.info("🚀 Аналитическая система (ЦОД-2 / Санкт-Петербург)")
    log.info(f"   Читаю из ЦОД-2: {KAFKA_DC2}")
    log.info(f"   Пишу в ЦОД-1:   {KAFKA_DC1}")
    log.info(f"   Топики-источники: {SOURCE_PRODUCTS}, {SOURCE_REQUESTS}")

    log.info("⏳ Ожидаем MirrorMaker2 (90 сек)...")
    time.sleep(90)

    consumer = make_consumer(KAFKA_DC2, "analytics-dc2-group")
    producer = make_producer(KAFKA_DC1)

    try:
        consumer.subscribe([SOURCE_PRODUCTS, SOURCE_REQUESTS])
    except Exception as e:
        log.warning(f"Не удалось подписаться: {e}. Пробуем через 30 сек...")
        time.sleep(30)
        consumer.subscribe([SOURCE_PRODUCTS, SOURCE_REQUESTS])

    products_buf: list = []
    requests_buf: list = []
    last_analysis = time.time()

    log.info(f"📊 Сбор данных начат. Анализ каждые {ANALYTICS_WINDOW} сек.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"Kafka error: {msg.error()}")
            else:
                try:
                    val   = json.loads(msg.value().decode("utf-8"))
                    topic = msg.topic()
                    if "products" in topic:
                        products_buf.append(val)
                        log.debug(f"📦 {val.get('name')}")
                    elif "requests" in topic:
                        requests_buf.append(val)
                        log.debug(f"🔍 {val.get('action')}")
                except Exception as exc:
                    log.error(f"Ошибка обработки: {exc}")

            if time.time() - last_analysis >= ANALYTICS_WINDOW:
                if products_buf:
                    log.info(f"\n{'─'*55}")
                    log.info(f"📊 ОТЧЁТ ЦОД-2 (за {ANALYTICS_WINDOW} сек.)")
                    stats = analyze(products_buf)
                    log.info(f"   Товаров:      {stats['total_products']}")
                    log.info(f"   Топ кат.:     {stats['top_categories']}")
                    log.info(f"   Топ брендов:  {stats['top_brands']}")
                    log.info(f"   Запасы (руб): {stats['inventory_value_rub']:,.0f}")

                    recs = generate_recommendations(products_buf, requests_buf)
                    users = {r.get("user_id") for r in requests_buf if r.get("user_id")}
                    for uid in (users or {"all_users"}):
                        send_recommendations(producer, uid, recs)

                    products_buf.clear()
                    requests_buf.clear()
                else:
                    log.info("⏳ Данных ещё нет. Ждём репликации из ЦОД-1...")

                last_analysis = time.time()

    except KeyboardInterrupt:
        log.info("🛑 Остановлен")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
