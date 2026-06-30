#!/usr/bin/env python3
"""
Инструмент командной строки для управления списком запрещённых товаров.
Шаг 4 финального проекта.

Использование:
  python manage_banned.py add <product_id> [--reason "Причина"]
  python manage_banned.py remove <product_id>
  python manage_banned.py list
  python manage_banned.py seed    # добавить тестовые запрещённые товары

Отправляет команды напрямую в Kafka (топик banned-products-commands).
"""

import argparse
import json
import os
import sys
import time

from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092,localhost:9094,localhost:9096")
BAN_TOPIC = "banned-products-commands"

# Начальный список запрещённых товаров (для seed)
INITIAL_BANNED = [
    {"product_id": "22222", "reason": "Нелегальный товар — категория 'Запрещённые'"},
    {"product_id": "55555", "reason": "Запрещённый препарат — не подлежит продаже"},
    {"product_id": "BAN-001", "reason": "SKU в списке запрещённых"},
]


def get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "acks": "all",
        "retries": 3,
    })


def send_command(producer: Producer, action: str, product_id: str, reason: str = ""):
    msg = json.dumps({
        "action": action,
        "product_id": product_id,
        "reason": reason,
    })
    producer.produce(
        topic=BAN_TOPIC,
        key=product_id,
        value=msg,
    )
    producer.flush(timeout=10)


def cmd_add(args):
    p = get_producer()
    send_command(p, "add", args.product_id, args.reason)
    print(f"✅ Команда 'add' отправлена для товара: {args.product_id}")


def cmd_remove(args):
    p = get_producer()
    send_command(p, "remove", args.product_id)
    print(f"✅ Команда 'remove' отправлена для товара: {args.product_id}")


def cmd_list(args):
    """Список через Faust HTTP API."""
    import urllib.request
    faust_url = os.getenv("FAUST_URL", "http://localhost:6066")
    try:
        with urllib.request.urlopen(f"{faust_url}/banned/list", timeout=5) as r:
            data = json.loads(r.read())
            print(f"\n📋 Запрещённых товаров: {data.get('banned_count', 0)}")
            for pid, reason in data.get("banned_products", {}).items():
                print(f"   • {pid}: {reason}")
    except Exception as e:
        print(f"⚠️  Не удалось получить список (Faust не запущен?): {e}")
        print("   Попробуйте: curl http://localhost:6066/banned/list")


def cmd_seed(args):
    """Загружает начальный список запрещённых товаров."""
    p = get_producer()
    print(f"🌱 Загружаем начальный список запрещённых товаров ({len(INITIAL_BANNED)} шт.)...")
    for item in INITIAL_BANNED:
        send_command(p, "add", item["product_id"], item["reason"])
        print(f"   ✅ Добавлен: {item['product_id']} — {item['reason']}")
        time.sleep(0.5)
    print("🏁 Seed завершён!")


def main():
    parser = argparse.ArgumentParser(
        description="Управление списком запрещённых товаров",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python manage_banned.py add 12345 --reason "Подделка"
  python manage_banned.py remove 12345
  python manage_banned.py list
  python manage_banned.py seed
        """,
    )
    parser.add_argument(
        "--brokers",
        default=KAFKA_BROKERS,
        help=f"Kafka brokers (default: {KAFKA_BROKERS})",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # add
    p_add = subparsers.add_parser("add", help="Добавить товар в список запрещённых")
    p_add.add_argument("product_id", help="ID товара")
    p_add.add_argument("--reason", default="запрещён", help="Причина запрета")
    p_add.set_defaults(func=cmd_add)

    # remove
    p_remove = subparsers.add_parser("remove", help="Удалить товар из списка запрещённых")
    p_remove.add_argument("product_id", help="ID товара")
    p_remove.set_defaults(func=cmd_remove)

    # list
    p_list = subparsers.add_parser("list", help="Показать список запрещённых товаров")
    p_list.set_defaults(func=cmd_list)

    # seed
    p_seed = subparsers.add_parser("seed", help="Загрузить начальный список запрещённых товаров")
    p_seed.set_defaults(func=cmd_seed)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
