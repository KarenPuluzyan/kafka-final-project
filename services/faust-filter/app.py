#!/usr/bin/env python3
"""
Faust-приложение: фильтрация запрещённых товаров.
Шаг 4 финального проекта.

Использует опыт из Kafka_Yandex_Block2 (app.py с Faust).

Функции:
- Читает товары из топика 'products'
- Проверяет по списку запрещённых (Faust Table)
- Разрешённые товары → топик 'products-filtered'
- Запрещённые → отбрасываются (логируются)
- CLI управление списком: python manage_banned.py add/remove/list
"""

import faust
import logging
import os

# ─── Настройка ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FILTER] %(levelname)s %(message)s",
)
log = logging.getLogger("faust-filter")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:29092,kafka-2:29094,kafka-3:29096")
BROKER_URL = "kafka://" + KAFKA_BROKERS.split(",")[0]

# ─── Faust-приложение ─────────────────────────────────────
app = faust.App(
    "marketplace-filter",
    broker=BROKER_URL,
    value_serializer="json",
    web_port=6066,
)

# ─── Модели данных ───────────────────────────────────────
class Product(faust.Record, serializer="json"):
    product_id: str
    name: str
    category: str = ""
    store_id: str = ""
    price: dict = {}
    description: str = ""
    brand: str = ""
    stock: dict = {}
    sku: str = ""
    tags: list = []
    images: list = []
    specifications: dict = {}
    created_at: str = ""
    updated_at: str = ""
    index: str = "products"


class BanCommand(faust.Record, serializer="json"):
    action: str   # "add" | "remove"
    product_id: str
    reason: str = ""


# ─── Топики ──────────────────────────────────────────────
products_topic = app.topic("products", value_type=Product)
filtered_topic = app.topic("products-filtered", value_type=Product)
ban_commands_topic = app.topic("banned-products-commands", value_type=BanCommand)

# ─── Таблица запрещённых товаров (in-memory) ─────────────
# Хранит: product_id -> reason
banned_table = app.Table("banned_products", default=str, partitions=1)


# ─── Агент: обработка команд управления списком ──────────
@app.agent(ban_commands_topic)
async def handle_ban_commands(commands):
    async for cmd in commands:
        if cmd.action == "add":
            banned_table[cmd.product_id] = cmd.reason or "запрещён"
            log.info(f"[BAN ADD] Добавлен в список запрещённых: {cmd.product_id}")
        elif cmd.action == "remove":
            if cmd.product_id in banned_table:
                del banned_table[cmd.product_id]
                log.info(f"[BAN REMOVE] Удалён из списка запрещённых: {cmd.product_id}")
        log.info(f"[BAN LIST] Всего запрещённых: {len(banned_table)}")


# ─── Агент: фильтрация товаров ───────────────────────────
@app.agent(products_topic)
async def filter_products(products):
    async for product in products:
        product_id = product.product_id
        name = product.name

        if product_id in banned_table:
            reason = banned_table[product_id]
            log.warning(
                f"[BANNED] Товар отфильтрован: {product_id} | {name} | Причина: {reason}"
            )
            # Запрещённый товар не передаётся дальше
            continue

        # Разрешённый товар → следующий топик
        await filtered_topic.send(value=product)
        log.info(f"[ALLOWED] Товар разрешён: {product_id} | {name}")


# ─── HTTP API для управления списком ─────────────────────
@app.page("/banned/add/{product_id}")
@app.table_route(table=banned_table, match_info="product_id")
async def add_banned(web, request, product_id):
    reason = request.query.get("reason", "запрещён вручную")
    await ban_commands_topic.send(
        value=BanCommand(action="add", product_id=product_id, reason=reason)
    )
    return web.json({"status": "queued", "action": "add", "product_id": product_id})


@app.page("/banned/remove/{product_id}")
@app.table_route(table=banned_table, match_info="product_id")
async def remove_banned(web, request, product_id):
    await ban_commands_topic.send(
        value=BanCommand(action="remove", product_id=product_id)
    )
    return web.json({"status": "queued", "action": "remove", "product_id": product_id})


@app.page("/banned/list")
async def list_banned(web, request):
    return web.json({
        "banned_count": len(banned_table),
        "banned_products": dict(banned_table.items()),
    })


if __name__ == "__main__":
    app.main()
