FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.3.0

COPY shop_api.py /app/shop_api.py

CMD ["python3", "/app/shop_api.py"]
