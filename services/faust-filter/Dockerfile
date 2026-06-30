FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    faust-streaming==0.11.3 \
    aiokafka==0.10.0 \
    confluent-kafka==2.3.0

COPY app.py /app/app.py
COPY manage_banned.py /app/manage_banned.py

CMD ["faust", "-A", "app", "worker", "--loglevel=info"]
