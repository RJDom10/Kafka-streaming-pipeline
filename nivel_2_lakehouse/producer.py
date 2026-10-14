import os
import json
import time
from confluent_kafka import Producer
import requests

# Configuracion via Variables de Entorno (preparado para Docker)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wiki.changes")
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Configuracion del Productor Kafka
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "wikimedia-producer",
    "acks": "1",
    "compression.type": "snappy",
    "linger.ms": 20,
}

producer = Producer(producer_config)

def delivery_callback(err, msg):
    """Callback invocado por librdkafka cuando el broker confirma la recepcion."""
    if err:
        print(f"[ERROR] Error al entregar mensaje: {err}")

def sanitize_event(data: dict) -> dict:
    """Filtra y limpia el payload de Wikimedia para conservar solo columnas clave."""
    length = data.get("length", {})
    old_len = length.get("old", 0) if isinstance(length, dict) else 0
    new_len = length.get("new", 0) if isinstance(length, dict) else 0

    return {
        "wiki": data.get("wiki", "unknown"),
        "title": data.get("title", ""),
        "user_name": data.get("user", "anonymous"),
        "bot": data.get("bot", False),
        "change_type": data.get("type", "edit"),
        "length_old": old_len,
        "length_new": new_len,
        "byte_diff": (new_len - old_len),
        "event_timestamp": data.get("timestamp", int(time.time())),
    }

def stream_wikimedia():
    headers = {
        "User-Agent": "DataEngineeringLabCourse/2.0 (academic-practice@university.edu)"
    }

    print(f"Conectando a Wikimedia EventStreams...")
    print(f"Publicando eventos en el topico '{TOPIC_NAME}' de Kafka...\n")

    try:
        response = requests.get(
            WIKIMEDIA_STREAM_URL, headers=headers, stream=True, timeout=30
        )

        for line in response.iter_lines():
            if not line:
                continue

            decoded = line.decode("utf-8")
            if decoded.startswith("data: "):
                raw_json = decoded[6:]
                try:
                    event_data = json.loads(raw_json)
                    payload = sanitize_event(event_data)

                    message_key = payload["wiki"].encode("utf-8")
                    message_val = json.dumps(payload).encode("utf-8")

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=message_key,
                        value=message_val,
                        callback=delivery_callback,
                    )

                    producer.poll(0)

                    print(
                        f"[{payload['wiki']}] {payload['user_name']} -> {payload['title'][:40]} ({payload['byte_diff']} bytes)"
                    )

                except json.JSONDecodeError:
                    continue

    except KeyboardInterrupt:
        print("\nInterrupcion por el usuario. Vaciando cola de Kafka...")
    finally:
        producer.flush()
        print("Productor desconectado de forma segura.")

if __name__ == "__main__":
    stream_wikimedia()
