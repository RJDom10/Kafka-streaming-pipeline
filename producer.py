import os
import json
import time
from confluent_kafka import Producer
import requests

# Configuración vía Variables de Entorno (preparado para Docker)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wiki.changes")
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# 1. Configuración del Productor Kafka
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "wikimedia-producer",
    "acks": "1",  # Confirmación del líder (equilibrio óptimo latencia/durabilidad)
    "compression.type": "snappy",  # Ahorro de ancho de banda y memoria
    "linger.ms": 20,  # Pequeño buffer (20ms) para agrupar mensajes por lotes
}

producer = Producer(producer_config)

def delivery_callback(err, msg):
    """Callback invocado por librdkafka cuando el broker confirma la recepción."""
    if err:
        print(f"❌ Error al entregar mensaje: {err}")

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
        # Convertir timestamp Unix de Wikimedia (segundos) a ISO UTC aproximado
        "event_timestamp": data.get("timestamp", int(time.time())),
    }

def stream_wikimedia():
    headers = {
        "User-Agent": "DataEngineeringLabCourse/1.0 (academic-practice@university.edu)"
    }

    print(f"🔌 Conectando a Wikimedia EventStreams...")
    print(f"📡 Publicando eventos en el tópico '{TOPIC_NAME}' de Kafka...\n")

    try:
        response = requests.get(
            WIKIMEDIA_STREAM_URL, headers=headers, stream=True, timeout=30
        )

        for line in response.iter_lines():
            if not line:
                continue

            decoded = line.decode("utf-8")
            # Los eventos de Server-Sent Events inician con el prefijo "data: "
            if decoded.startswith("data: "):
                raw_json = decoded[6:]
                try:
                    event_data = json.loads(raw_json)
                    payload = sanitize_event(event_data)

                    # Usamos 'wiki' como clave para que ediciones del mismo idioma
                    # viajen a la misma partición y mantengan orden causal
                    message_key = payload["wiki"].encode("utf-8")
                    message_val = json.dumps(payload).encode("utf-8")

                    producer.produce(
                        topic=TOPIC_NAME,
                        key=message_key,
                        value=message_val,
                        callback=delivery_callback,
                    )

                    # Atiende callbacks internos periódicamente sin bloquear
                    producer.poll(0)

                    print(
                        f"[{payload['wiki']}] {payload['user_name']} -> {payload['title'][:40]} ({payload['byte_diff']} bytes)"
                    )

                except json.JSONDecodeError:
                    continue

    except KeyboardInterrupt:
        print("\n🛑 Interrupción por el usuario. Vaciando cola de Kafka...")
    finally:
        # Espera a que todos los mensajes encolados se hayan enviado antes de cerrar
        producer.flush()
        print("✔ Productor desconectado de forma segura.")

if __name__ == "__main__":
    stream_wikimedia()