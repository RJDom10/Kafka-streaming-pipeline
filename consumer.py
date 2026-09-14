import os
import time
from datetime import datetime
import json
import sys
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_batch

# Configuración vía Variables de Entorno (preparado para Docker)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wiki.changes")
GROUP_ID = os.getenv("GROUP_ID", "wiki-persister-group")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB", "wikidb")
PG_USER = os.getenv("PG_USER", "wikiuser")
PG_PASS = os.getenv("PG_PASSWORD", "wikipassword")

BATCH_SIZE = 50

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        connect_timeout=5,
    )

def main():
    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 2000,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_NAME])

    try:
        pg_conn = get_pg_connection()
        pg_cursor = pg_conn.cursor()
        print("✔ Conectado exitosamente a PostgreSQL (wikidb).")
    except Exception as e:
        print(f"❌ Error al conectar a Postgres: {e}")
        sys.exit(1)

    insert_query = """
        INSERT INTO wiki_recent_changes (
            wiki, title, user_name, bot, change_type, 
            length_old, length_new, byte_diff, event_timestamp
        ) VALUES (
            %(wiki)s, %(title)s, %(user_name)s, %(bot)s, %(change_type)s,
            %(length_old)s, %(length_new)s, %(byte_diff)s, %(event_timestamp)s
        );
    """

    print(f"👂 Consumidor suscrito a '{TOPIC_NAME}'. Esperando eventos...")
    batch_records = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            
            if msg.error():
                # Fin de partición alcanzado (normal)
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # El tópico aún no existe (esperar y reintentar en lugar de fallar)
                elif msg.error().code() in (
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                    KafkaError._UNKNOWN_TOPIC,
                ):
                    print(f"⏳ Esperando a que el tópico '{TOPIC_NAME}' esté disponible...")
                    time.sleep(2)
                    continue
                else:
                    print(f"❌ Error crítico de Kafka: {msg.error()}")
                    break

            try:
                data = json.loads(msg.value().decode("utf-8"))
                ts_raw = data.get("event_timestamp", 0)
                data["event_timestamp"] = (
                    datetime.utcfromtimestamp(ts_raw)
                    if ts_raw
                    else datetime.utcnow()
                )

                batch_records.append(data)

                if len(batch_records) >= BATCH_SIZE:
                    execute_batch(pg_cursor, insert_query, batch_records)
                    pg_conn.commit()
                    print(f"💾 {len(batch_records)} registros insertados en Postgres. (Offset: {msg.offset()})")
                    batch_records.clear()

            except Exception as ex:
                print(f"⚠️ Error procesando registro: {ex}")
                pg_conn.rollback()

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo consumidor...")
    finally:
        if batch_records:
            execute_batch(pg_cursor, insert_query, batch_records)
            pg_conn.commit()
            print(f"💾 {len(batch_records)} registros residuales guardados en Postgres.")

        pg_cursor.close()
        pg_conn.close()
        consumer.close()
        print("✔ Conexiones cerradas.")

if __name__ == "__main__":
    main()