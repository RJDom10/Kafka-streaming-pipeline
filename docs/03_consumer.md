# 📄 Documentación: `consumer.py`

**Archivo:** `consumer.py`
**Propósito:** Script Python que actúa como **Consumidor Kafka**. Lee los mensajes del tópico `wiki.changes`, los transforma mínimamente y los persiste en lotes en la tabla `wiki_recent_changes` de PostgreSQL de forma eficiente.

---

## Dependencias e Imports

```python
import os
```
Para leer variables de entorno con `os.getenv()`. Permite configurar tanto la conexión a Kafka como la conexión a PostgreSQL sin hardcodear valores en el código.

```python
import time
```
Solo se usa en el manejo de errores: `time.sleep(2)` cuando el tópico aún no existe. Hace que el consumer espere 2 segundos antes de reintentar en lugar de entrar en un loop de CPU al 100%.

```python
from datetime import datetime
```
Se usa para convertir el `event_timestamp` (número entero Unix, segundos desde 1970) a un objeto `datetime` de Python que `psycopg2` pueda insertar correctamente en la columna `TIMESTAMPTZ` de PostgreSQL.

```python
import json
```
Para deserializar (parsear) el valor del mensaje Kafka de bytes→string→diccionario Python.

```python
import sys
```
Solo se usa para `sys.exit(1)`: terminar el proceso con código de error si no puede conectarse a PostgreSQL al inicio. El código `1` indica error (vs `0` que indica éxito).

```python
from confluent_kafka import Consumer, KafkaError
```
- `Consumer`: Clase cliente para consumir mensajes de Kafka
- `KafkaError`: Clase con constantes de códigos de error de Kafka para manejar errores específicos

```python
import psycopg2
from psycopg2.extras import execute_batch
```
- `psycopg2`: El driver PostgreSQL más usado en Python. Permite ejecutar SQL contra PostgreSQL.
- `execute_batch`: Función optimizada de `psycopg2.extras` que ejecuta la misma sentencia SQL múltiples veces con diferentes parámetros en un único round-trip de red, en lugar de N round-trips separados. Esto es **mucho más eficiente** que un loop de `cursor.execute()`.

---

## Configuración de Conexiones

### Kafka
```python
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wiki.changes")
GROUP_ID = os.getenv("GROUP_ID", "wiki-persister-group")
```
- `KAFKA_BROKER`: Dirección del broker. Por defecto `localhost:9092` para ejecutar fuera de Docker.
- `TOPIC_NAME`: Tópico a consumir.
- `GROUP_ID`: **Identificador del grupo de consumidores.**

### ¿Qué es un Consumer Group?
El `GROUP_ID` es fundamental en Kafka. Cuando múltiples consumidores comparten el mismo `GROUP_ID`:
- Kafka distribuye las particiones del tópico entre ellos automáticamente
- Cada partición es asignada a exactamente un consumidor del grupo
- Si hay 3 particiones y 3 consumidores en el mismo grupo → cada uno procesa 1 partición en paralelo
- Si hay más consumidores que particiones → los extras quedan inactivos

### PostgreSQL
```python
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB", "wikidb")
PG_USER = os.getenv("PG_USER", "wikiuser")
PG_PASS = os.getenv("PG_PASSWORD", "wikipassword")
```
Parámetros de conexión a PostgreSQL leídos de variables de entorno. `PG_PORT` se convierte a `int()` porque todas las variables de entorno son strings y `psycopg2` espera el puerto como número entero.

```python
BATCH_SIZE = 50
```
**🔑 VARIABLE CLAVE.** Número de registros que se acumulan antes de ejecutar el `INSERT` masivo. Con `50`, el consumer acumula 50 mensajes y los inserta en una sola transacción SQL. Elegido como equilibrio entre:
- Muy pequeño (1-5): Muchas transacciones, mucho overhead
- Muy grande (500+): Mayor latencia, mayor riesgo de pérdida en caso de fallo

---

## Función: `get_pg_connection()`

```python
def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        connect_timeout=5,
    )
```
Función simple que crea y retorna una conexión a PostgreSQL. `connect_timeout=5` hace que falle en 5 segundos si no puede conectarse, en vez de esperar indefinidamente.

Está separada en su propia función para facilitar la **reconexión** en caso de que la conexión se pierda (aunque en este lab no se implementa reconexión automática).

---

## Función Principal: `main()`

### Configuración del Consumer Kafka

```python
consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
```
Punto de entrada al clúster Kafka.

```python
    "group.id": GROUP_ID,
```
Registra este consumidor bajo el grupo `wiki-persister-group`.

```python
    "auto.offset.reset": "earliest",
```
**¿Desde dónde empezar a leer?** Si este consumer group no tiene offsets guardados (es la primera vez que corre):
- `"earliest"`: Lee desde el mensaje más antiguo disponible en Kafka. No pierde mensajes históricos.
- `"latest"`: Solo lee mensajes nuevos que lleguen después de que el consumer arranque.

```python
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 2000,
```
- `enable.auto.commit: True`: Kafka guarda automáticamente la posición (offset) del último mensaje procesado.
- `auto.commit.interval.ms: 2000`: Guarda el offset cada 2 segundos.

**¿Por qué es importante el commit de offsets?**
El offset es un número que indica hasta qué mensaje llegó cada consumer group en cada partición. Si el consumer se reinicia (fallo, actualización), Kafka sabe desde qué mensaje retomar gracias al offset guardado. Sin commits, al reiniciar procesaría los mensajes desde el principio.

```python
consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])
```
Crea el consumidor y se suscribe al tópico. Con `subscribe` (vs `assign`), Kafka gestiona automáticamente la asignación de particiones y los rebalanceos.

### Conexión a PostgreSQL

```python
try:
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    print("✅ Conectado exitosamente a PostgreSQL (wikidb).")
except Exception as e:
    print(f"❌ Error al conectar a Postgres: {e}")
    sys.exit(1)
```
Intenta conectarse a PostgreSQL al inicio. Si falla (ej: postgres aún no está listo), termina el proceso con `sys.exit(1)`. Docker tiene `restart: on-failure`, así que Docker lo reiniciará automáticamente hasta que PostgreSQL esté disponible.

### Query de Inserción

```python
insert_query = """
    INSERT INTO wiki_recent_changes (
        wiki, title, user_name, bot, change_type, 
        length_old, length_new, byte_diff, event_timestamp
    ) VALUES (
        %(wiki)s, %(title)s, %(user_name)s, %(bot)s, %(change_type)s,
        %(length_old)s, %(length_new)s, %(byte_diff)s, %(event_timestamp)s
    );
"""
```
La sentencia SQL de inserción usa **parámetros nombrados** (`%(nombre)s`). `psycopg2` sustituye estos placeholders con los valores reales de cada diccionario en el batch. Esto es **SQL parameterizado**: previene inyección SQL y es más eficiente que concatenar strings.

---

## Loop Principal de Consumo

```python
batch_records = []
```
Lista que acumula los registros procesados hasta llegar a `BATCH_SIZE`.

```python
while True:
    msg = consumer.poll(timeout=1.0)
```
- `consumer.poll()`: Obtiene el siguiente mensaje disponible. Si no hay mensajes, espera hasta `timeout=1.0` segundo y retorna `None`.
- El loop `while True` es el corazón del consumer: corre indefinidamente hasta una interrupción.

```python
    if msg is None:
        continue
```
Si no llegó ningún mensaje en 1 segundo, vuelve al inicio del loop.

### Manejo de Errores de Kafka

```python
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
```
`_PARTITION_EOF` no es un error: significa que el consumer llegó al último mensaje disponible en esa partición y está al día. Se ignora y se continúa esperando nuevos mensajes.

```python
        elif msg.error().code() in (
            KafkaError.UNKNOWN_TOPIC_OR_PART,
            KafkaError._UNKNOWN_TOPIC,
        ):
            print(f"⏳ Esperando a que el tópico '{TOPIC_NAME}' esté disponible...")
            time.sleep(2)
            continue
```
Si el tópico no existe aún (posible race condition al arrancar), espera 2 segundos y reintenta. Aunque `depends_on` de Docker Compose debería garantizar el orden, este código agrega robustez extra.

```python
        else:
            print(f"❌ Error crítico de Kafka: {msg.error()}")
            break
```
Cualquier otro error (ej: pérdida de conexión con el broker) rompe el loop. Docker reiniciará el proceso.

### Procesamiento del Mensaje

```python
    data = json.loads(msg.value().decode("utf-8"))
```
Convierte el valor del mensaje de bytes→string con `.decode("utf-8")` y luego el string JSON→diccionario Python con `json.loads()`.

```python
    ts_raw = data.get("event_timestamp", 0)
    data["event_timestamp"] = (
        datetime.utcfromtimestamp(ts_raw)
        if ts_raw
        else datetime.utcnow()
    )
```
**Conversión de timestamp.** El producer guardó el timestamp como un entero Unix (segundos desde 1970). PostgreSQL necesita un objeto `datetime`. `datetime.utcfromtimestamp(ts_raw)` hace la conversión. Si `ts_raw` es 0 o None, usa `datetime.utcnow()` como fallback.

```python
    batch_records.append(data)
```
Agrega el registro procesado a la lista del batch.

### Inserción en Lote

```python
    if len(batch_records) >= BATCH_SIZE:
        execute_batch(pg_cursor, insert_query, batch_records)
        pg_conn.commit()
        print(f"💾 {len(batch_records)} registros insertados en Postgres. (Offset: {msg.offset()})")
        batch_records.clear()
```
Cuando el batch alcanza 50 registros:
1. `execute_batch()`: Envía los 50 INSERTs en un solo round-trip a PostgreSQL
2. `pg_conn.commit()`: Confirma la transacción. Hasta este punto los datos están en memoria de PostgreSQL; el commit los escribe al disco de forma durable.
3. `print()`: Log informativo que muestra el offset actual (posición en el stream de Kafka)
4. `batch_records.clear()`: Limpia la lista para el siguiente batch

---

## Cierre Limpio (Finally)

```python
except KeyboardInterrupt:
    print("\n⚡ Deteniendo consumidor...")
finally:
    if batch_records:
        execute_batch(pg_cursor, insert_query, batch_records)
        pg_conn.commit()
        print(f"💾 {len(batch_records)} registros residuales guardados en Postgres.")

    pg_cursor.close()
    pg_conn.close()
    consumer.close()
```
El bloque `finally` se ejecuta **siempre**, ya sea por Ctrl+C o por error. Garantiza:
1. **Persistir el batch parcial**: Si había mensajes acumulados que no llegaron a 50, se insertan de todas formas.
2. **Cerrar el cursor y conexión de PostgreSQL**: Libera recursos del lado de la BD.
3. **`consumer.close()`**: Notifica a Kafka que este consumidor se desconecta. Kafka redistribuye sus particiones a otros consumidores del grupo (si los hay) de forma ordenada (rebalanceo limpio vs. esperar el timeout de sesión).

---

## Flujo Resumido

```
Kafka Broker (tópico wiki.changes)
       |
       v  consumer.poll(timeout=1.0)
   Mensaje recibido (bytes)
       |
       v  .decode("utf-8") + json.loads()
   Diccionario Python
       |
       v  Conversión de timestamp Unix → datetime
   Registro listo para BD
       |
       v  batch_records.append()
   Acumular hasta BATCH_SIZE=50
       |
       v  execute_batch() + commit()
   PostgreSQL (tabla wiki_recent_changes)
```

## Comparativa: execute_batch vs. INSERT individual

| Método | 50 registros | Round-trips a PG | Eficiencia |
|---|---|---|---|
| `cursor.execute()` en loop | 50 queries separadas | 50 | ❌ Lento |
| `execute_batch()` | 1 llamada múltiple | 1-2 | ✅ Rápido |
| `execute_values()` | 1 query con VALUES múltiples | 1 | ✅✅ Más rápido |
