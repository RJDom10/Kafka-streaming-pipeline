# 📄 Documentación: `producer.py`

**Archivo:** `producer.py`
**Propósito:** Script Python que actúa como **Productor Kafka**. Se conecta al stream público de eventos en tiempo real de Wikimedia (Wikipedia), limpia y transforma cada evento, y los publica en el tópico `wiki.changes` de Kafka para que otros servicios los consuman.

---

## Dependencias e Imports

```python
import os
```
Módulo estándar de Python para interactuar con el sistema operativo. Se usa aquí exclusivamente para leer **variables de entorno** con `os.getenv()`. Esto permite que el script sea configurable sin modificar su código fuente: las variables se inyectan desde `docker-compose.yml`.

```python
import json
```
Módulo estándar para serializar y deserializar datos en formato JSON. Se usa para:
1. **Parsear** los eventos crudos que llegan de Wikimedia (que vienen en formato JSON como string)
2. **Serializar** el payload limpio a JSON string antes de enviarlo a Kafka

```python
import time
```
Módulo estándar de Python para operaciones relacionadas con tiempo. Se usa en el fallback de `event_timestamp`: si el evento de Wikimedia no incluye timestamp, se usa `int(time.time())` que devuelve los segundos desde Unix Epoch (1 enero 1970 UTC).

```python
from confluent_kafka import Producer
```
Importa la clase `Producer` de la librería `confluent-kafka` (versión 2.4.0 según `requirements.txt`). Esta librería es el **cliente oficial de Confluent** para Kafka en Python. Internamente usa `librdkafka`, una librería en C extremadamente eficiente y testeada en producción.

```python
import requests
```
Librería HTTP para Python. Se usa para hacer la petición HTTP GET al endpoint SSE (Server-Sent Events) de Wikimedia y procesar la respuesta como un stream continuo con `stream=True`.

---

## Configuración del Script

```python
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
```
Lee la variable de entorno `KAFKA_BROKER`. Si no existe (cuando corres el script fuera de Docker), usa `"kafka:29092"` como valor por defecto. En Docker, esta variable viene de `docker-compose.yml` como `kafka:29092` (el hostname interno del broker).

```python
TOPIC_NAME = os.getenv("TOPIC_NAME", "wiki.changes")
```
Nombre del tópico Kafka donde se publicarán los mensajes. Configurable via variable de entorno; por defecto `wiki.changes`.

```python
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
```
URL del endpoint SSE de Wikimedia. `recentchange` es el stream que incluye **todas** las ediciones, creaciones y eliminaciones de páginas en todos los proyectos de Wikimedia (Wikipedia en todos los idiomas, Wikidata, Wikisource, etc.) en tiempo real. Este endpoint emite eventos continuamente las 24 horas.

---

## Configuración del Productor Kafka

```python
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
```
Dirección del broker Kafka al que se conectará el productor para enviar mensajes. `bootstrap` porque es el punto inicial de contacto: al conectarse, el cliente descubre automáticamente toda la topología del clúster.

```python
    "client.id": "wikimedia-producer",
```
Identificador del cliente que aparece en los logs de Kafka y en Kafka UI. Útil para debugging cuando hay múltiples productores.

```python
    "acks": "1",
```
**Configuración de durabilidad/latencia.** Controla cuántas réplicas deben confirmar recibir el mensaje antes de que el productor lo considere enviado exitosamente:
- `acks: "0"` — No espera confirmación (más rápido, puede perder mensajes)
- `acks: "1"` — Solo el líder confirma (equilibrio óptimo) ← **Este valor**
- `acks: "all"` — Todas las réplicas deben confirmar (más seguro, más lento)

```python
    "compression.type": "snappy",
```
Comprime los mensajes antes de enviarlos al broker. `snappy` es el algoritmo de Google: excelente balance entre velocidad de compresión/descompresión y ratio de compresión (~30-50% menos datos). Alternativas: `gzip` (mejor compresión, más lento), `lz4` (más rápido, menor compresión), `zstd` (mejor que gzip en ambas métricas).

```python
    "linger.ms": 20,
```
**Configuración de micro-batching.** El productor esperará hasta 20 milisegundos antes de enviar un lote de mensajes. En lugar de enviar mensaje por mensaje, agrupa los que lleguen en ese intervalo en un solo envío de red. Como Wikimedia genera decenas de eventos por segundo, esto ahorra muchas operaciones de I/O.

---

## Función: `delivery_callback(err, msg)`

```python
def delivery_callback(err, msg):
    """Callback invocado por librdkafka cuando el broker confirma la recepción."""
    if err:
        print(f"❌ Error al entregar mensaje: {err}")
```
Esta función es un **callback asíncrono** que `librdkafka` invoca cuando el broker confirma (o rechaza) la entrega de un mensaje. Se registra en cada llamada a `producer.produce(..., callback=delivery_callback)`.

Por qué es asíncrono: el productor no espera la confirmación de forma bloqueante. Internamente tiene una cola de mensajes en vuelo. Los callbacks se procesan cuando se llama a `producer.poll()` o `producer.flush()`. Esto permite enviar miles de mensajes sin esperar la confirmación de cada uno individualmente.

---

## Función: `sanitize_event(data: dict) -> dict`

```python
def sanitize_event(data: dict) -> dict:
```
Recibe el evento crudo de Wikimedia (que puede tener 30+ campos) y devuelve un diccionario limpio con solo los 9 campos relevantes para el laboratorio. Esto es **transformación de datos** en el nivel del productor.

```python
    length = data.get("length", {})
```
El campo `length` en los eventos de Wikimedia es un diccionario anidado con subclaves `old` (longitud anterior en bytes) y `new` (longitud nueva). `.get("length", {})` devuelve `{}` si el campo no existe, evitando `KeyError`.

```python
    old_len = length.get("old", 0) if isinstance(length, dict) else 0
    new_len = length.get("new", 0) if isinstance(length, dict) else 0
```
Doble validación: primero verifica que `length` sea un diccionario (no todos los tipos de eventos tienen este campo), luego extrae los valores con `0` como fallback.

```python
    return {
        "wiki": data.get("wiki", "unknown"),          # Ej: "enwiki", "eswiki", "wikidata"
        "title": data.get("title", ""),                # Título de la página editada
        "user_name": data.get("user", "anonymous"),   # Usuario o IP que hizo el cambio
        "bot": data.get("bot", False),                 # True si fue un bot automatizado
        "change_type": data.get("type", "edit"),       # "edit", "new", "log", "categorize"
        "length_old": old_len,                         # Bytes antes del cambio
        "length_new": new_len,                         # Bytes después del cambio
        "byte_diff": (new_len - old_len),              # Diferencia: positivo=adición, negativo=borrado
        "event_timestamp": data.get("timestamp", int(time.time())),  # Unix timestamp del evento
    }
```

---

## Función Principal: `stream_wikimedia()`

```python
headers = {
    "User-Agent": "DataEngineeringLabCourse/1.0 (academic-practice@university.edu)"
}
```
Wikimedia requiere un User-Agent descriptivo en las peticiones. Es una política de cortesía/identificación. Sin un User-Agent válido, el servidor puede rechazar la conexión.

```python
response = requests.get(
    WIKIMEDIA_STREAM_URL, headers=headers, stream=True, timeout=30
)
```
- `stream=True`: **Clave para Server-Sent Events.** Indica a `requests` que NO descargue toda la respuesta de una vez en memoria. En su lugar, mantiene la conexión HTTP abierta y permite leer línea por línea indefinidamente. Sin `stream=True` el script esperaría que la respuesta "termine" (lo cual nunca ocurre en un stream).
- `timeout=30`: Si no recibe respuesta inicial en 30 segundos, lanza una excepción.

```python
for line in response.iter_lines():
```
Itera sobre el stream línea por línea. Cada evento SSE de Wikimedia llega como un bloque de texto con el formato:
```
data: {"wiki":"enwiki","title":"Python (programming language)",...}

data: {"wiki":"dewiki","title":"Algorithmus",...}
```

```python
    if not line:
        continue
```
Las líneas vacías en SSE son separadores entre eventos. Se descartan.

```python
    decoded = line.decode("utf-8")
    if decoded.startswith("data: "):
        raw_json = decoded[6:]
```
El protocolo SSE prefija las líneas de datos con `"data: "` (6 caracteres). Se decodifica de bytes a string y se extrae el JSON puro quitando ese prefijo.

```python
        event_data = json.loads(raw_json)
        payload = sanitize_event(event_data)
```
Parsea el JSON string al diccionario Python y lo pasa por `sanitize_event` para obtener solo los campos necesarios.

```python
        message_key = payload["wiki"].encode("utf-8")
        message_val = json.dumps(payload).encode("utf-8")
```
**Serialización para Kafka.** Kafka trabaja con bytes (arrays de bytes), no con strings ni diccionarios Python. Se convierte:
- La **clave** del mensaje: el nombre del wiki (ej: `b"enwiki"`)
- El **valor** del mensaje: el payload completo serializado a JSON y luego a bytes

**¿Por qué usar `wiki` como clave?**
Kafka garantiza que todos los mensajes con la **misma clave** van a la **misma partición** y se procesan en orden. Al usar `wiki` como clave, todas las ediciones de `enwiki` van a la misma partición, preservando el orden cronológico por idioma.

```python
        producer.produce(
            topic=TOPIC_NAME,
            key=message_key,
            value=message_val,
            callback=delivery_callback,
        )
```
Encola el mensaje en el buffer interno del productor. NO lo envía inmediatamente. `librdkafka` decide cuándo enviarlo basándose en `linger.ms` y el tamaño del batch.

```python
        producer.poll(0)
```
Procesa los callbacks de entrega pendientes **sin bloquear** (timeout=0). Si hay mensajes cuya confirmación llegó del broker, ejecuta sus callbacks. Si no llama a `poll()` periódicamente, la cola de callbacks podría llenarse.

---

## Manejo de Cierre

```python
except KeyboardInterrupt:
    print("\n⚡ Interrupción por el usuario. Vaciando cola de Kafka...")
finally:
    producer.flush()
    print("✅ Productor desconectado de forma segura.")
```
- `KeyboardInterrupt`: Captura Ctrl+C del usuario para un cierre limpio.
- `producer.flush()`: **CRÍTICO.** Espera a que todos los mensajes encolados (pero no enviados aún) sean enviados y confirmados por el broker antes de cerrar. Sin `flush()`, al terminar el proceso podrían perderse los últimos mensajes que estaban en el buffer de `linger.ms`.

---

## Flujo Resumido

```
URL SSE Wikimedia
       |
       v (HTTP GET con stream=True)
   iter_lines()  <-- loop infinito
       |
       v (cada línea que empieza con "data: ")
   json.loads()  → evento crudo (30+ campos)
       |
       v
   sanitize_event()  → payload limpio (9 campos)
       |
       v
   encode a bytes (key=wiki, value=JSON)
       |
       v
   producer.produce()  → buffer interno (linger.ms=20)
       |
       v (cada 20ms o cuando el buffer llena)
   Kafka Broker  →  Tópico wiki.changes (partición según clave)
```
