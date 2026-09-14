# Documento Final: Flujo de Ejecución, Arquitectura e Importancia de Kafka

**Documento:** `07_flujo_y_arquitectura.md`
**Propósito:** Explicar de forma integral el flujo completo de ejecución del sistema, la razón de ser de cada componente arquitectónico y por qué Apache Kafka es el elemento central e irremplazable de este diseño.

---

## 1. ¿Qué problema resuelve esta arquitectura?

### El problema sin Kafka

Imagina que quieres capturar el stream de Wikimedia y guardarlo en PostgreSQL directamente:

```
[Script Python] ──── HTTP SSE ────> [Wikimedia]
      │
      └──── INSERT directo ────> [PostgreSQL]
```

¿Qué problemas tiene esta aproximación?

1. **Acoplamiento fuerte**: Si PostgreSQL está caído por mantenimiento, el script falla y **pierde todos los eventos** durante ese tiempo.
2. **Sin paralelismo**: Un solo proceso hace todo. Si Wikimedia emite 500 eventos/segundo y PostgreSQL solo puede insertar 50/segundo, el script no puede escalar.
3. **Sin buffer**: Los picos de tráfico (ej: una noticia global hace que miles editen Wikipedia simultáneamente) pueden sobrecargar la BD.
4. **Sin replay**: Si hay un bug en el código de procesamiento, no puedes volver a procesar los eventos pasados.
5. **Sin múltiples consumidores**: No puedes enviar los mismos eventos a PostgreSQL Y a un data warehouse Y a un sistema de alertas simultáneamente.

### La solución: Desacoplamiento con Kafka

```
[Wikimedia] ──SSE──> [Producer] ──> [KAFKA] ──> [Consumer PG] ──> [PostgreSQL]
                                       │
                                       ├──> [Consumer Alertas]
                                       ├──> [Consumer Analytics]
                                       └──> [Consumer ML Pipeline]
```

Kafka actúa como **buffer inteligente y distribuidor de eventos**, desacoplando quien produce datos de quien los consume.

---

## 2. ¿Por qué es necesario Kafka? Importancia y Fundamentos

### Kafka como Log Distribuido de Eventos

Apache Kafka no es una "cola de mensajes" tradicional. Es fundamentalmente un **log distribuido, ordenado e inmutable**. Los mensajes NO se eliminan al ser consumidos (como en RabbitMQ); persisten durante un tiempo configurable (por defecto 7 días).

Esto tiene implicaciones profundas:

| Característica | Kafka | Cola tradicional (ej: RabbitMQ) |
|---|---|---|
| Mensajes al consumir | Permanecen (retenidos N días) | Se eliminan |
| Múltiples consumidores | Cada grupo lee independientemente | Un mensaje = un consumidor |
| Replay de eventos | Sí (cambia el offset) | No |
| Ordenamiento | Garantizado por partición | Limitado |
| Throughput | Millones msgs/seg | Miles msgs/seg |

### Las 4 Garantías Fundamentales de Kafka

**1. Durabilidad**: Los mensajes se escriben en disco en el broker. Con replicación (factor ≥2), sobreviven la caída de brokers.

**2. Ordenamiento**: Dentro de una partición, los mensajes tienen un orden estricto e inmutable dado por el offset (0, 1, 2, 3...). El proyecto usa `wiki` como clave de particionamiento, garantizando que todas las ediciones de `enwiki` estén ordenadas en la misma partición.

**3. Tolerancia a Fallos del Consumidor**: Si el consumer se cae después de procesar 47 registros de un batch de 50, al reiniciarse Kafka sabe exactamente dónde retomar (offset guardado en `__consumer_offsets`). No reprocesa ni pierde mensajes.

**4. Escalabilidad Horizontal**: Para aumentar el throughput, simplemente agregas más brokers y más particiones. El trabajo se distribuye automáticamente.

---

## 3. Anatomía de Kafka en este Proyecto

### El Broker

```
┌──────────────── Kafka Broker (lab1-kafka) ──────────────────┐
│                                                              │
│  Tópico: wiki.changes                                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Partición 0                                         │   │
│  │  [msg offset=0][msg offset=1][msg offset=2]...       │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │  Partición 1                                         │   │
│  │  [msg offset=0][msg offset=1][msg offset=2]...       │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │  Partición 2                                         │   │
│  │  [msg offset=0][msg offset=1][msg offset=2]...       │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### Las 3 Particiones

El tópico `wiki.changes` tiene 3 particiones. El producer asigna cada mensaje a una partición basándose en la **clave del mensaje** (`wiki`):
- `hash("enwiki") % 3 = 1` → Partición 1
- `hash("eswiki") % 3 = 0` → Partición 0
- `hash("wikidata") % 3 = 2` → Partición 2

(Los números son ilustrativos; el hash real puede variar)

### ¿Dónde está Kafka en el proyecto?

| Aspecto | Ubicación en el proyecto |
|---|---|
| **Servidor Kafka** | Servicio `kafka` en `docker-compose.yml` (imagen `confluentinc/cp-kafka:7.6.1`) |
| **Creación del tópico** | Servicio `init-kafka` en `docker-compose.yml` (comando `kafka-topics --create`) |
| **Configuración de particiones** | `--partitions 3` en el comando de `init-kafka` |
| **Configuración de replicación** | `--replication-factor 1` en el comando de `init-kafka` |
| **Límite de memoria de Kafka** | `KAFKA_JVM_PERFORMANCE_OPTS: "-Xmx512m -Xms512m"` en el servicio `kafka` |
| **Cliente productor** | `producer.py` (clase `Producer` de `confluent-kafka`) |
| **Cliente consumidor** | `consumer.py` (clase `Consumer` de `confluent-kafka`) |
| **Interfaz visual** | Servicio `kafka-ui` en `docker-compose.yml` → `http://localhost:8080` |

---

## 4. Flujo Completo de Ejecución

### Fase 0: Construcción de Imágenes

```bash
docker compose up --build
```

Docker lee el `Dockerfile` y construye la imagen `lab_kafka_test-producer` (y consumer):
```
FROM python:3.11-slim       # Descarga imagen base
WORKDIR /app                 # Configura directorio
COPY requirements.txt        # Copia dependencias
RUN pip install              # Instala confluent-kafka, psycopg2, requests
COPY producer.py consumer.py # Copia código
```

### Fase 1: Arranque de Infraestructura Base

Docker Compose respeta las dependencias declaradas con `depends_on`:

```
t=0s   kafka arranca        → KRaft inicializa, escucha en :9092 y :29092
t=5s   kafka healthcheck    → Docker ejecuta kafka-broker-api-versions
t=10s  kafka = HEALTHY      → Se desbloquean los demás servicios
t=10s  postgres arranca     → Ejecuta init.sql: CREATE TABLE, CREATE INDEX
t=10s  init-kafka arranca   → Ejecuta: kafka-topics --create wiki.changes
t=10s  kafka-ui arranca     → Disponible en http://localhost:8080
t=15s  postgres = HEALTHY   → Se desbloquea el consumer
t=15s  init-kafka = DONE    → Se desbloquean producer y consumer
t=15s  producer ARRANCA     ← Primer evento de Wikimedia capturable
t=15s  consumer ARRANCA     ← Primer mensaje de Kafka consumible
```

### Fase 2: Bucle del Producer (en tiempo real, infinito)

```python
# Cada ~100ms llega un evento de Wikimedia:

WIKIMEDIA SSE → "data: {wiki:'enwiki', title:'Python', user:'AliceBot', bot:true, ...}"
                                         │
                                    json.loads()
                                         │
                                    sanitize_event()  ←── Descarta 25+ campos irrelevantes
                                         │
                                    {wiki:'enwiki', user_name:'AliceBot', bot:True, ...}
                                         │
                                    encode a bytes
                                         │
                                    producer.produce(key=b'enwiki', value=b'{...json...}')
                                         │
                              [buffer interno librdkafka]
                                         │ (cada 20ms o cuando llena)
                                    snappy compress
                                         │
                                    → Kafka Broker → Partición 1 (hash de 'enwiki')
                                         │
                              delivery_callback() confirma envío
```

### Fase 3: Asignación de Particiones al Consumer

Al arrancar, el consumer se une al grupo `wiki-persister-group` y Kafka le asigna las 3 particiones (ya que es el único consumidor del grupo):

```
Consumer (wiki-persister-group)
  ├── Asignado: Partición 0 (offset inicial: 0)
  ├── Asignado: Partición 1 (offset inicial: 0)
  └── Asignado: Partición 2 (offset inicial: 0)
```

### Fase 4: Bucle del Consumer (en tiempo real, infinito)

```python
# Loop principal cada 1 segundo:

consumer.poll(1.0)  →  Mensaje de Kafka (msg.value() = bytes)
                               │
                          .decode('utf-8')
                               │
                          json.loads()  →  {'wiki':'enwiki', 'event_timestamp': 1726265400, ...}
                               │
                          datetime.utcfromtimestamp(1726265400)  →  datetime(2026, 9, 13, 20, 10, 0)
                               │
                          batch_records.append(data)
                               │
                    [batch_records tiene 1, 2, 3... registros]
                               │
                    ¿len(batch_records) >= 50?
                         NO → poll siguiente mensaje
                         SÍ → execute_batch(50 INSERTs) → pg_conn.commit()
                               │
                    PostgreSQL: 50 filas en wiki_recent_changes
                               │
                    batch_records.clear()  →  siguiente batch
```

### Fase 5: Persistencia en PostgreSQL

Cada `execute_batch()` ejecuta eficientemente:
```sql
INSERT INTO wiki_recent_changes (wiki, title, user_name, bot, change_type, 
  length_old, length_new, byte_diff, event_timestamp)
VALUES
  ('enwiki', 'Python', 'AliceBot', true, 'edit', 1200, 1350, 150, '2026-09-13 20:10:00+00'),
  ('eswiki', 'Algoritmo', 'Carlos99', false, 'edit', 800, 820, 20, '2026-09-13 20:10:01+00'),
  -- ... 48 registros más ...
  ;
```

---

## 5. Diagrama Completo de la Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           INTERNET                                                   │
│                                                                                     │
│  https://stream.wikimedia.org/v2/stream/recentchange                                │
│  (Server-Sent Events: miles de ediciones/minuto en todos los idiomas)               │
└─────────────────────────────────────┬───────────────────────────────────────────────┘
                                      │ HTTP GET stream=True
                                      │ ~100-500 eventos/segundo
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           RED DOCKER: lab1-network                                  │
│                                                                                     │
│  ┌──────────────────┐    produce()     ┌──────────────────────────────────────────┐ │
│  │  PRODUCER        │ ──────────────> │  KAFKA BROKER (KRaft)                    │ │
│  │  (producer.py)   │  key=wiki        │  Tópico: wiki.changes                    │ │
│  │                  │  val=JSON bytes  │  ┌─────────────────────────────────────┐ │ │
│  │  sanitize_event()│  snappy+linger   │  │  Partición 0: [...msgs ordenados...] │ │ │
│  │  encode to bytes │                  │  │  Partición 1: [...msgs ordenados...] │ │ │
│  └──────────────────┘                  │  │  Partición 2: [...msgs ordenados...] │ │ │
│                                        │  └─────────────────────────────────────┘ │ │
│                                        │  Retención: 7 días                        │ │
│                                        │  Offsets: guardados en __consumer_offsets │ │
│                                        └──────────────────────────────────────────┘ │
│                                                        │ poll(timeout=1.0)           │
│                                                        ▼                             │
│                                        ┌──────────────────────────────────────────┐ │
│                                        │  CONSUMER (consumer.py)                  │ │
│                                        │  Group: wiki-persister-group             │ │
│                                        │                                          │ │
│                                        │  decode → json.loads → datetime          │ │
│                                        │  batch_records (50 msgs)                 │ │
│                                        │  execute_batch() → commit()              │ │
│                                        └─────────────────────┬────────────────────┘ │
│                                                              │ INSERT batch SQL      │
│                                                              ▼                       │
│                                        ┌──────────────────────────────────────────┐ │
│                                        │  POSTGRESQL 16 (wikidb)                  │ │
│                                        │  tabla: wiki_recent_changes              │ │
│                                        │  índices: timestamp, bot, wiki           │ │
│                                        │  volumen persistente: postgres_data      │ │
│                                        └──────────────────────────────────────────┘ │
│                                                                                     │
│  ┌──────────────────┐                  ┌──────────────────────────────────────────┐ │
│  │  INIT-KAFKA      │                  │  KAFKA UI                                │ │
│  │  (se apaga solo) │                  │  http://localhost:8080                   │ │
│  │  Crea tópico     │                  │  (Consola visual de tópicos/mensajes)    │ │
│  │  wiki.changes    │                  └──────────────────────────────────────────┘ │
│  └──────────────────┘                                                               │
└─────────────────────────────────────────────────────────────────────────────────────┘

Puertos expuestos al HOST:
  :9092  → Kafka (para herramientas externas)
  :5432  → PostgreSQL (para pgAdmin, DBeaver, etc.)
  :8080  → Kafka UI (para inspección visual)
```

---

## 6. Importancia de la Arquitectura Construida

### 6.1 Principio de Responsabilidad Única (SRP)

Cada componente hace **una sola cosa bien**:
- `producer.py`: Solo captura y publica eventos (no sabe nada de PostgreSQL)
- `consumer.py`: Solo lee y persiste eventos (no sabe nada de Wikimedia)
- Kafka: Solo transporta y guarda mensajes (no sabe qué significan)
- PostgreSQL: Solo almacena datos relacionales (no sabe de dónde vienen)

### 6.2 Desacoplamiento Temporal

Producer y consumer no necesitan estar corriendo al mismo tiempo. Si el consumer se cae por 10 minutos, Kafka **acumula** los mensajes. Al reiniciarse, el consumer los procesa desde donde quedó sin perder un solo evento.

### 6.3 Tolerancia a Fallos

```
Escenario: PostgreSQL se cae por mantenimiento (5 minutos)
┌─────────────────────────────────────────────────────┐
│ t=0    PostgreSQL cae                                │
│ t=0    Consumer falla al hacer commit                │
│ t=0    Docker reinicia consumer (restart: on-failure)│
│ t=0    Consumer no puede conectar → sys.exit(1)      │
│ t=0    Docker vuelve a reiniciar → loop de reintentos│
│ t=0    Kafka sigue acumulando mensajes SIN PERDER    │
│ t=5min PostgreSQL se recupera                        │
│ t=5min Consumer conecta exitosamente                 │
│ t=5min Consumer lee desde el offset guardado         │
│ t=5min Procesa todos los mensajes acumulados         │
│ RESULTADO: Cero pérdida de datos                     │
└─────────────────────────────────────────────────────┘
```

### 6.4 Escalabilidad

Para multiplicar el throughput de procesamiento:
1. Cambia `--partitions 3` a `--partitions 9` en `init-kafka`
2. Levanta 9 instancias del consumer con `docker compose scale consumer=9`
3. Kafka distribuye automáticamente las 9 particiones entre los 9 consumers

Sin cambiar una sola línea de código de la aplicación.

### 6.5 Trazabilidad Completa

Cada mensaje en Kafka tiene:
- **Offset**: posición exacta en la partición (inmutable)
- **Timestamp**: cuándo llegó al broker
- **Key**: `wiki` (permite saber qué partición)
- **Headers**: metadatos adicionales (no usados aquí pero disponibles)

Esto permite **auditoría completa** y **replay** de cualquier evento histórico.

### 6.6 Extensibilidad

Agregar nuevos consumidores es trivial. Por ejemplo, agregar análisis en tiempo real:

```yaml
# Agregar en docker-compose.yml:
consumer-alerts:
  build: .
  command: python -u consumer_alerts.py
  environment:
    KAFKA_BROKER: "kafka:29092"
    TOPIC_NAME: "wiki.changes"
    GROUP_ID: "wiki-alerts-group"   # ← Grupo DIFERENTE: lee todo desde el principio
```

Este nuevo consumer lee el mismo tópico **de forma completamente independiente** al consumer de PostgreSQL. Kafka entrega una copia a cada grupo de consumidores.

---

## 7. Comparativa: Con Kafka vs. Sin Kafka

| Aspecto | Sin Kafka (directo) | Con Kafka (este proyecto) |
|---|---|---|
| Pérdida de datos si la BD cae | ❌ Sí, irreversible | ✅ No, Kafka acumula |
| Múltiples destinos (PG + DW + ML) | ❌ Requiere cambiar código | ✅ Nuevos consumer groups |
| Replay de eventos históricos | ❌ Imposible | ✅ Cambiar offset |
| Escalar el procesamiento | ❌ Requiere refactoring | ✅ Más consumers + particiones |
| Picos de tráfico | ❌ Sobrecarga la BD | ✅ Kafka absorbe el pico |
| Observabilidad | ❌ Solo logs de la app | ✅ Kafka UI + métricas |
| Independencia de componentes | ❌ Todo acoplado | ✅ Cada parte es independiente |

---

## 8. Secuencia de Comandos para Ejecutar el Lab

```bash
# 1. Levantar toda la arquitectura
docker compose up --build -d

# 2. Verificar que todos los servicios están corriendo
docker compose ps

# 3. Ver logs del producer en tiempo real
docker compose logs -f producer

# 4. Ver logs del consumer en tiempo real
docker compose logs -f consumer

# 5. Abrir Kafka UI en el navegador
# http://localhost:8080

# 6. Conectarse a PostgreSQL y ver datos
docker exec -it lab1-postgres psql -U wikiuser -d wikidb -c "SELECT COUNT(*), wiki FROM wiki_recent_changes GROUP BY wiki ORDER BY count DESC LIMIT 10;"

# 7. Detener todo (preservando datos de PostgreSQL)
docker compose down

# 8. Detener TODO y eliminar datos (reset completo)
docker compose down -v
```
