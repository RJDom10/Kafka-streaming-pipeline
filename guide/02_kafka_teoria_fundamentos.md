# Kafka: Teoria, Fundamentos y Conceptos Tecnicos Profundos

**Documento:** `02_kafka_teoria_fundamentos.md`
**Objetivo:** Comprender Apache Kafka desde sus fundamentos teoricos hasta los conceptos avanzados que lo hacen la columna vertebral del streaming de datos moderno.

---

## 1. Que es Apache Kafka

Apache Kafka es una **plataforma distribuida de streaming de eventos** (event streaming platform). Fue creado en LinkedIn en 2010 por Jay Kreps, Neha Narkhede y Jun Rao, y donado a la Apache Software Foundation en 2011. Hoy es mantenido principalmente por Confluent.

### Definicion Formal

Kafka es un **log distribuido, particionado, replicado y ordenado de mensajes**. A diferencia de las colas de mensajes tradicionales (como RabbitMQ o ActiveMQ), Kafka:

1. **NO elimina los mensajes** al ser consumidos. Los retiene durante un periodo configurable (por defecto 7 dias).
2. **Permite multiples consumidores independientes** leer el mismo dato sin interferirse.
3. **Garantiza el orden** de los mensajes dentro de cada particion.
4. **Escala horizontalmente** agregando brokers al cluster.

### La Metafora del Commit Log

La mejor forma de entender Kafka es como un **commit log** (como el WAL de PostgreSQL o el binlog de MySQL):
- Los eventos se escriben al final del log (append-only)
- Cada evento tiene un numero de secuencia unico (offset)
- El log es inmutable: no se puede modificar ni eliminar un evento ya escrito
- Cualquiera puede "leer" el log desde cualquier posicion

---

## 2. Arquitectura de Kafka: Componentes Fundamentales

### 2.1 El Broker

Un **broker** es una instancia del servidor Kafka. Es el proceso que:
- Recibe mensajes de los productores
- Los almacena en disco en formato de log segmentado
- Los entrega a los consumidores

En produccion, un cluster Kafka tipicamente tiene 3, 5 o 7 brokers para alta disponibilidad.

```
Cluster Kafka (3 brokers)
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Broker 1    │  │  Broker 2    │  │  Broker 3    │
│  (ID: 1)     │  │  (ID: 2)     │  │  (ID: 3)     │
│  Controller  │  │  Follower    │  │  Follower    │
└──────────────┘  └──────────────┘  └──────────────┘
```

En este proyecto: **1 solo broker** (suficiente para laboratorio).

### 2.2 Topics (Topicos)

Un **topic** es una categoria o canal de mensajes. Es el equivalente a una "tabla" en bases de datos, pero para eventos en tiempo real.

- Los productores escriben mensajes a un topic especifico
- Los consumidores se suscriben a topics para leer mensajes
- Un topic puede tener multiples productores y multiples consumer groups simultaneamente

**En este proyecto:** El topic `wiki.changes` recibe todos los eventos de edicion de Wikimedia.

### 2.3 Particiones — La Unidad de Paralelismo

Cada topic se divide en **N particiones**. Una particion es:
- Un **log ordenado e inmutable** de mensajes
- La unidad de paralelismo en Kafka
- Asignada a exactamente un consumidor dentro de un grupo en un momento dado

```
Topic: wiki.changes (3 particiones)

Particion 0:  [msg#0][msg#1][msg#2][msg#3]...[msg#5200]
              ^offset=0                        ^offset=5200

Particion 1:  [msg#0][msg#1][msg#2][msg#3]...[msg#5350]
              ^offset=0                        ^offset=5350

Particion 2:  [msg#0][msg#1][msg#2][msg#3]...[msg#680]
              ^offset=0                        ^offset=680
```

**Regla de Oro:** El numero de particiones determina el maximo paralelismo posible. Con 3 particiones, el maximo de consumers utiles en un mismo grupo es 3.

### 2.4 El Offset — La Posicion en el Log

El **offset** es un numero entero que identifica de forma unica la posicion de un mensaje dentro de una particion. Es monotonicamente creciente (0, 1, 2, 3...) y nunca se reutiliza.

```
Particion 0:
Offset:  0     1     2     3     4     5   ...
         |     |     |     |     |     |
Msg:   [A]   [B]   [C]   [D]   [E]   [F]

Consumer lee hasta offset=3:
  CURRENT-OFFSET = 3 (D fue el ultimo leido)
  LOG-END-OFFSET = 5 (F es el ultimo en el broker)
  LAG = 5 - 3 = 2 (E y F aun no han sido procesados)
```

El offset es **critico para la tolerancia a fallos**: si un consumer se reinicia, Kafka sabe exactamente desde donde retomar la lectura.

### 2.5 Replicas y Factor de Replicacion

Cada particion puede tener **N replicas** distribuidas entre los brokers. El factor de replicacion determina cuantas copias existen.

```
Particion 0 con ReplicationFactor=3:

Broker 1: [LIDER] Particion 0 Replica 1  (recibe escrituras)
Broker 2: [SEGUIDOR] Particion 0 Replica 2  (sincroniza del lider)
Broker 3: [SEGUIDOR] Particion 0 Replica 3  (sincroniza del lider)
```

- **Lider (Leader):** El unico broker que acepta lecturas y escrituras para esa particion
- **Seguidor (Follower):** Replicas pasivas que sincronizan del lider
- **ISR (In-Sync Replicas):** Conjunto de replicas que estan al dia con el lider

**En este proyecto:** `ReplicationFactor=1` (laboratorio local, 1 broker). En produccion con 3 brokers: `ReplicationFactor=3`.

### 2.6 ZooKeeper vs KRaft

#### ZooKeeper (modo clasico, hasta Kafka 3.3)
ZooKeeper es un servicio de coordinacion distribuida externo que Kafka usaba para:
- Elegir al controller del cluster
- Guardar metadatos de topicos y particiones
- Detectar cuando un broker cae

**Problema:** Tener ZooKeeper como dependencia externa aumenta la complejidad operacional enormemente.

#### KRaft (Kafka Raft Metadata, modo moderno)
Introducido en Kafka 2.8 y estable desde 3.3. Elimina ZooKeeper al:
- Usar el protocolo **Raft** (algoritmo de consenso distribuido) internamente
- Almacenar los metadatos en el propio Kafka (topico interno `__cluster_metadata`)
- Permitir que un proceso Kafka sea simultaneamente broker y controller

**Beneficios de KRaft:**
- 50-100% mas rapido en reconocimiento de fallos
- Soporta hasta 1 millon de particiones (vs 200,000 con ZooKeeper)
- Arquitectura mas simple: un servicio menos que operar

**En este proyecto:** Se usa KRaft (`KAFKA_PROCESS_ROLES: 'broker,controller'`).

---

## 3. Productores: Como Escriben Mensajes

### 3.1 Ciclo de Vida de un Mensaje

```
1. producer.produce(topic, key, value)
          |
          v
2. Serializacion (Python dict -> JSON string -> bytes)
          |
          v
3. Particionamiento (hash(key) % num_particiones)
          |
          v
4. Buffer interno (linger.ms=20: espera 20ms para agrupar)
          |
          v
5. Compresion (snappy comprime el batch)
          |
          v
6. Envio al broker lider de la particion
          |
          v
7. Broker escribe en disco (segmento del log)
          |
          v
8. Confirmacion al producer (acks=1)
          |
          v
9. delivery_callback() invocado
```

### 3.2 Particionamiento por Clave

Cuando un mensaje tiene una **clave** (key), Kafka usa `hash(key) % num_particiones` para determinar la particion destino. Esto garantiza que todos los mensajes con la misma clave van a la misma particion, preservando el orden causal.

```python
# En producer.py:
message_key = payload["wiki"].encode("utf-8")
# hash("enwiki") % 3 = 1  -> Particion 1
# hash("eswiki") % 3 = 0  -> Particion 0
# Todas las ediciones de enwiki van a Particion 1 (ordenadas cronologicamente)
```

### 3.3 Niveles de Durabilidad (acks)

| acks | Comportamiento | Riesgo | Latencia |
|---|---|---|---|
| `0` | No espera confirmacion | Perdida si el broker cae | Minima |
| `1` | Espera confirmacion del lider | Perdida si el lider cae antes de replicar | Baja |
| `all` o `-1` | Espera que todas las ISR confirmen | Ninguna con ISR >= 2 | Alta |

---

## 4. Consumidores: Como Leen Mensajes

### 4.1 Consumer Groups

Un **consumer group** es un conjunto de consumidores que cooperan para procesar un topic. Kafka distribuye las particiones entre los miembros del grupo:

```
Topic wiki.changes (3 particiones)
Consumer Group: wiki-persister-group

Escenario A: 1 consumer (situacion actual)
  Consumer-1 <- Particion 0, 1, 2 (procesa todo solo)

Escenario B: 2 consumers
  Consumer-1 <- Particion 0, 1
  Consumer-2 <- Particion 2

Escenario C: 3 consumers (maximo paralelismo con 3 particiones)
  Consumer-1 <- Particion 0
  Consumer-2 <- Particion 1
  Consumer-3 <- Particion 2

Escenario D: 4 consumers (un consumer queda inactivo)
  Consumer-1 <- Particion 0
  Consumer-2 <- Particion 1
  Consumer-3 <- Particion 2
  Consumer-4 <- (inactivo, sin particion asignada)
```

### 4.2 Rebalanceo

Cuando un consumer entra o sale del grupo, Kafka **rebalancea** las asignaciones de particiones. Durante el rebalanceo, el consumo se pausa brevemente.

Protocolo de rebalanceo:
1. El Group Coordinator (broker) detecta el cambio (heartbeat perdido o nuevo consumer)
2. Envia una solicitud de `JoinGroup` a todos los consumers
3. El lider del grupo calcula la nueva asignacion de particiones
4. Todos los consumers reciben su nueva asignacion via `SyncGroup`

### 4.3 Commit de Offsets

El mecanismo de commits garantiza que un consumer sabe exactamente donde retomar si se reinicia:

```
Particion 1:
Offset:  0  1  2  3  4  5  6  7  8  9  10
                            ^
                     COMMITTED OFFSET = 5
                     (consumer proceso hasta aqui)

Si el consumer se reinicia:
  - Lee el offset comprometido (5) del topico __consumer_offsets
  - Reanuda desde offset 6
  - No reprocesa 0-5, no pierde 6-10
```

---

## 5. Garantias de Entrega

### 5.1 At-Most-Once (Como maximo una vez)
- El mensaje puede perderse pero nunca se reprocesa
- Util cuando la perdida de datos es aceptable
- `acks=0`, sin reintentos

### 5.2 At-Least-Once (Al menos una vez)
- El mensaje nunca se pierde pero puede procesarse mas de una vez
- **Es lo que usa este proyecto** (auto.commit + acks=1)
- El consumidor debe ser idempotente para manejar duplicados

### 5.3 Exactly-Once (Exactamente una vez)
- Garantia maxima: cada mensaje se procesa exactamente una vez
- Requiere transacciones en el producer y offset manual en el consumer
- Mayor overhead de performance

---

## 6. Kafka en Numeros: Capacidades Reales

| Metrica | Valor tipico en produccion |
|---|---|
| Throughput por broker | 1-2 millones de mensajes/segundo |
| Latencia extremo a extremo | 2-5 milisegundos |
| Tamano maximo de mensaje | 1 MB (configurable hasta 10 MB+) |
| Retencion de mensajes | 7 dias (configurable a illimitado) |
| Particiones por cluster | 200,000 (ZK) / 1,000,000 (KRaft) |
| Replication factor recomendado | 3 (para produccion) |
| Factor de compresion (snappy) | 30-60% de reduccion |

---

## 7. Kafka vs. Alternativas

| Caracteristica | Kafka | RabbitMQ | AWS SQS | Redis Streams |
|---|---|---|---|---|
| Retencion de mensajes | Si (dias/semanas) | No (se eliminan al consumir) | 14 dias max | Configurable |
| Multiples consumers independientes | Si (consumer groups) | Limitado | No | Si |
| Throughput | Millisiones msgs/s | Miles msgs/s | Miles msgs/s | Cientos de miles |
| Replay de eventos | Si (cambiar offset) | No | No | Si |
| Ordenamiento | Por particion | Por cola | No garantizado | Por stream |
| Complejidad operacional | Alta | Media | Baja (managed) | Baja |
| Caso de uso ideal | Streaming a escala masiva | Colas de tareas | Microservicios AWS | Cache + eventos simples |

---

## 8. El Tópico Interno `__consumer_offsets`

Este topico especial de Kafka almacena los offsets comprometidos de todos los consumer groups. Es critico para la tolerancia a fallos:

```
__consumer_offsets contiene registros como:
  [wiki-persister-group, wiki.changes, particion=0] -> offset=5200
  [wiki-persister-group, wiki.changes, particion=1] -> offset=5350
  [wiki-persister-group, wiki.changes, particion=2] -> offset=680
```

Por eso existe la configuracion:
```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```
En produccion este topico tendria replication factor 3 para que los offsets no se pierdan si un broker cae.
