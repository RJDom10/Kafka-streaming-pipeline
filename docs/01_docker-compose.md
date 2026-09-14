# 📄 Documentación: `docker-compose.yml`

**Archivo:** `docker-compose.yml`
**Propósito:** Orquestador de la infraestructura completa del laboratorio. Define, configura y conecta todos los servicios (contenedores) que necesita el sistema para funcionar como una unidad cohesionada.

---

## ¿Qué es Docker Compose?

Docker Compose es una herramienta que permite definir y gestionar aplicaciones multi-contenedor. En lugar de levantar cada servicio manualmente con `docker run`, un archivo `docker-compose.yml` describe **todos** los servicios, sus configuraciones, dependencias y redes en un solo lugar. Con un solo comando (`docker compose up`) se levanta toda la arquitectura.

---

## Estructura General del Archivo

```yaml
services:       # Lista de todos los contenedores/servicios
volumes:        # Almacenamiento persistente para los datos
networks:       # Red interna compartida entre contenedores
```

---

## 🔧 Servicio 1: `kafka` — El Broker de Mensajes

### `image: confluentinc/cp-kafka:7.6.1`
Imagen Docker de Confluent Platform Kafka v7.6.1. Es la distribución empresarial de Kafka que incluye soporte nativo para el modo **KRaft** (sin ZooKeeper). Confluent es la empresa fundada por los creadores originales de Apache Kafka.

### `container_name: lab1-kafka`
Nombre fijo del contenedor. Sin esto Docker generaría un nombre aleatorio como `lab_kafka_test-kafka-1`. Los otros contenedores referencian este servicio por su nombre en la red interna.

### `hostname: kafka`
Nombre DNS dentro de la red Docker. Cuando el producer escribe `KAFKA_BROKER: "kafka:29092"`, está usando este hostname para localizar el broker.

### `ports: "9092:9092"`
Mapeo HOST:CONTENEDOR. El puerto `9092` del contenedor se expone como `9092` en tu máquina. Permite conectarse a Kafka desde herramientas externas como `kafka-console-producer` en tu terminal.

---

## Variables de Entorno del Broker Kafka

### `KAFKA_NODE_ID: 1`
Identificador único del nodo en el clúster. En un clúster de 3 brokers habría IDs 1, 2 y 3.

### `KAFKA_PROCESS_ROLES: 'broker,controller'`
**Esta es la variable que activa el modo KRaft.** En Kafka tradicional:
- El **broker** recibe y entrega mensajes
- El **controller** (ZooKeeper externo) gestiona el estado del clúster

En KRaft, un mismo proceso puede ser ambas cosas simultáneamente, eliminando la dependencia de ZooKeeper.

### `KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'`
Define los nodos que votan en las decisiones del controller. Formato: `ID@HOST:PUERTO`. Con un solo nodo solo hay un voter. Para alta disponibilidad en producción: `'1@kafka1:29093,2@kafka2:29093,3@kafka3:29093'`

### Listeners (la parte más compleja)

```
KAFKA_LISTENERS:          Define DÓNDE Kafka ESCUCHA (sockets activos)
KAFKA_ADVERTISED_LISTENERS: Define QUÉ LE DICE A LOS CLIENTES que usen
```

| Listener | Puerto | Uso |
|---|---|---|
| PLAINTEXT | 29092 | Comunicación interna entre contenedores Docker |
| PLAINTEXT_HOST | 9092 | Conexiones desde tu máquina host |
| CONTROLLER | 29093 | Protocolo interno KRaft |

### `CLUSTER_ID: '4L622nShTUiBenA1g20Tsw'`
**🔑 UUID de 22 caracteres en Base64.** Identifica de forma única este clúster Kafka. Necesario en KRaft para que el estado persista entre reinicios. Se genera una sola vez con: `kafka-storage random-uuid`

### `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`
**🔑 VARIABLE CLAVE** — Factor de replicación del tópico interno `__consumer_offsets`. Este tópico guarda hasta dónde llegó cada consumer group. Con `1` solo hay una copia. En producción con 3 brokers se usa `3`.

### `KAFKA_JVM_PERFORMANCE_OPTS: "-Xmx512m -Xms512m"`
Limita la JVM de Java (que ejecuta Kafka) a 512 MB de heap máximo y mínimo. Sin esto Kafka puede consumir 4-8 GB de RAM.

---

## 🛠️ Servicio 2: `init-kafka` — Inicializador de Tópicos

```yaml
depends_on:
  kafka:
    condition: service_healthy
```
Solo arranca cuando Kafka pase su healthcheck. Garantiza que el broker esté listo para recibir comandos.

```
kafka-topics --bootstrap-server kafka:29092
  --create
  --if-not-exists
  --topic wiki.changes
  --partitions 3            # 🔑 NÚMERO DE PARTICIONES
  --replication-factor 1    # 🔑 FACTOR DE REPLICACIÓN
```

### ¿Dónde cambiar particiones y replicación?
**EXACTAMENTE aquí.** Estas son las dos variables más importantes de Kafka:

| Parámetro | Valor Actual | Para cambiar | Impacto |
|---|---|---|---|
| `--partitions` | `3` | Edita el número en el comando | Más particiones = más paralelismo de consumo |
| `--replication-factor` | `1` | Edita el número en el comando | Más réplicas = mayor tolerancia a fallos |

> ⚠️ El `--replication-factor` no puede ser mayor al número de brokers disponibles. Con 1 broker, máximo `1`.

Este contenedor se **apaga automáticamente** al terminar el comando. Los servicios que dependen de él usan `condition: service_completed_successfully`.

---

## 🗄️ Servicio 3: `postgres` — Base de Datos Relacional

### `image: postgres:16-alpine`
PostgreSQL 16 en Alpine Linux. La variante `-alpine` ocupa ~80 MB en vez de ~400 MB de la imagen estándar.

### Variables de entorno
```yaml
POSTGRES_DB: wikidb         # Crea esta base de datos al arrancar
POSTGRES_USER: wikiuser     # Crea este usuario
POSTGRES_PASSWORD: wikipassword  # Con esta contraseña
```

### Volúmenes
```yaml
- postgres_data:/var/lib/postgresql/data    # Persistencia: datos sobreviven reinicios
- ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro  # Ejecuta init.sql al primer arranque
```
PostgreSQL ejecuta automáticamente todos los `.sql` en `/docker-entrypoint-initdb.d/` cuando la base de datos se inicializa por primera vez.

---

## 👁️ Servicio 4: `kafka-ui`

Interfaz web visual para inspeccionar Kafka. Accesible en `http://localhost:8080`. Permite ver tópicos, particiones, mensajes, consumer groups y offsets sin necesidad de usar la línea de comandos.

---

## 🐍 Servicios 5 y 6: `producer` y `consumer`

```yaml
build:
  context: .
  dockerfile: Dockerfile
```
En lugar de usar una imagen pre-existente, Docker **construye** la imagen localmente usando el `Dockerfile` del proyecto. El `context: .` indica que el directorio de trabajo para el build es el directorio actual.

```yaml
restart: on-failure
```
Si el script Python falla (excepción no capturada), Docker reinicia automáticamente el contenedor. Importante porque el producer se conecta a una URL externa que podría fallar.

---

## Resumen: Variables que puedes cambiar

| Variable | Dónde | Valor actual | Descripción |
|---|---|---|---|
| `--partitions 3` | Comando de `init-kafka` | 3 | Paralelismo del tópico |
| `--replication-factor 1` | Comando de `init-kafka` | 1 | Copias por partición |
| `KAFKA_JVM_PERFORMANCE_OPTS` | Entorno de `kafka` | 512m | Memoria JVM de Kafka |
| `CLUSTER_ID` | Entorno de `kafka` | UUID | Identidad del clúster |
| `memory: limits` | `deploy.resources` | 1024M/512M/256M | RAM máxima por servicio |
| `POSTGRES_PASSWORD` | Entorno de `postgres` | wikipassword | Contraseña de la BD |
| `BATCH_SIZE` | `consumer.py` | 50 | Registros por transacción |
