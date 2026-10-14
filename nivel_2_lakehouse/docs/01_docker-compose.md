# Documentacion: `docker-compose.yml`

## Proposito del Archivo

Este archivo de orquestacion define los **8 servicios Docker** que conforman
la infraestructura completa del Streaming Lakehouse. A diferencia del Nivel 1
(que usaba 6 servicios con PostgreSQL como destino), el Nivel 2 reemplaza la
capa de almacenamiento relacional por un stack moderno de Data Lakehouse.

## Vista General de Servicios

```
                              docker-compose.yml
    ┌──────────────────────────────────────────────────────────────┐
    │                                                              │
    │   [kafka]  <--  [init-kafka]                                 │
    │      |                                                       │
    │      v                                                       │
    │   [producer]  (Wikimedia SSE -> Kafka)                       │
    │      |                                                       │
    │      v                                                       │
    │   [spark-iceberg]  (Bronze/Silver/Gold PySpark Jobs)         │
    │      |                                                       │
    │      v                                                       │
    │   [iceberg-rest]  (Catalogo de Metadatos)                    │
    │      |                                                       │
    │      v                                                       │
    │   [minio]  <--  [mc-init]  (Object Storage S3)               │
    │      |                                                       │
    │      v                                                       │
    │   [trino]  (Motor de Consultas SQL)                          │
    │                                                              │
    └──────────────────────────────────────────────────────────────┘
```

## Servicio 1: `kafka` -- El Broker de Mensajes

Configuracion identica al Nivel 1. Kafka en modo KRaft (sin ZooKeeper) actua
como la capa de ingesta que recibe los eventos de Wikimedia en tiempo real.

**Diferencia clave con el Nivel 1:** El consumidor ya no es un script Python
que escribe a PostgreSQL, sino PySpark Structured Streaming que escribe a
tablas Iceberg en MinIO.

### Variables de Entorno Relevantes

| Variable | Valor | Explicacion |
|---|---|---|
| `CLUSTER_ID` | `bGFrZWhvdXNlLWxhYjItaWQ` | UUID Base64 unico para este cluster KRaft. Diferente al del Nivel 1 para evitar conflictos si ambos se ejecutan simultaneamente |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Factor de replicacion para entorno de desarrollo con 1 solo broker |
| `KAFKA_JVM_PERFORMANCE_OPTS` | `-Xmx512m -Xms512m` | Limita la JVM a 512 MB para optimizar uso de memoria en el lab |

## Servicio 2: `init-kafka` -- Inicializador de Topicos

Contenedor efimero que crea el topico `wiki.changes` con 3 particiones y
se apaga automaticamente. Usa `condition: service_healthy` para esperar
a que Kafka este completamente operativo antes de ejecutar.

## Servicio 3: `minio` -- Object Storage S3-Compatible

MinIO es un servidor de almacenamiento de objetos de alto rendimiento,
compatible con la API de Amazon S3. En este laboratorio reemplaza a AWS S3,
permitiendo ejecutar todo el stack localmente.

### Puertos Expuestos

| Puerto | Protocolo | Uso |
|---|---|---|
| 9000 | S3 API | Endpoint para Spark, Iceberg y Trino |
| 9001 | HTTP Console | Interfaz web para explorar buckets y objetos |

### Credenciales

- **Usuario:** `admin`
- **Password:** `password`
- **Region:** `us-east-1`

### Healthcheck

MinIO expone un endpoint de salud en `/minio/health/live`. El healthcheck
verifica cada 10 segundos que el servicio responda correctamente.

## Servicio 4: `mc-init` -- Creador de Buckets

Contenedor efimero que usa el cliente MinIO (`mc`) para crear dos buckets:

| Bucket | Proposito |
|---|---|
| `warehouse` | Almacena los archivos de datos de las tablas Iceberg (Parquet) y sus metadatos |
| `checkpoints` | Almacena los puntos de control del Structured Streaming de Spark para garantizar exactly-once |

El bucle `until` reintenta la conexion a MinIO hasta que este disponible,
proporcionando robustez adicional sobre la dependencia `service_healthy`.

## Servicio 5: `iceberg-rest` -- Catalogo REST de Apache Iceberg

El catalogo REST es el componente central que gestiona los **metadatos** de
todas las tablas Iceberg: esquemas, snapshots, particiones, archivos de datos
y archivos de manifiestos.

Tanto Spark como Trino se conectan a este catalogo para:
- Descubrir namespaces y tablas existentes
- Registrar nuevos snapshots al escribir datos
- Resolver rutas de archivos de datos en MinIO
- Ejecutar operaciones de mantenimiento (compaction, expire snapshots)

### Variables de Entorno del Catalogo

| Variable | Valor | Explicacion |
|---|---|---|
| `CATALOG_WAREHOUSE` | `s3://warehouse/` | Ruta base en MinIO donde se almacenan los datos |
| `CATALOG_IO__IMPL` | `org.apache.iceberg.aws.s3.S3FileIO` | Implementacion de I/O para acceso a MinIO via S3 |
| `CATALOG_S3_ENDPOINT` | `http://minio:9000` | Endpoint S3 interno del contenedor MinIO |

## Servicio 6: `spark-iceberg` -- Motor de Procesamiento Distribuido

Imagen personalizada (`Dockerfile.spark`) basada en `tabulario/spark-iceberg`
que agrega los JARs del conector Kafka para Structured Streaming.

### Puertos Expuestos

| Puerto | Uso |
|---|---|
| 8888 | Jupyter Notebook para exploracion interactiva de datos |
| 4040 | Spark UI del driver (visible cuando un job esta ejecutandose) |

### Volumenes

El directorio local `./spark/` se monta en `/home/iceberg/scripts/` dentro del
contenedor. Esto permite editar los scripts PySpark sin reconstruir la imagen.

### Limites de Memoria

Se asignan **2048 MB** al contenedor Spark. Esto es necesario porque Spark ejecuta
tanto el driver como los executors dentro del mismo contenedor en modo local,
y el procesamiento de JSON y Parquet requiere memoria significativa.

## Servicio 7: `trino` -- Motor de Consultas SQL

Trino es un motor de consultas SQL distribuido que puede leer directamente
las tablas Iceberg sin necesidad de cargar los datos en otra base de datos.

### Configuracion del Conector

El archivo `conf/trino/catalog/iceberg.properties` se monta en
`/etc/trino/catalog/` y configura el conector para conectarse al catalogo
REST de Iceberg y acceder a los datos en MinIO.

### Puerto

El puerto interno 8080 de Trino se mapea al **8082** del host para evitar
conflicto con el Spark UI.

## Servicio 8: `producer` -- Productor de Eventos Wikimedia

Identico al productor del Nivel 1. Conecta al stream SSE de Wikimedia y
publica eventos sanitizados al topico `wiki.changes`.

## Red y Volumenes

### Red: `lab2-network`

Todos los servicios comparten la red `lab2-network`, aislada de la red
`lab1-network` del Nivel 1. Esto permite ejecutar ambos laboratorios
simultaneamente sin conflictos de DNS internos.

### Volumen: `minio_data`

Volumen persistente para los datos de MinIO. Si se destruye con
`docker compose down -v`, todas las tablas Iceberg se pierden y es
necesario re-ejecutar el pipeline completo.
