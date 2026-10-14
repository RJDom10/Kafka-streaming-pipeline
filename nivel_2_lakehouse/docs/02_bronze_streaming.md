# Documentacion: `spark/bronze_streaming.py`

## Proposito del Script

Este script implementa la **Capa Bronze** de la Arquitectura Medallion usando
PySpark Structured Streaming. Su unica responsabilidad es consumir eventos
del topico Kafka `wiki.changes` y escribirlos **tal cual llegan** (sin
transformacion) a una tabla Iceberg en MinIO.

La capa Bronze actua como un **log inmutable y auditado** de todos los eventos
que ingresaron al sistema, preservando el JSON original del productor junto
con los metadatos de transporte de Kafka.

## Flujo de Datos

```
Kafka (wiki.changes)
    |
    | readStream (format="kafka")
    |
    v
DataFrame Kafka [key, value, topic, partition, offset, timestamp]
    |
    | select() + cast() + alias()
    |
    v
DataFrame Bronze [kafka_key, kafka_value, kafka_topic, kafka_partition,
                   kafka_offset, kafka_timestamp, ingestion_timestamp]
    |
    | writeStream (format="iceberg", outputMode="append")
    |
    v
Tabla Iceberg: lakehouse.bronze.wiki_raw_events
    |
    v
MinIO: s3://warehouse/bronze/wiki_raw_events/data/*.parquet
```

## Configuracion de la SparkSession

### Extensiones Iceberg

```python
.config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
```

Habilita las extensiones SQL de Iceberg en Spark, permitiendo sentencias como
`CREATE TABLE ... USING iceberg`, `CALL ... expire_snapshots`, y otras
operaciones especificas de Iceberg.

### Catalogo REST

```python
.config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
.config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "rest")
.config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", "http://iceberg-rest:8181")
```

Registra un catalogo llamado `lakehouse` que usa el protocolo REST para
comunicarse con el servicio `iceberg-rest`. Todas las operaciones DDL y DML
sobre tablas Iceberg pasan a traves de este catalogo.

### Acceso S3 a MinIO

```python
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
.config("spark.hadoop.fs.s3a.access.key", "admin")
.config("spark.hadoop.fs.s3a.secret.key", "password")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

Configura el filesystem S3A de Hadoop para que apunte a MinIO en lugar de
AWS S3. La opcion `path.style.access=true` es necesaria para MinIO porque
no soporta virtual-hosted-style URLs.

## Esquema de la Tabla Bronze

| Columna | Tipo | Origen | Descripcion |
|---|---|---|---|
| `kafka_key` | STRING | `msg.key` | Clave del mensaje (codigo wiki, ej: `enwiki`) |
| `kafka_value` | STRING | `msg.value` | JSON crudo completo del evento Wikimedia |
| `kafka_topic` | STRING | `msg.topic` | Nombre del topico (`wiki.changes`) |
| `kafka_partition` | INT | `msg.partition` | Particion de Kafka (0, 1 o 2) |
| `kafka_offset` | BIGINT | `msg.offset` | Offset secuencial dentro de la particion |
| `kafka_timestamp` | TIMESTAMP | `msg.timestamp` | Timestamp asignado por el broker |
| `ingestion_timestamp` | TIMESTAMP | `current_timestamp()` | Momento exacto de escritura a Iceberg |

## Particionamiento

```sql
PARTITIONED BY (days(kafka_timestamp))
```

Se usa **Hidden Partitioning** de Iceberg. A diferencia de Hive donde el usuario
debe incluir la columna de particion en las queries, Iceberg resuelve
automaticamente la particion a partir de `kafka_timestamp` sin que el usuario
tenga que especificar `WHERE partition_date = '2024-09-15'`.

## Configuracion del Streaming

### startingOffsets = "earliest"

Al iniciar por primera vez, el streaming lee desde el offset mas antiguo
disponible en Kafka. En ejecuciones posteriores, retoma desde el ultimo
checkpoint guardado.

### failOnDataLoss = "false"

Si Kafka ha eliminado segmentos de log antiguos (por retencion), el streaming
no falla sino que continua desde los offsets disponibles.

### trigger(processingTime="10 seconds")

Cada 10 segundos, Spark ejecuta un micro-batch que:
1. Lee todos los mensajes nuevos desde el ultimo checkpoint
2. Los transforma al esquema Bronze
3. Los escribe como archivos Parquet a MinIO
4. Actualiza el checkpoint en `s3a://checkpoints/bronze/wiki_raw_events`

### Checkpointing

El checkpoint almacena el estado del streaming (offsets procesados por
particion de Kafka). Esto garantiza **exactly-once semantics**: si el
streaming falla y se reinicia, retoma exactamente donde se quedo sin
duplicar ni perder datos.
