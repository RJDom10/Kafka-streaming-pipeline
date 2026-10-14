# Nivel 2: Streaming Lakehouse con Arquitectura Medallion

## Descripcion General

Este laboratorio extiende el pipeline de streaming del Nivel 1 para implementar
un **Data Lakehouse** completo usando tecnologias de procesamiento distribuido
y almacenamiento columnar abierto.

En lugar de persistir los eventos directamente en PostgreSQL (modelo OLTP del
Nivel 1), aqui los eventos fluyen a traves de tres capas progresivas de
refinamiento de datos (**Arquitectura Medallion**) almacenadas en Apache Iceberg
sobre Object Storage S3-compatible (MinIO).

```
Wikimedia SSE --> Producer --> Kafka --> PySpark Structured Streaming
                                                   |
                                         +---------+---------+
                                         |         |         |
                                      Bronze    Silver     Gold
                                     (Iceberg) (Iceberg) (Iceberg)
                                         |         |         |
                                         +----+----+----+----+
                                              |
                                     MinIO (Object Storage S3)
                                              |
                                    Trino (Motor SQL Distribuido)
```

## Stack Tecnologico

| Componente | Tecnologia | Rol en la Arquitectura |
|---|---|---|
| Message Broker | Apache Kafka (KRaft) | Ingesta de eventos en tiempo real |
| Object Storage | MinIO (S3-compatible) | Almacenamiento de archivos Parquet/Avro |
| Table Format | Apache Iceberg | Tablas ACID con Time Travel y Schema Evolution |
| Catalog | Iceberg REST Catalog | Gestion centralizada de metadatos de tablas |
| Compute Engine | Apache Spark (PySpark) | Procesamiento distribuido (streaming + batch) |
| Query Engine | Trino | Consultas SQL ANSI sobre tablas Iceberg |
| Producer | Python (confluent-kafka) | Ingestor de eventos de Wikimedia |

## Capas de la Arquitectura Medallion

### Bronze (Datos Crudos)
- **Ingesta:** PySpark Structured Streaming consume de Kafka en micro-batches
- **Formato:** JSON crudo almacenado como STRING + metadatos de Kafka
- **Garantia:** Exactly-once via checkpointing en MinIO
- **Script:** `spark/bronze_streaming.py`

### Silver (Datos Limpios)
- **Transformacion:** Parseo de JSON a columnas tipadas, filtrado, deduplicacion
- **Formato:** Columnas con tipos estrictos (STRING, BOOLEAN, INT, TIMESTAMP)
- **Particionamiento:** Por dia de `event_timestamp` (hidden partitioning de Iceberg)
- **Script:** `spark/silver_batch.py`

### Gold (Metricas Agregadas)
- **Agregacion:** Metricas por hora y por wiki (ediciones, bytes, editores unicos)
- **Formato:** Tabla dimensional lista para consulta directa
- **Actualizacion:** Overwrite por particion (idempotente)
- **Script:** `spark/gold_aggregations.py`

## Inicio Rapido

### 1. Levantar toda la infraestructura

```bash
cd nivel_2_lakehouse
docker compose up -d --build
```

### 2. Verificar que todos los servicios esten activos

```bash
docker compose ps
```

Servicios esperados: `kafka`, `minio`, `iceberg-rest`, `spark-iceberg`, `trino`, `producer`.

### 3. Ejecutar la ingesta Bronze (streaming continuo)

```bash
docker compose exec spark-iceberg spark-submit /home/iceberg/scripts/bronze_streaming.py
```

Dejar corriendo 2-3 minutos para acumular datos. Detener con `Ctrl+C`.

### 4. Ejecutar la transformacion Silver (batch)

```bash
docker compose exec spark-iceberg spark-submit /home/iceberg/scripts/silver_batch.py
```

### 5. Ejecutar la agregacion Gold (batch)

```bash
docker compose exec spark-iceberg spark-submit /home/iceberg/scripts/gold_aggregations.py
```

### 6. Consultar datos con Trino

```bash
docker compose exec trino trino
```

```sql
-- Ver los wikis mas activos en la capa Gold
SELECT wiki, total_edits, human_edits, bot_edits, unique_editors
FROM lakehouse.gold.wiki_hourly_metrics
ORDER BY total_edits DESC
LIMIT 10;
```

## Interfaces Web

| Servicio | URL | Credenciales |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / password |
| Spark UI (cuando un job esta corriendo) | http://localhost:4040 | -- |
| Jupyter Notebook | http://localhost:8888 | -- |
| Trino Web UI | http://localhost:8082 | -- |

## Estructura del Proyecto

```
nivel_2_lakehouse/
    README.md                                 <- Este archivo
    docker-compose.yml                        <- Orquestacion de 8 servicios
    Dockerfile.producer                       <- Imagen del productor Python
    Dockerfile.spark                          <- Spark + Iceberg + Kafka connector
    producer.py                               <- Productor de eventos Wikimedia
    requirements.txt                          <- Dependencias Python del productor
    spark/
        bronze_streaming.py                   <- Kafka -> Bronze (streaming)
        silver_batch.py                       <- Bronze -> Silver (batch)
        gold_aggregations.py                  <- Silver -> Gold (batch)
    conf/
        trino/catalog/
            iceberg.properties                <- Conector Trino-Iceberg
    docs/                                     <- Documentacion tecnica linea por linea
    guide/                                    <- Teoria profunda de cada tecnologia
    practica_2/                               <- Laboratorio guiado con retos
    soluciones/                               <- Soluciones oficiales de los retos
```

## Requisitos del Sistema

- Docker Engine 24+ con Docker Compose v2
- Minimo 8 GB de RAM disponible para Docker
- 10 GB de espacio en disco
- Conexion a Internet (para el stream de Wikimedia y descarga de imagenes)
