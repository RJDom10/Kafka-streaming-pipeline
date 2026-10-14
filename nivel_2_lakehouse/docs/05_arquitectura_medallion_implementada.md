# Arquitectura Medallion Implementada

## Vision General

La Arquitectura Medallion (tambien llamada arquitectura multi-hop) organiza
los datos en un Data Lakehouse en tres capas progresivas de refinamiento.
Cada capa agrega valor sobre la anterior: desde datos crudos sin procesar
(Bronze) hasta metricas analiticamente consumibles (Gold).

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LAKEHOUSE                                   │
│                                                                     │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐                  │
│  │  BRONZE   │ ---> │  SILVER   │ ---> │   GOLD   │                  │
│  │  (Raw)    │      │ (Cleaned) │      │ (Metrics)│                  │
│  │           │      │           │      │          │                  │
│  │ JSON crudo│      │ Tipado    │      │ Agregado │                  │
│  │ + metadata│      │ Filtrado  │      │ Por hora │                  │
│  │ Kafka     │      │ Deduplicado      │ Por wiki │                  │
│  └──────────┘      └──────────┘      └──────────┘                  │
│       |                  |                  |                        │
│       +------------------+------------------+                        │
│                          |                                           │
│                   MinIO (S3 Storage)                                 │
│                   Archivos Parquet                                   │
│                                                                     │
│              ┌───────────────────────┐                               │
│              │    Iceberg Catalog    │                               │
│              │  (Metadatos + Schema) │                               │
│              └───────────────────────┘                               │
│                          |                                           │
│              ┌───────────────────────┐                               │
│              │     Trino (SQL)       │                               │
│              │  Consultas directas   │                               │
│              └───────────────────────┘                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Comparativa: Nivel 1 vs Nivel 2

| Dimension | Nivel 1 (OLTP) | Nivel 2 (Lakehouse) |
|---|---|---|
| Almacenamiento | PostgreSQL (filas) | MinIO + Iceberg (columnas Parquet) |
| Formato | Registros en tablas SQL | Archivos Parquet particionados |
| Motor de consumo | Python + psycopg2 | PySpark Structured Streaming |
| Motor de consulta | PostgreSQL SQL | Trino SQL / Spark SQL |
| Escalabilidad | Vertical (mas RAM/CPU) | Horizontal (mas executors/workers) |
| Time Travel | No disponible | Snapshots inmutables de Iceberg |
| Schema Evolution | ALTER TABLE (destructivo) | Evolucion sin reescritura |
| Compresion | Limitada (TOAST) | Snappy/Zstd por columna |
| Particionamiento | Indices manuales | Hidden partitioning automatico |
| Costo de almacenamiento | Alto (storage SSD) | Bajo (object storage) |

## Flujo Completo del Pipeline

### Paso 1: Ingesta (Producer -> Kafka)

El productor Python (`producer.py`) se conecta al stream SSE de Wikimedia
y publica eventos sanitizados al topico `wiki.changes` de Kafka. Cada
evento contiene: `wiki`, `title`, `user_name`, `bot`, `change_type`,
`length_old`, `length_new`, `byte_diff`, `event_timestamp`.

### Paso 2: Bronze (Kafka -> Iceberg via Structured Streaming)

PySpark lee el topico Kafka en micro-batches de 10 segundos. Cada batch
se escribe como archivos Parquet a la tabla `lakehouse.bronze.wiki_raw_events`
en MinIO. El JSON se almacena como STRING sin parsear, junto con los
metadatos de Kafka (partition, offset, timestamp).

### Paso 3: Silver (Bronze -> Iceberg via Batch)

Un job batch lee toda la tabla Bronze, parsea el JSON a columnas tipadas,
filtra registros malformados y deduplica por clave natural. El resultado
se escribe a `lakehouse.silver.wiki_cleaned`.

### Paso 4: Gold (Silver -> Iceberg via Batch)

Un job batch lee la tabla Silver y calcula metricas horarias por wiki:
total de ediciones, proporcion bots/humanos, bytes totales y editores
unicos. El resultado se escribe a `lakehouse.gold.wiki_hourly_metrics`.

### Paso 5: Consulta (Trino SQL)

Trino se conecta al catalogo REST de Iceberg y permite ejecutar queries
SQL ANSI directamente sobre cualquiera de las tres capas, sin necesidad
de mover o copiar datos.

## Garantias de Calidad por Capa

| Garantia | Bronze | Silver | Gold |
|---|---|---|---|
| Datos crudos preservados | Si | No | No |
| Tipos estrictos | No | Si | Si |
| Sin nulos en columnas clave | No | Si | Si |
| Sin duplicados | No | Si | Si |
| Metricas pre-calculadas | No | No | Si |
| Trazabilidad a Kafka | Si | Si (offset) | No |
| Particionamiento temporal | Si | Si | Si |

## Ventajas Tecnicas de Apache Iceberg

### Time Travel

Iceberg mantiene un historial de snapshots inmutables. Es posible consultar
el estado de una tabla en cualquier punto del pasado:

```sql
-- Desde Spark SQL
SELECT * FROM lakehouse.bronze.wiki_raw_events
VERSION AS OF 12345678901234;  -- snapshot-id

-- Desde Trino
SELECT * FROM lakehouse.bronze.wiki_raw_events
FOR VERSION AS OF 12345678901234;
```

### Schema Evolution

Se pueden agregar, renombrar o eliminar columnas sin reescribir los datos
existentes. Iceberg resuelve las diferencias de esquema en tiempo de lectura:

```sql
ALTER TABLE lakehouse.silver.wiki_cleaned
ADD COLUMN comment STRING AFTER change_type;
```

### Hidden Partitioning

A diferencia de Hive, el usuario no necesita incluir la columna de particion
en las queries. Iceberg deduce automaticamente la particion a partir de la
expresion `days(event_timestamp)`.

### Compaction

Los micro-batches del streaming generan muchos archivos pequenos. Iceberg
permite compactarlos sin afectar las consultas en curso:

```sql
CALL lakehouse.system.rewrite_data_files(
    table => 'bronze.wiki_raw_events',
    strategy => 'binpack'
);
```
