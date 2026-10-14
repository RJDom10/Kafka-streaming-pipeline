# Apache Iceberg: Table Format Abierto con ACID

## Que es Apache Iceberg

Apache Iceberg es un **formato de tabla abierto** para datasets analiticos
de gran escala. No es una base de datos ni un motor de consultas: es una
**especificacion** que define como organizar archivos de datos (Parquet, ORC,
Avro) en un sistema de almacenamiento (S3, HDFS, GCS) con garantias ACID,
schema evolution y time travel.

## El Problema que Resuelve

Antes de Iceberg, los Data Lakes usaban Apache Hive como table format.
Hive tiene limitaciones severas:

| Problema de Hive | Solucion de Iceberg |
|---|---|
| Las particiones deben ser columnas fisicas visibles | Hidden partitioning: la particion se calcula automaticamente |
| No hay snapshots ni rollback | Time Travel con snapshots inmutables |
| Schema changes requieren reescribir datos | Schema Evolution sin reescritura |
| No hay transacciones ACID | Commits atomicos con Optimistic Concurrency |
| Listado de archivos lento en S3 | Tracking explicito de archivos via manifests |
| Muchos archivos pequenos degradan rendimiento | Compaction nativa |

## Arquitectura Interna de Iceberg

```
┌─────────────────────────────────────────────┐
│              ICEBERG TABLE                   │
│                                             │
│  metadata/                                  │
│    v1.metadata.json  (snapshot 1)           │
│    v2.metadata.json  (snapshot 2)  <-- current
│    snap-001.avro     (manifest list)        │
│    snap-002.avro     (manifest list)        │
│    manifest-001.avro (data file tracking)   │
│    manifest-002.avro (data file tracking)   │
│                                             │
│  data/                                      │
│    event_timestamp_day=2024-09-15/          │
│      00001-data.parquet                     │
│      00002-data.parquet                     │
│    event_timestamp_day=2024-09-16/          │
│      00003-data.parquet                     │
│                                             │
└─────────────────────────────────────────────┘
```

### Capa de Metadatos

1. **Metadata File** (`v2.metadata.json`): Contiene el esquema actual de la
   tabla, la especificacion de particiones, propiedades y la referencia al
   snapshot actual.

2. **Snapshot**: Representa el estado completo de la tabla en un punto en
   el tiempo. Cada operacion de escritura crea un nuevo snapshot.

3. **Manifest List** (`snap-002.avro`): Lista todos los manifest files que
   componen un snapshot.

4. **Manifest File** (`manifest-001.avro`): Lista los archivos de datos
   individuales con estadisticas por columna (min, max, null count).

### Capa de Datos

Archivos Parquet (o ORC/Avro) organizados en directorios de particion.
Iceberg conoce la ubicacion exacta de cada archivo, evitando costosos
listados de directorio en S3.

## Hidden Partitioning

En Hive, para particionar por dia se necesita una columna explicita `date`:

```sql
-- Hive: el usuario DEBE filtrar por la columna de particion
SELECT * FROM events WHERE date = '2024-09-15';
```

En Iceberg, la particion se calcula automaticamente:

```sql
-- Iceberg: filtra por la columna real, Iceberg resuelve la particion
SELECT * FROM events WHERE event_timestamp > '2024-09-15';
```

Iceberg aplica `days(event_timestamp)` internamente para determinar que
archivos de datos necesita leer, sin exponer la columna de particion al
usuario.

## Time Travel

Cada escritura crea un snapshot inmutable. Es posible consultar cualquier
snapshot historico:

```sql
-- Por Snapshot ID
SELECT * FROM table VERSION AS OF 12345678901234;

-- Por Timestamp
SELECT * FROM table TIMESTAMP AS OF '2024-09-15 10:00:00';

-- Ver historial de snapshots
SELECT * FROM lakehouse.bronze.wiki_raw_events.snapshots;
```

## Schema Evolution

Iceberg soporta las siguientes operaciones de esquema sin reescribir datos:

| Operacion | Ejemplo |
|---|---|
| Agregar columna | `ALTER TABLE t ADD COLUMN comment STRING` |
| Renombrar columna | `ALTER TABLE t RENAME COLUMN user TO user_name` |
| Eliminar columna | `ALTER TABLE t DROP COLUMN old_field` |
| Reordenar columnas | `ALTER TABLE t ALTER COLUMN x AFTER y` |
| Cambiar tipo (widening) | `ALTER TABLE t ALTER COLUMN id TYPE bigint` |

Los archivos de datos antiguos no se modifican. Iceberg resuelve las
diferencias de esquema en tiempo de lectura.

## Catalogo REST

El catalogo es el servicio que mantiene el estado de los metadatos. En
nuestro laboratorio usamos un catalogo REST (`tabulario/iceberg-rest`)
que expone una API HTTP para:

- Listar namespaces y tablas
- Obtener la ubicacion del metadata file actual
- Registrar nuevos snapshots (commits)
- Gestionar propiedades de tablas
