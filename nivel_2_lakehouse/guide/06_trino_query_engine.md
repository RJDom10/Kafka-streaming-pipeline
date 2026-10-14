# Trino: Motor SQL Federado sobre Lakehouse

## Que es Trino

Trino (anteriormente PrestoSQL, originado en Facebook) es un motor de
consultas SQL distribuido de alta velocidad. No almacena datos: los lee
directamente de las fuentes conectadas a traves de **conectores**.

## Arquitectura de Trino

```
┌─────────────────────────────────────────────┐
│              TRINO CLUSTER                   │
│                                             │
│   ┌──────────────┐                          │
│   │ COORDINATOR   │  (Planifica queries)    │
│   └──────┬───────┘                          │
│          |                                  │
│   ┌──────v───────┐  ┌──────────────┐        │
│   │   WORKER 1   │  │   WORKER 2   │        │
│   └──────┬───────┘  └──────┬───────┘        │
│          |                 |                │
└──────────┼─────────────────┼────────────────┘
           |                 |
    ┌──────v──┐       ┌──────v──┐
    │ Iceberg │       │ Postgres│  (u otro conector)
    │ (MinIO) │       │         │
    └─────────┘       └─────────┘
```

### Coordinator

Recibe las queries SQL, las parsea, optimiza y genera un plan de ejecucion
distribuido. Asigna tareas a los workers y agrega los resultados parciales.

### Workers

Ejecutan las tareas asignadas: leen datos de las fuentes, aplican filtros,
proyecciones, joins y agregaciones. Retornan resultados parciales al
coordinator.

### En Nuestro Laboratorio

Usamos una instancia unica de Trino (coordinator + worker en el mismo
contenedor). En produccion, se despliegan decenas de workers para
procesar queries sobre petabytes de datos.

## Conector Iceberg

El conector Iceberg (`iceberg.properties`) permite a Trino:

- Descubrir tablas a traves del catalogo REST
- Leer archivos Parquet directamente de MinIO
- Usar las estadisticas de Iceberg para **pruning** (solo leer archivos relevantes)
- Ejecutar Time Travel queries
- Ejecutar operaciones de mantenimiento (compaction, expire snapshots)

## Queries de Ejemplo

### Explorar el Catalogo

```sql
-- Listar namespaces (esquemas)
SHOW SCHEMAS FROM lakehouse;

-- Listar tablas en el namespace silver
SHOW TABLES FROM lakehouse.silver;

-- Ver estructura de una tabla
DESCRIBE lakehouse.silver.wiki_cleaned;
```

### Consultar la Capa Gold

```sql
-- Top 10 wikis por volumen de ediciones
SELECT wiki, SUM(total_edits) AS ediciones
FROM lakehouse.gold.wiki_hourly_metrics
GROUP BY wiki
ORDER BY ediciones DESC
LIMIT 10;
```

### Consultar la Capa Silver (detalle)

```sql
-- Ultimas 20 ediciones humanas en Wikipedia en espanol
SELECT title, user_name, byte_diff, event_timestamp
FROM lakehouse.silver.wiki_cleaned
WHERE wiki = 'eswiki' AND bot = false
ORDER BY event_timestamp DESC
LIMIT 20;
```

### Time Travel

```sql
-- Ver snapshots disponibles
SELECT snapshot_id, committed_at, summary
FROM lakehouse.bronze."wiki_raw_events$snapshots";

-- Consultar un snapshot historico
SELECT COUNT(*) FROM lakehouse.bronze.wiki_raw_events
FOR VERSION AS OF 12345678901234;
```

## Acceso al CLI de Trino

```bash
# Desde el host
docker compose exec trino trino

# Se abre un shell interactivo SQL
trino> SHOW SCHEMAS FROM lakehouse;
trino> SELECT COUNT(*) FROM lakehouse.bronze.wiki_raw_events;
```

## Trino vs Spark SQL

| Aspecto | Trino | Spark SQL |
|---|---|---|
| Tipo | Motor de consultas puro | Motor de procesamiento general |
| Latencia | Segundos (interactivo) | Segundos a minutos (batch) |
| Streaming | No soporta | Structured Streaming |
| ETL pesado | No recomendado | Disenado para ETL |
| Uso ideal | Consultas ad-hoc, BI | Pipelines de transformacion |
| Concurrencia | Alta (muchos usuarios) | Baja (jobs individuales) |

En un Lakehouse de produccion, ambos coexisten: Spark para ETL y
procesamiento pesado, Trino para consultas interactivas y BI.
