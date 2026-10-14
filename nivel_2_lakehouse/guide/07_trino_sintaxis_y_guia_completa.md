# Manual Integral de Trino: Arquitectura, Sintaxis SQL ANSI y Analitica sobre Apache Iceberg

## 1. Que es Trino y por que se Utiliza en el Data Lakehouse

### 1.1 Definicion Formal

Trino (anteriormente conocido como PrestoSQL) es un motor de procesamiento distribuido masivamente paralelo (MPP, por sus siglas en ingles: *Massively Parallel Processing*) de codigo abierto, disenado para ejecutar consultas analiticas ad-hoc y exploratorias con latencias de sub-segundos a pocos segundos sobre volumenes de datos que van desde gigabytes hasta petabytes.

A diferencia de los sistemas de gestion de bases de datos relacionales tradicionales (RDBMS), Trino **no posee un mecanismo de almacenamiento propio**. Trino opera bajo el paradigma de **desacoplamiento total entre almacenamiento y computo** (*Storage-Compute Decoupling*). Su funcion es actuar como una capa de computo analitico puro que se conecta a fuentes de almacenamiento heterogeneas mediante conectores especializados basados en su interfaz SPI (*Service Provider Interface*).

### 1.2 Contexto Historico y Evolucion

Trino fue concebido originalmente en 2012 dentro de la infraestructura de ingenieria de Facebook bajo el nombre de **Presto**. El problema que buscaba resolver era la lentitud inherente de Apache Hive y Apache Hadoop MapReduce, los cuales requerian escribir resultados intermedios en disco HDFS tras cada etapa de mapeo y reduccion, haciendo inviable el analisis interactivo de datos por parte de analistas y cientificos de datos.

En 2018, los creadores originales de Presto (Martin Traverso, Dain Sundstrom, David Phillips y Eric Hwang) establecieron la *Presto Software Foundation*, rebautizando el proyecto como **Trino** en diciembre de 2020 para preservar la neutralidad del codigo abierto y garantizar un desarrollo comunitario continuo sin ataduras corporativas.

### 1.3 El Rol de Trino en la Arquitectura Lakehouse

En una arquitectura Lakehouse moderna basada en Apache Iceberg y MinIO (S3), Trino cumple un rol indispensable:

1. **Punto de Entrada SQL Universal**: Provee una interfaz SQL ANSI estricta para herramientas de Business Intelligence (PowerBI, Tableau, Superset, Metabase) y clientes SQL (DBeaver, DataGrip, VS Code).
2. **Consultas sin Overhead de Spark**: Mientras que Apache Spark requiere iniciar contextos distribuidos, coordinar ejecutores pesados y gestionar colas YARN o Kubernetes (con tiempos de arranque de 10 a 30 segundos por aplicacion), Trino mantiene un cluster caliente en ejecucion continua capaz de responder queries en milisegundos.
3. **Lectura Directa de Formatos Abiertos**: Lee directamente archivos Apache Parquet desde almacenamiento de objetos utilizando los metadatos transaccionales de Apache Iceberg, aprovechando estadisticas a nivel de archivo para podar datos irrelevantes antes de descargarlos.

### 1.4 Trino vs. Apache Spark: Criterios de Seleccion

| Criterio | Trino | Apache Spark |
| :--- | :--- | :--- |
| **Tipo de Motor** | Motor de consultas analiticas puras (MPP) | Motor de procesamiento general y distribuido |
| **Modelo de Memoria** | Pipeline en memoria sin persistencia intermedia (*pipelined execution*) | Basado en RDDs/DataFrames con capacidad de *spill to disk* |
| **Tolerancia a Fallos** | Si una tarea falla, generalmente la consulta se reinicia (enfocado en velocidad) | Tolerancia a fallos por particion y tarea (enfocado en confiabilidad para jobs largos) |
| **Latencia Tipica** | Sub-segundo a decenas de segundos (interactivo) | Segundos a horas (batch pesado) |
| **Casos de Uso Ideales** | Dashboards BI, consultas ad-hoc, analitica interactiva, federacion de datos | Ingesta streaming continuo, transformaciones complejas (ETL/ELT), Machine Learning |
| **Soporte Streaming** | No aplica (solo lectura de tablas estables) | Nativo mediante Spark Structured Streaming |

---

## 2. Arquitectura Interna de Ejecucion de Trino

La arquitectura de Trino sigue un patron maestro-trabajador (*Coordinator-Worker*) altamente optimizado:

```
                          +-------------------------+
                          |   Cliente SQL / BI      |
                          | (Trino CLI, DBeaver)    |
                          +------------+------------+
                                       | HTTP / JDBC (Puerto 8082 / 8080)
                                       v
                          +-------------------------+
                          |    TRINO COORDINATOR    |
                          |  - Parser & Analyzer    |
                          |  - Cost-Based Optimizer |
                          |  - Execution Scheduler  |
                          +------------+------------+
                                       | Planes de ejecucion (Splits)
             +-------------------------+-------------------------+
             |                                                   |
             v                                                   v
+-------------------------+                         +-------------------------+
|      TRINO WORKER 1     |                         |      TRINO WORKER 2     |
|  - Tasks & Pipelines    |                         |  - Tasks & Pipelines    |
|  - Data Source Scanners | <=====================> |  - Data Source Scanners |
|  - Operators in Memory  |    Intercambio de datos |  - Operators in Memory  |
+------------+------------+         (Pages)         +------------+------------+
             |                                                   |
             +-------------------------+-------------------------+
                                       | Lectura concurrente de Parquet
                                       v
                          +-------------------------+
                          |  Iceberg REST + MinIO   |
                          | (Metadatos + Archivos)  |
                          +-------------------------+
```

### 2.1 El Coordinator
Es el nodo responsable de recibir, analizar, optimizar y planificar las consultas:
* **Parser y Analyzer**: Valida la sintaxis SQL ANSI y comprueba la existencia de tablas, columnas y tipos de datos consultando el catalogo.
* **Cost-Based Optimizer (CBO)**: Emplea estadisticas de tabla (numero de filas, valores nulos, cardinalidad) provistas por Apache Iceberg para reordenar joins y seleccionar los algoritmos de union mas eficientes (ej. *Broadcast Join* vs. *Partitioned Hash Join*).
* **Scheduler**: Divide el plan logico en etapas (*Stages*) y estas en tareas (*Tasks*), generando unidades atomicas de trabajo denominadas **Splits** que reparte entre los Workers disponibles.

### 2.2 Los Workers
Son los nodos que realizan el procesamiento material de los datos:
* Ejecutan operadores altamente vectorizados en memoria (filtros, proyecciones, funciones hash, agregaciones).
* Leen los bloques de datos Parquet directamente del almacenamiento de objetos mediante conexiones HTTP concurrentes.
* Intercambian flujos de datos entre si en forma de **Pages** (bloques de columnas serializadas) sin necesidad de tocar el disco local.

### 2.3 Modo Mononodo (Despliegue del Laboratorio)
En nuestro entorno Docker (`lab2-trino`), el Coordinator y el Worker residen en el mismo contenedor para minimizar el consumo de recursos de hardware en la maquina local, manteniendo exactamente la misma semantica y capacidades SQL que un cluster de produccion de 100 nodos.

---

## 3. Jerarquia de Nomenclatura: Catalogo, Esquema y Tabla

Trino organiza los datos bajo una convencion estandar de tres niveles:

$$	ext{catalogo} \,.\, 	ext{esquema} \,.\, 	ext{tabla}$$

1. **Catalogo (*Catalog*)**: Define la conexion a una fuente de datos concreta mediante un conector. En nuestro entorno tenemos:
   * `lakehouse`: Conector Iceberg apuntando al catalogo REST de MinIO (`conf/trino/catalog/lakehouse.properties`).
   * `iceberg`: Alias identico que apunta al mismo catalogo REST (`conf/trino/catalog/iceberg.properties`).
   * `system`: Catalogo interno de Trino para monitorear nodos, tareas y configuraciones.
2. **Esquema (*Schema* o *Namespace*)**: Agrupacion logica de tablas. Corresponde a las capas de la Arquitectura Medallion:
   * `bronze`: Tablas con eventos crudos ingestados desde Kafka.
   * `silver`: Tablas limpias, validadas y deduplicadas.
   * `gold`: Tablas con metricas precalculadas y agrupadas.
3. **Tabla (*Table*)**: Estructura de datos columnar con esquema formal respaldada por archivos Parquet gestionados por Iceberg.

---

## 4. Sintaxis DDL (Data Definition Language) sobre Iceberg

### 4.1 Exploracion del Entorno

```sql
-- Listar todos los catalogos configurados
SHOW CATALOGS;

-- Listar esquemas (namespaces) dentro del lakehouse
SHOW SCHEMAS FROM lakehouse;

-- Listar tablas contenidas en un esquema especifico
SHOW TABLES FROM lakehouse.silver;

-- Describir las columnas, tipos de datos y comentarios de una tabla
DESCRIBE lakehouse.gold.wiki_hourly_metrics;

-- Ver el comando exacto DDL utilizado para crear la tabla
SHOW CREATE TABLE lakehouse.gold.wiki_hourly_metrics;
```

### 4.2 Creacion de Esquemas y Tablas

```sql
-- Crear un nuevo esquema para pruebas de analitica
CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox
WITH (location = 's3://warehouse/sandbox');

-- Crear una tabla gestionada en Iceberg
CREATE TABLE IF NOT EXISTS lakehouse.sandbox.wiki_resumen_diario (
    fecha               DATE,
    wiki                VARCHAR,
    total_cambios       BIGINT,
    bytes_acumulados    BIGINT,
    creado_el           TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['fecha']
);
```

### 4.3 Evolucion de Esquema (Schema Evolution)

Una de las mayores virtudes de Apache Iceberg en Trino es que las alteraciones de esquema son operaciones atomicas de metadatos, sin reescritura de archivos Parquet:

```sql
-- Agregar una columna opcional
ALTER TABLE lakehouse.sandbox.wiki_resumen_diario 
ADD COLUMN ratio_eficiencia DOUBLE;

-- Renombrar una columna existente
ALTER TABLE lakehouse.sandbox.wiki_resumen_diario 
RENAME COLUMN total_cambios TO conteo_ediciones;

-- Eliminar una columna (Iceberg deja de proyectarla sin borrar el archivo fisico)
ALTER TABLE lakehouse.sandbox.wiki_resumen_diario 
DROP COLUMN ratio_eficiencia;
```

### 4.4 Creacion de Vistas Logicas

```sql
-- Crear una vista que expone solo ediciones de la Wikipedia en espanol
CREATE OR REPLACE VIEW lakehouse.gold.vw_ediciones_espanol AS
SELECT 
    event_hour,
    total_edits,
    bot_edits,
    human_edits,
    total_bytes,
    avg_byte_diff,
    unique_editors
FROM lakehouse.gold.wiki_hourly_metrics
WHERE wiki = 'eswiki';
```

---

## 5. Sintaxis DML y Consultas Analiticas Avanzadas

### 5.1 Agregaciones y Filtros Condicionales

Trino incluye funciones avanzadas de agregacion que optimizan los calculos analiticos:

```sql
SELECT 
    wiki,
    COUNT(*) AS total_filas,
    -- Conteo condicional directo sin necesidad de CASE WHEN verboso
    count_if(bot = true) AS ediciones_bot,
    count_if(bot = false) AS ediciones_humanas,
    -- Cardinalidad aproximada ultra-rapida mediante algoritmo HyperLogLog
    approx_distinct(user_name) AS editores_estimados_hll,
    -- Cardinalidad exacta
    COUNT(DISTINCT user_name) AS editores_exactos,
    ROUND(AVG(byte_diff), 2) AS promedio_bytes,
    SUM(byte_diff) AS delta_bytes_total
FROM lakehouse.silver.wiki_cleaned
GROUP BY wiki
HAVING COUNT(*) > 50
ORDER BY total_filas DESC
LIMIT 10;
```

### 5.2 Funciones de Ventana (Window Functions)

Las funciones de ventana permiten realizar calculos analiticos sobre conjuntos de filas relacionados sin colapsar el resultado:

```sql
-- Identificar a los 3 editores con mayor volumen de cambios por cada wiki
WITH ranking_editores AS (
    SELECT 
        wiki,
        user_name,
        COUNT(*) AS total_ediciones,
        SUM(byte_diff) AS bytes_aportados,
        -- Asigna un ranking ordenado dentro de la particion de cada wiki
        DENSE_RANK() OVER (
            PARTITION BY wiki 
            ORDER BY COUNT(*) DESC
        ) AS posicion_ranking
    FROM lakehouse.silver.wiki_cleaned
    WHERE bot = false
    GROUP BY wiki, user_name
)
SELECT 
    wiki,
    posicion_ranking,
    user_name,
    total_ediciones,
    bytes_aportados
FROM ranking_editores
WHERE posicion_ranking <= 3
ORDER BY wiki ASC, posicion_ranking ASC;
```

### 5.3 Funciones de Manipulacion Temporal

```sql
SELECT 
    -- Truncar timestamp a granularidad de hora
    date_trunc('hour', event_timestamp) AS ventana_hora,
    -- Extraer componentes de fecha
    EXTRACT(DOW FROM event_timestamp) AS dia_semana, -- 1 = Lunes, 7 = Domingo
    EXTRACT(HOUR FROM event_timestamp) AS hora_dia,
    -- Formatear timestamp a cadena legible
    format_datetime(event_timestamp, 'yyyy-MM-dd HH:mm:ss') AS fecha_formateada,
    COUNT(*) AS volumen
FROM lakehouse.silver.wiki_cleaned
GROUP BY 1, 2, 3, 4
ORDER BY ventana_hora DESC
LIMIT 5;
```

### 5.4 Parseo e Inspeccion de JSON en Trino

Dado que la capa Bronze conserva el payload original como un string JSON, Trino permite consultar campos internos sin deserializar toda la tabla:

```sql
SELECT 
    kafka_partition,
    kafka_offset,
    -- Extrae valores escalares desde cadenas JSON
    json_extract_scalar(kafka_value, '$.wiki') AS wiki_extraida,
    json_extract_scalar(kafka_value, '$.user_name') AS usuario_extraido,
    CAST(json_extract_scalar(kafka_value, '$.byte_diff') AS BIGINT) AS bytes_extraidos
FROM lakehouse.bronze.wiki_raw_events
WHERE json_extract_scalar(kafka_value, '$.bot') = 'false'
LIMIT 10;
```

---

## 6. Funcionalidades Nativas de Apache Iceberg en Trino

### 6.1 Inspeccion de Tablas de Metadatos del Sistema

Apache Iceberg expone tablas virtuales que permiten auditar el estado fisico e historico de los datos:

```sql
-- 1. Ver el historial de transacciones (Snapshots)
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['total-records'] AS total_registros,
    summary['added-data-files'] AS archivos_anadidos
FROM lakehouse.gold."wiki_hourly_metrics$snapshots"
ORDER BY committed_at DESC;

-- 2. Inspeccionar los archivos Parquet fisicos almacenados en MinIO
SELECT 
    file_path,
    file_format,
    record_count,
    file_size_in_bytes,
    column_sizes
FROM lakehouse.gold."wiki_hourly_metrics$files";

-- 3. Ver los archivos de manifiesto activos (Manifests)
SELECT 
    path,
    length,
    partition_spec_id,
    added_snapshot_id,
    added_data_files_count
FROM lakehouse.gold."wiki_hourly_metrics$manifests";

-- 4. Ver el resumen estadistico por particion
SELECT 
    partition,
    record_count,
    file_count
FROM lakehouse.gold."wiki_hourly_metrics$partitions";
```

### 6.2 Consultas de Viaje en el Tiempo (Time Travel)

Permite consultar el estado de la tabla exactamente como existia en un momento anterior o en un snapshot determinado:

```sql
-- Consulta por identificador de snapshot especifico
SELECT COUNT(*) AS filas_en_snapshot
FROM lakehouse.gold.wiki_hourly_metrics
FOR VERSION AS OF 5683920194829104856;

-- Consulta por marca de tiempo relativa
SELECT COUNT(*) AS filas_hace_cinco_minutos
FROM lakehouse.gold.wiki_hourly_metrics
FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '5' MINUTE);
```

### 6.3 Procedimientos de Mantenimiento Transaccional

Iceberg en Trino soporta comandos de mantenimiento para optimizar el almacenamiento:

```sql
-- Compactacion de archivos pequenos (resuelve el problema de small files generado por streaming)
ALTER TABLE lakehouse.bronze.wiki_raw_events 
EXECUTE optimize(file_size_threshold => '128MB');

-- Expirar snapshots historicos antiguos para liberar espacio en MinIO
ALTER TABLE lakehouse.bronze.wiki_raw_events 
EXECUTE expire_snapshots(retention_threshold => '7d');

-- Eliminar archivos huerfanos que no pertenezcan a ningun snapshot valido
ALTER TABLE lakehouse.bronze.wiki_raw_events 
EXECUTE remove_orphan_files(retention_threshold => '7d');
```

---

## 7. Optimizacion y Diagnostico de Consultas con EXPLAIN

### 7.1 EXPLAIN (Plan Logico)
Muestra el arbol de operadores que Trino ejecutara sin procesar los datos:
```sql
EXPLAIN 
SELECT wiki, SUM(total_edits) 
FROM lakehouse.gold.wiki_hourly_metrics 
GROUP BY wiki;
```

### 7.2 EXPLAIN ANALYZE (Perfil de Ejecucion Real)
Ejecuta la consulta y genera un informe detallado con:
* Tiempos de ejecucion por etapa (*Stage*).
* Memoria utilizada por operador.
* Numero de filas descartadas por empuje de predicados (*predicate pushdown*).
* Cantidad de archivos Parquet leidos vs. ignorados (*partition pruning*).

```sql
EXPLAIN ANALYZE 
SELECT wiki, SUM(total_edits) 
FROM lakehouse.gold.wiki_hourly_metrics 
WHERE event_hour >= TIMESTAMP '2026-09-28 00:00:00 UTC'
GROUP BY wiki;
```

---

## 8. Guia de Conexion para Herramientas Externas

### 8.1 Conexion via Trino CLI (Terminal Interactiva)
```bash
docker compose -f nivel_2_lakehouse/docker-compose.yml exec -it trino trino
```

### 8.2 Conexion desde DBeaver o DataGrip (JDBC)
* **Driver**: Trino JDBC Driver (descarga automatica en DBeaver)
* **Host**: `localhost`
* **Port**: `8082`
* **Catalog**: `lakehouse`
* **Schema**: `gold`
* **Username**: `alumno` (o cualquier identificador arbitrario)
* **Password**: *Dejar en blanco*
* **SSL**: Deshabilitado

### 8.3 Conexion desde Python (`trino-python-client`)
```python
from trino.dbapi import connect

conn = connect(
    host="localhost",
    port=8082,
    user="data-scientist",
    catalog="lakehouse",
    schema="gold",
)

cur = conn.cursor()
cur.execute("SELECT wiki, total_edits FROM wiki_hourly_metrics LIMIT 5")
rows = cur.fetchall()

for row in rows:
    print(row)
```
