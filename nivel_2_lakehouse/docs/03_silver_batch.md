# Documentacion: `spark/silver_batch.py`

## Proposito del Script

Este script implementa la **Capa Silver** de la Arquitectura Medallion.
Lee la tabla Bronze (eventos crudos), aplica tres etapas de transformacion
(parseo, filtrado, deduplicacion) y escribe el resultado limpio a la
tabla Silver en Iceberg.

La capa Silver es donde los datos adquieren **calidad de produccion**:
tipos estrictos, registros validados y sin duplicados.

## Etapas de Transformacion

### Etapa 1: Parseo de JSON a Columnas Tipadas

```python
parsed_df = bronze_df.select(
    from_json(col("kafka_value"), WIKI_EVENT_SCHEMA).alias("data"),
    ...
)
```

La funcion `from_json()` interpreta la cadena JSON almacenada en `kafka_value`
usando el esquema `WIKI_EVENT_SCHEMA` definido explicitamente. Esto convierte
un string como:

```json
{"wiki":"enwiki","title":"Main Page","user_name":"Bot123","bot":true,...}
```

En un `StructType` con columnas individuales tipadas que luego se extraen con
`col("data.wiki")`, `col("data.title")`, etc.

### Etapa 2: Filtrado de Registros Malformados

```python
cleaned_df = parsed_df.filter(
    col("wiki").isNotNull()
    & col("title").isNotNull()
    & col("user_name").isNotNull()
    & col("event_timestamp").isNotNull()
)
```

Se eliminan registros donde el parseo de JSON produjo valores nulos en
columnas criticas. Esto puede ocurrir si el productor envio un evento
con formato inesperado o si el JSON estaba truncado.

### Etapa 3: Deduplicacion por Clave Natural

```python
window_spec = Window.partitionBy(
    "wiki", "title", "user_name", "event_timestamp"
).orderBy(col("kafka_offset").desc())

deduplicated_df = (
    cleaned_df
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)
```

Si el streaming Bronze fue re-ejecutado o si el productor envio duplicados,
multiples registros pueden tener la misma clave natural
`(wiki, title, user_name, event_timestamp)`.

La funcion de ventana `row_number()` asigna un numero secuencial a cada
registro dentro de su grupo, ordenado por `kafka_offset` descendente (el mas
reciente primero). Se conserva solo el registro con `row_num == 1`.

## Esquema de la Tabla Silver

| Columna | Tipo | Descripcion |
|---|---|---|
| `wiki` | STRING | Codigo del wiki (ej: `enwiki`, `eswiki`, `dewiki`) |
| `title` | STRING | Titulo del articulo editado |
| `user_name` | STRING | Nombre del editor |
| `bot` | BOOLEAN | `true` si la edicion fue hecha por un bot automatico |
| `change_type` | STRING | Tipo de cambio: `edit`, `new`, `log`, `categorize` |
| `length_old` | INT | Longitud del articulo antes de la edicion (bytes) |
| `length_new` | INT | Longitud del articulo despues de la edicion (bytes) |
| `byte_diff` | INT | Diferencia en bytes (`length_new - length_old`) |
| `event_timestamp` | TIMESTAMP | Timestamp UTC del evento en Wikimedia |
| `kafka_partition` | INT | Particion Kafka de origen (para trazabilidad) |
| `kafka_offset` | BIGINT | Offset Kafka de origen (para trazabilidad) |
| `processed_at` | TIMESTAMP | Timestamp de cuando se proceso el registro |

## Conversion de Timestamps

```python
from_unixtime(col("data.event_timestamp")).cast("timestamp")
```

El productor envia `event_timestamp` como un entero Unix epoch en segundos
(ej: `1695000000`). La funcion `from_unixtime()` lo convierte a un string
de timestamp legible, y `.cast("timestamp")` lo convierte al tipo nativo
de Spark `TimestampType`.

## Modo de Escritura

```python
deduplicated_df.writeTo(SILVER_TABLE).append()
```

Se usa modo **append** para agregar registros nuevos sin modificar los
existentes. En un escenario de produccion, se usaria `MERGE INTO` para
implementar upserts verdaderos, pero para este laboratorio append es
suficiente y mas sencillo de comprender.
