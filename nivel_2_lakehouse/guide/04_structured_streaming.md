# Spark Structured Streaming: Procesamiento Incremental

## Que es Structured Streaming

Structured Streaming es el modelo de procesamiento de streams de Apache Spark.
Trata un stream de datos como una **tabla infinita que crece continuamente**.
Cada nuevo dato que llega se agrega como una nueva fila a esta tabla virtual.

## Modelo Mental: Tabla Infinita

```
                    Datos que llegan continuamente
                              |
                              v
    ┌──────────────────────────────────────────────┐
    │          INPUT TABLE (tabla infinita)          │
    │                                                │
    │  t=0   {"wiki":"enwiki", "title":"Cat", ...}  │
    │  t=1   {"wiki":"eswiki", "title":"Gato", ...} │
    │  t=2   {"wiki":"dewiki", "title":"Katze",...}  │
    │  t=3   ...                                     │
    │  t=4   ...                                     │
    │        (crece indefinidamente)                  │
    └──────────────────────────────────────────────┘
                              |
                     Transformaciones
                     (filter, select, groupBy, ...)
                              |
                              v
    ┌──────────────────────────────────────────────┐
    │          RESULT TABLE                          │
    │  (se actualiza en cada micro-batch)            │
    └──────────────────────────────────────────────┘
                              |
                        Output Sink
                    (Iceberg, console, Kafka, ...)
```

## Micro-Batch vs Continuous Processing

Structured Streaming ofrece dos modos de ejecucion:

| Modo | Latencia | Consistencia | Uso |
|---|---|---|---|
| Micro-batch | Segundos | Exactly-once | Produccion general |
| Continuous | Milisegundos | At-least-once | Ultra-baja latencia |

En nuestro laboratorio usamos **micro-batch** con trigger de 10 segundos:

```python
.trigger(processingTime="10 seconds")
```

Cada 10 segundos, Spark:
1. Consulta a Kafka por nuevos mensajes desde el ultimo offset
2. Lee todos los mensajes nuevos como un mini-DataFrame
3. Aplica las transformaciones definidas
4. Escribe el resultado al sink (tabla Iceberg)
5. Actualiza el checkpoint con los nuevos offsets

## Fuentes de Datos (Sources)

Structured Streaming puede leer de multiples fuentes:

| Fuente | Formato | Uso |
|---|---|---|
| Apache Kafka | `"kafka"` | Streams de eventos en tiempo real |
| Archivos | `"parquet"`, `"json"`, `"csv"` | Archivos que llegan a un directorio |
| Socket | `"socket"` | Desarrollo y pruebas |
| Rate | `"rate"` | Generador sintetico para benchmarks |

## Checkpointing y Exactly-Once

El checkpoint es el mecanismo que garantiza **exactly-once semantics**:

```python
.option("checkpointLocation", "s3a://checkpoints/bronze/wiki_raw_events")
```

El checkpoint almacena:
- **Offsets:** Que mensajes de Kafka ya fueron procesados
- **Estado:** Para operaciones con estado (aggregations, joins)
- **Metadata:** Informacion del batch actual

Si la aplicacion falla y se reinicia:
1. Spark lee el ultimo checkpoint
2. Determina el ultimo offset procesado exitosamente
3. Retoma la lectura desde ese punto
4. No procesa mensajes ya escritos (exactly-once)

## Output Modes

| Modo | Comportamiento | Compatible con |
|---|---|---|
| `append` | Solo escribe filas nuevas | Iceberg, Kafka, archivos |
| `complete` | Reescribe la tabla completa | Console, memoria |
| `update` | Escribe solo filas modificadas | Console, memoria, Kafka |

En nuestro laboratorio usamos `append` para Bronze, que es el unico modo
compatible con tablas Iceberg en streaming.

## Watermarking (Concepto Avanzado)

Para operaciones con estado (agregaciones sobre ventanas temporales),
Spark necesita saber cuando puede descartar datos antiguos:

```python
df.withWatermark("event_time", "10 minutes")
  .groupBy(window("event_time", "5 minutes"))
  .count()
```

El watermark de 10 minutos indica que Spark puede descartar datos con
mas de 10 minutos de retraso. No se usa en nuestra capa Bronze (que es
append puro) pero es fundamental para streaming Gold en produccion.
