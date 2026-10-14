# Guia Tecnica — Fundamentos del Stack Lakehouse

Esta carpeta contiene la teoria profunda de cada tecnologia utilizada en el
Nivel 2 del laboratorio. Cada documento explica los conceptos fundamentales,
la arquitectura interna y las decisiones de diseno detras de cada componente.

## Contenido

| Archivo | Tema |
|---|---|
| [01_pyspark_fundamentos.md](./01_pyspark_fundamentos.md) | Apache Spark y PySpark: modelo de ejecucion distribuida |
| [02_apache_iceberg_fundamentos.md](./02_apache_iceberg_fundamentos.md) | Apache Iceberg: table format abierto con ACID |
| [03_minio_object_storage.md](./03_minio_object_storage.md) | MinIO y Object Storage: el reemplazo de HDFS |
| [04_structured_streaming.md](./04_structured_streaming.md) | Spark Structured Streaming: procesamiento incremental |
| [05_medallion_en_practica.md](./05_medallion_en_practica.md) | Arquitectura Medallion: cuando y por que usarla |
| [06_trino_query_engine.md](./06_trino_query_engine.md) | Trino: motor SQL federado sobre Lakehouse |
| [07_trino_sintaxis_y_guia_completa.md](./07_trino_sintaxis_y_guia_completa.md) | Manual integral de Trino: sintaxis SQL ANSI, Iceberg y analitica |

## Orden de Lectura Recomendado

1. `01_pyspark_fundamentos.md` — Entender el motor antes de los patrones
2. `04_structured_streaming.md` — La modalidad streaming de Spark
3. `03_minio_object_storage.md` — Donde se almacenan los datos
4. `02_apache_iceberg_fundamentos.md` — Como se organizan los datos
5. `05_medallion_en_practica.md` — El patron arquitectonico completo
6. `06_trino_query_engine.md` y `07_trino_sintaxis_y_guia_completa.md` — Teoria, sintaxis y uso de Trino
