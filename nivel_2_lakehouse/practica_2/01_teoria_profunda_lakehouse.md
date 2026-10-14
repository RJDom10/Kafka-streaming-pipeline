# Teoria Profunda: El Paradigma Lakehouse

Un Data Lakehouse combina la flexibilidad y escalabilidad de los Data Lakes con
la fiabilidad y el rendimiento de los Data Warehouses.

## 1. El Problema Historico

- **Data Warehouses (DWH):** Muy rapidos, ACID, SQL nativo. Pero muy costosos para almacenar petabytes de datos no estructurados o streams.
- **Data Lakes (DL):** Almacenamiento barato (HDFS/S3), soportan cualquier formato. Pero sufren del problema de "Data Swamp" (pantano de datos): mala calidad, sin transacciones, sin esquemas estrictos.

## 2. La Solucion Lakehouse

El Lakehouse es una arquitectura abierta que implementa funciones de DWH directamente sobre el almacenamiento barato del DL. El componente critico que permite esto es el **Table Format**.

## 3. Formatos Abiertos: El Ecosistema

- **File Format (Parquet, ORC, Avro):** Como se codifican los datos en el disco. Parquet (usado en este lab) almacena datos por columnas, lo que permite compresion extrema y lecturas muy rapidas si solo se consultan algunas columnas.
- **Table Format (Iceberg, Delta Lake, Hudi):** Una especificacion sobre los archivos de datos que provee garantias transaccionales (ACID), schema evolution y time travel.
- **Compute Engine (Spark, Flink):** Motores distribuidos que leen/escriben los formatos.
- **Query Engine (Trino, Presto):** Motores SQL que consultan las tablas directamente sin mover datos.

## 4. Por que Apache Iceberg?

Apache Iceberg fue desarrollado en Netflix para superar las limitaciones de Apache Hive.
La clave de Iceberg es que **rastrea archivos, no directorios**.

En Hive, para encontrar los datos de un dia, el motor lista el directorio `date=2024-09-15` en S3. Listar S3 es una operacion lenta (`O(N)`).
En Iceberg, el catalogo contiene un "manifiesto" con la ruta exacta de todos los archivos (`O(1)`). Esto hace que Iceberg sea ordenes de magnitud mas rapido en tablas masivas con millones de archivos.

## 5. El Flujo Medallion Moderno

Este laboratorio implementa el patron estandar de la industria:
1. **Streaming continuo a Bronze:** Ingesta inmediata sin logica de negocio.
2. **Micro-batch/Batch a Silver:** Validacion y limpieza.
3. **Batch a Gold:** Modelado dimensional o metricas agregadas para BI.
