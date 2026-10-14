# MinIO y Object Storage: El Reemplazo de HDFS

## Que es Object Storage

Object Storage es un paradigma de almacenamiento que organiza datos como
**objetos** dentro de **buckets** (contenedores planos). A diferencia de un
filesystem jerarquico (directorios y archivos), Object Storage usa una
estructura plana donde cada objeto tiene:

- **Key:** Ruta completa del objeto (ej: `warehouse/bronze/data/file.parquet`)
- **Value:** El contenido binario del archivo
- **Metadata:** Atributos del objeto (tamano, tipo MIME, checksums)

## Object Storage vs Filesystem Tradicional

| Aspecto | Filesystem (HDFS/ext4) | Object Storage (S3/MinIO) |
|---|---|---|
| Estructura | Jerarquica (directorios) | Plana (key-value en buckets) |
| Escalabilidad | Limitada por nodo maestro | Ilimitada (distribuido) |
| Costo | Alto (discos SSD locales) | Bajo (discos HDD, erasure coding) |
| Acceso | POSIX (read, write, seek) | HTTP REST (GET, PUT, DELETE) |
| Consistencia | Fuerte | Eventual (S3) / Fuerte (MinIO) |
| Rendimiento por archivo | Alto | Medio (latencia de red) |
| Rendimiento en lotes | Medio | Alto (paralelismo masivo) |

## Que es MinIO

MinIO es un servidor de Object Storage de alto rendimiento, 100% compatible
con la API de Amazon S3. Esto permite usar exactamente el mismo codigo y
las mismas herramientas que se usarian con AWS S3, pero ejecutando todo
localmente.

### Compatibilidad S3

Cualquier SDK, herramienta o framework que soporte S3 funciona con MinIO
sin modificacion:
- AWS CLI (`aws s3 ls s3://warehouse/`)
- PySpark (`spark.read.parquet("s3a://warehouse/data/")`)
- Trino, Presto, DuckDB, etc.

## Buckets en Nuestro Laboratorio

| Bucket | Contenido |
|---|---|
| `warehouse` | Archivos Parquet de las tablas Iceberg + metadata JSON/Avro |
| `checkpoints` | Estado del Structured Streaming (offsets procesados) |

## Acceso Programatico: Protocolo S3A

PySpark usa el protocolo **S3A** (implementacion de Hadoop para S3) para
leer y escribir archivos en MinIO:

```python
# Configuracion en SparkSession
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
.config("spark.hadoop.fs.s3a.access.key", "admin")
.config("spark.hadoop.fs.s3a.secret.key", "password")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

La opcion `path.style.access=true` es critica para MinIO. Sin ella,
el SDK intentaria resolver `bucket.minio:9000` como hostname virtual,
lo cual no funciona en redes Docker locales.

## Consola Web

MinIO incluye una interfaz web moderna en el puerto 9001 que permite:
- Navegar buckets y objetos visualmente
- Subir y descargar archivos
- Ver estadisticas de almacenamiento
- Gestionar politicas de acceso

**URL:** http://localhost:9001
**Credenciales:** admin / password

## Por que Object Storage en lugar de HDFS

HDFS (Hadoop Distributed File System) fue el estandar de almacenamiento
para Data Lakes durante una decada. Object Storage lo ha reemplazado
por las siguientes razones:

1. **Costo:** S3/MinIO usa erasure coding en discos economicos. HDFS
   replica bloques 3x en discos costosos.

2. **Elasticidad:** El almacenamiento se escala independientemente del
   computo. Con HDFS, agregar almacenamiento requiere agregar nodos con
   CPU y RAM.

3. **Durabilidad:** S3 ofrece 99.999999999% de durabilidad. HDFS depende
   de la salud de los DataNodes.

4. **Operaciones:** MinIO/S3 es un servicio gestionado o un binario unico.
   HDFS requiere NameNode, DataNodes, JournalNode, etc.
