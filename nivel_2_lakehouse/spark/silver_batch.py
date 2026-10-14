"""
Capa Silver -- Batch Processing: Bronze -> Silver
===================================================
Lee los eventos crudos de la tabla Bronze, parsea el JSON a columnas
tipadas, aplica filtrado de registros malformados, deduplicacion por
clave natural y escribe el resultado limpio a la tabla Silver en Iceberg.

Tabla origen:  lakehouse.bronze.wiki_raw_events
Tabla destino: lakehouse.silver.wiki_cleaned
Modo:          Batch (ejecucion manual o programada)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    row_number,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    LongType,
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
ICEBERG_CATALOG = "lakehouse"
BRONZE_TABLE = f"{ICEBERG_CATALOG}.bronze.wiki_raw_events"
SILVER_TABLE = f"{ICEBERG_CATALOG}.silver.wiki_cleaned"

# Esquema JSON del payload que genera producer.py.
# Debe coincidir exactamente con la estructura del diccionario
# retornado por la funcion sanitize_event() del productor.
WIKI_EVENT_SCHEMA = StructType([
    StructField("wiki", StringType(), True),
    StructField("title", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("change_type", StringType(), True),
    StructField("length_old", IntegerType(), True),
    StructField("length_new", IntegerType(), True),
    StructField("byte_diff", IntegerType(), True),
    StructField("event_timestamp", LongType(), True),
])


def create_spark_session():
    """Crea la sesion Spark con catalogo Iceberg REST y acceso S3 a MinIO."""
    return (
        SparkSession.builder
        .appName("Silver-WikiCleaned-Batch")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", ICEBERG_CATALOG)
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "rest")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.uri",
            "http://iceberg-rest:8181",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint",
            "http://minio:9000",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access",
            "true",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def ensure_namespace(spark):
    """Crea el namespace 'silver' en el catalogo si no existe."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.silver")


def ensure_table(spark):
    """
    Crea la tabla Silver con esquema tipado estricto.
    Particionada por dia del timestamp del evento original.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            wiki            STRING    COMMENT 'Codigo del wiki (ej: enwiki, eswiki)',
            title           STRING    COMMENT 'Titulo del articulo editado',
            user_name       STRING    COMMENT 'Nombre del editor',
            bot             BOOLEAN   COMMENT 'True si la edicion fue hecha por un bot',
            change_type     STRING    COMMENT 'Tipo de cambio (edit, new, log, categorize)',
            length_old      INT       COMMENT 'Longitud del articulo antes de la edicion (bytes)',
            length_new      INT       COMMENT 'Longitud del articulo despues de la edicion (bytes)',
            byte_diff       INT       COMMENT 'Diferencia en bytes (length_new - length_old)',
            event_timestamp TIMESTAMP COMMENT 'Timestamp UTC del evento en Wikimedia',
            kafka_partition INT       COMMENT 'Particion Kafka de origen (trazabilidad)',
            kafka_offset    BIGINT    COMMENT 'Offset Kafka de origen (trazabilidad)',
            processed_at    TIMESTAMP COMMENT 'Timestamp de procesamiento Silver'
        )
        USING iceberg
        PARTITIONED BY (days(event_timestamp))
    """)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("SILVER LAYER -- Batch: Bronze -> Silver")
    print("=" * 70)

    ensure_namespace(spark)
    ensure_table(spark)

    # ----- Leer tabla Bronze completa -----
    bronze_df = spark.read.table(BRONZE_TABLE)
    total_bronze = bronze_df.count()
    print(f"  Registros en Bronze: {total_bronze}")

    if total_bronze == 0:
        print("  No hay registros en Bronze. Finalizando.")
        spark.stop()
        return

    # ----- Parsear JSON crudo a columnas tipadas -----
    # from_json() interpreta kafka_value (string JSON) usando el esquema definido
    parsed_df = bronze_df.select(
        from_json(col("kafka_value"), WIKI_EVENT_SCHEMA).alias("data"),
        col("kafka_partition"),
        col("kafka_offset"),
    ).select(
        col("data.wiki"),
        col("data.title"),
        col("data.user_name"),
        col("data.bot"),
        col("data.change_type"),
        col("data.length_old"),
        col("data.length_new"),
        col("data.byte_diff"),
        # Convertir Unix epoch (segundos) a Timestamp
        from_unixtime(col("data.event_timestamp")).cast("timestamp").alias("event_timestamp"),
        col("kafka_partition"),
        col("kafka_offset"),
        current_timestamp().alias("processed_at"),
    )

    # ----- Filtrar registros nulos o malformados -----
    cleaned_df = parsed_df.filter(
        col("wiki").isNotNull()
        & col("title").isNotNull()
        & col("user_name").isNotNull()
        & col("event_timestamp").isNotNull()
    )
    after_filter = cleaned_df.count()
    print(f"  Registros despues de filtrado: {after_filter}")
    print(f"  Registros descartados (malformados): {total_bronze - after_filter}")

    # ----- Deduplicar por clave natural -----
    # Si un mismo evento aparece mas de una vez (por re-procesamiento del
    # streaming o duplicados del producer), se conserva solo el registro
    # con el mayor kafka_offset (el mas reciente).
    window_spec = Window.partitionBy(
        "wiki", "title", "user_name", "event_timestamp"
    ).orderBy(col("kafka_offset").desc())

    deduplicated_df = (
        cleaned_df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    after_dedup = deduplicated_df.count()
    print(f"  Registros despues de deduplicacion: {after_dedup}")
    print(f"  Duplicados eliminados: {after_filter - after_dedup}")

    # ----- Escribir a tabla Silver -----
    deduplicated_df.writeTo(SILVER_TABLE).append()

    print(f"  Escritura Silver completada: {after_dedup} registros.")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
