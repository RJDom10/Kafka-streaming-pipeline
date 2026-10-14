"""
Capa Gold -- Batch Aggregations: Silver -> Gold
=================================================
Calcula metricas agregadas por hora y por wiki a partir de la tabla Silver.
Estas metricas son consumibles directamente por motores de consulta (Trino)
o herramientas de BI sin transformacion adicional.

Tabla origen:  lakehouse.silver.wiki_cleaned
Tabla destino: lakehouse.gold.wiki_hourly_metrics
Modo:          Batch (ejecucion manual o programada)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg as spark_avg,
    when,
    date_trunc,
    current_timestamp,
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
ICEBERG_CATALOG = "lakehouse"
SILVER_TABLE = f"{ICEBERG_CATALOG}.silver.wiki_cleaned"
GOLD_TABLE = f"{ICEBERG_CATALOG}.gold.wiki_hourly_metrics"


def create_spark_session():
    """Crea la sesion Spark con catalogo Iceberg REST y acceso S3 a MinIO."""
    return (
        SparkSession.builder
        .appName("Gold-WikiHourlyMetrics-Batch")
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
    """Crea el namespace 'gold' en el catalogo si no existe."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.gold")


def ensure_table(spark):
    """
    Crea la tabla Gold con metricas pre-agregadas.
    Particionada por dia de la hora del evento.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            event_hour      TIMESTAMP COMMENT 'Hora truncada del evento (granularidad horaria)',
            wiki            STRING    COMMENT 'Codigo del wiki (ej: enwiki, eswiki)',
            total_edits     BIGINT    COMMENT 'Total de ediciones en esa hora para ese wiki',
            bot_edits       BIGINT    COMMENT 'Ediciones realizadas por bots',
            human_edits     BIGINT    COMMENT 'Ediciones realizadas por humanos',
            total_bytes     BIGINT    COMMENT 'Suma total de byte_diff en esa hora',
            avg_byte_diff   DOUBLE    COMMENT 'Promedio de byte_diff por edicion',
            unique_editors  BIGINT    COMMENT 'Numero de editores unicos en esa hora',
            computed_at     TIMESTAMP COMMENT 'Timestamp de cuando se calculo esta metrica'
        )
        USING iceberg
        PARTITIONED BY (days(event_hour))
    """)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("GOLD LAYER -- Batch Aggregation: Silver -> Gold")
    print("=" * 70)

    ensure_namespace(spark)
    ensure_table(spark)

    # ----- Leer tabla Silver -----
    silver_df = spark.read.table(SILVER_TABLE)
    total_silver = silver_df.count()
    print(f"  Registros en Silver: {total_silver}")

    if total_silver == 0:
        print("  No hay registros en Silver. Finalizando.")
        spark.stop()
        return

    # ----- Agregar por hora y wiki -----
    # date_trunc("hour", ...) trunca el timestamp a la hora exacta.
    # Ejemplo: 2024-09-15 14:32:17 -> 2024-09-15 14:00:00
    gold_df = (
        silver_df
        .withColumn("event_hour", date_trunc("hour", col("event_timestamp")))
        .groupBy("event_hour", "wiki")
        .agg(
            count("*").alias("total_edits"),
            count(when(col("bot") == True, 1)).alias("bot_edits"),
            count(when(col("bot") == False, 1)).alias("human_edits"),
            spark_sum("byte_diff").alias("total_bytes"),
            spark_avg("byte_diff").alias("avg_byte_diff"),
            countDistinct("user_name").alias("unique_editors"),
        )
        .withColumn("computed_at", current_timestamp())
    )

    # ----- Escribir a Gold (overwrite por particion) -----
    # overwritePartitions() reemplaza solo las particiones que contienen
    # datos nuevos, dejando intactas las particiones historicas.
    gold_df.writeTo(GOLD_TABLE).overwritePartitions()

    result_count = gold_df.count()
    wikis_count = gold_df.select("wiki").distinct().count()
    print(f"  Metricas Gold calculadas: {result_count} filas")
    print(f"  Wikis procesados: {wikis_count}")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
