"""
Capa Bronze -- Structured Streaming: Kafka -> Apache Iceberg
=============================================================
Ingesta continua de eventos crudos de Wikipedia desde el topico Kafka
hacia la tabla Bronze en Apache Iceberg sobre MinIO (S3).

Los eventos se almacenan tal cual llegan del productor, preservando
el JSON original como string y agregando los metadatos de transporte
de Kafka (topic, partition, offset, timestamp).

Tabla destino: lakehouse.bronze.wiki_raw_events
Checkpoints:   s3a://checkpoints/bronze/wiki_raw_events
Trigger:       Cada 10 segundos (micro-batch)
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# ---------------------------------------------------------------------------
# Constantes de configuracion
# ---------------------------------------------------------------------------
ICEBERG_CATALOG = "lakehouse"
BRONZE_TABLE = f"{ICEBERG_CATALOG}.bronze.wiki_raw_events"
KAFKA_TOPIC = "wiki.changes"
KAFKA_BROKER = "kafka:29092"
CHECKPOINT_PATH = "/tmp/checkpoints/bronze/wiki_raw_events"


def create_spark_session():
    """
    Crea la sesion de Spark con las extensiones de Iceberg habilitadas
    y configuracion de acceso a MinIO via protocolo S3A.
    """
    return (
        SparkSession.builder
        .appName("Bronze-WikiRawEvents-Streaming")
        # --- Extensiones Iceberg ---
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", ICEBERG_CATALOG)
        # --- Catalogo REST de Iceberg ---
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
        # --- Acceso S3A a MinIO (para checkpoints y file I/O de Hadoop) ---
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
    """Crea el namespace 'bronze' en el catalogo Iceberg si no existe."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.bronze")


def ensure_table(spark):
    """
    Crea la tabla Bronze en Iceberg si no existe.
    Almacena el evento crudo de Kafka como STRING junto con sus metadatos
    de transporte (partition, offset, timestamp).
    Particionada por dia de llegada del mensaje a Kafka.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            kafka_key           STRING    COMMENT 'Clave del mensaje Kafka (wiki language code)',
            kafka_value         STRING    COMMENT 'Payload JSON crudo del evento Wikimedia',
            kafka_topic         STRING    COMMENT 'Nombre del topico Kafka de origen',
            kafka_partition     INT       COMMENT 'Particion de Kafka donde residia el mensaje',
            kafka_offset        BIGINT    COMMENT 'Offset del mensaje dentro de la particion',
            kafka_timestamp     TIMESTAMP COMMENT 'Timestamp asignado por el broker Kafka',
            ingestion_timestamp TIMESTAMP COMMENT 'Timestamp del momento de escritura a Iceberg'
        )
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
    """)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("BRONZE LAYER -- Structured Streaming: Kafka -> Iceberg")
    print("=" * 70)
    print(f"  Topico Kafka:       {KAFKA_TOPIC}")
    print(f"  Tabla destino:      {BRONZE_TABLE}")
    print(f"  Checkpoint:         {CHECKPOINT_PATH}")
    print(f"  Trigger interval:   10 seconds")
    print("=" * 70)

    ensure_namespace(spark)
    ensure_table(spark)

    # ----- Leer stream de Kafka -----
    # Spark crea un DataFrame con columnas estandar de Kafka:
    #   key (binary), value (binary), topic (string),
    #   partition (int), offset (long), timestamp (timestamp)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ----- Transformar: castear binarios a string y agregar timestamp -----
    bronze_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("kafka_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp"),
    )

    # ----- Escribir a tabla Iceberg en modo append -----
    # outputMode("append"): cada micro-batch agrega filas nuevas sin modificar
    # las existentes. Es el unico modo compatible con tablas Iceberg.
    query = (
        bronze_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .toTable(BRONZE_TABLE)
    )

    timeout_sec = None
    if len(sys.argv) > 1:
        try:
            timeout_sec = int(sys.argv[1])
        except ValueError:
            timeout_sec = None

    if timeout_sec:
        print(f"\nStreaming Bronze ejecutandose por {timeout_sec} segundos...\n")
        query.awaitTermination(timeout=timeout_sec)
        query.stop()
        print("\nStreaming Bronze finalizado de forma controlada tras el tiempo indicado.\n")
    else:
        print("\nStreaming Bronze ACTIVO. Ctrl+C para detener.\n")
        query.awaitTermination()


if __name__ == "__main__":
    main()
