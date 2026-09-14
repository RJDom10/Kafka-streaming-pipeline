# Escalabilidad: De este Laboratorio a Produccion Industrial

**Documento:** `05_escalabilidad_tecnologias.md`
**Objetivo:** Mostrar como este pipeline puede escalar con tecnologias de clase mundial: PySpark, Apache Airflow, MLflow, TensorFlow y Apache Iceberg.

---

## 1. La Vision: Arquitectura de Datos Moderna Completa

El proyecto actual es el nucleo de una arquitectura que puede crecer hasta manejar petabytes de datos con las mismas tecnologias que usan Netflix, Uber, LinkedIn y Spotify.

```
VISION ARQUITECTURA COMPLETA

┌──────────────────────────────────────────────────────────────────────────┐
│                      INGESTA Y TRANSPORTE                                │
│  Wikimedia SSE ──> Kafka (Multi-broker, Schema Registry, 100+ particiones)│
│  Otras fuentes: APIs REST, CDC de bases de datos (Debezium), IoT, Logs   │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                                v
┌──────────────────────────────────────────────────────────────────────────┐
│                   PROCESAMIENTO (PYSPARK / FLINK)                        │
│  Spark Structured Streaming: limpieza, validacion, deduplicacion         │
│  Flink: procesamiento de eventos complejos, joins temporales             │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                                v
┌──────────────────────────────────────────────────────────────────────────┐
│              ALMACENAMIENTO (APACHE ICEBERG + S3/MinIO)                  │
│  Capa Bronce: datos crudos en Parquet                                    │
│  Capa Plata: datos limpios y enriquecidos                                │
│  Capa Oro: agregaciones y features de ML                                 │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    v                       v
┌────────────────────────┐   ┌──────────────────────────────┐
│  ORQUESTACION          │   │  ML/IA                       │
│  Apache Airflow        │   │  MLflow + TensorFlow/PyTorch │
│  DAGs para ETL         │   │  Deteccion de vandalismos    │
│  Monitorizacion        │   │  Prediccion de tendencias    │
└────────────────────────┘   └──────────────────────────────┘
                                │
                                v
┌──────────────────────────────────────────────────────────────────────────┐
│                    VISUALIZACION Y SERVICIO                              │
│  Apache Superset / Grafana: Dashboards en tiempo real                    │
│  FastAPI / GraphQL: APIs de datos                                        │
│  Trino / Presto: SQL sobre el Data Lakehouse                             │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. PySpark: Procesamiento Distribuido

### Que es PySpark

**Apache Spark** es el motor de procesamiento distribuido mas usado en Data Engineering. Permite procesar datos en paralelo en un cluster de maquinas. **PySpark** es la API de Python para Spark.

### Por que necesitamos Spark en este proyecto

El consumer actual en Python tiene limitaciones:
- Procesa en un solo hilo (un proceso Python)
- BATCH_SIZE=50 limita el throughput
- No puede hacer joins complejos con otras fuentes de datos en memoria distribuida
- No tiene recuperacion automatica avanzada ante fallos

### Spark Structured Streaming: El Reemplazo Natural del Consumer

```python
# consumer_spark.py — Version escalable con PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, LongType

# Esquema del mensaje JSON de Kafka
wiki_schema = StructType([
    StructField("wiki", StringType()),
    StructField("title", StringType()),
    StructField("user_name", StringType()),
    StructField("bot", BooleanType()),
    StructField("change_type", StringType()),
    StructField("length_old", IntegerType()),
    StructField("length_new", IntegerType()),
    StructField("byte_diff", IntegerType()),
    StructField("event_timestamp", LongType())
])

spark = SparkSession.builder \
    .appName("WikimediaKafkaConsumer") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Leer el stream de Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "wiki.changes") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10000) \  # Procesar hasta 10k msgs por micro-batch
    .load()

# Deserializar el JSON y aplicar transformaciones
wiki_df = kafka_stream \
    .select(from_json(col("value").cast("string"), wiki_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
    .withColumn("wiki_language", col("wiki").substr(1, 2)) \
    .withColumn("event_date", col("event_timestamp").cast("date")) \
    .filter(col("wiki").isNotNull()) \
    .filter(col("byte_diff").between(-100000, 100000))  # Filtro de anomalias

# Escribir a Apache Iceberg (capa Bronce)
wiki_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "s3://data-lake/bronce/wiki_changes") \
    .option("checkpointLocation", "s3://data-lake/checkpoints/wiki_bronce") \
    .trigger(processingTime="30 seconds") \  # Micro-batch cada 30 segundos
    .start()
```

### Ventajas de Spark vs Consumer Python Actual

| Aspecto | Consumer Python (actual) | PySpark Streaming |
|---|---|---|
| Paralelismo | 1 proceso | N workers en cluster |
| Throughput | ~100 msgs/s | Millones msgs/s |
| Joins con otras fuentes | No | Si (broadcast joins, stream-stream joins) |
| Recuperacion ante fallos | Reinicio manual | Automatica (checkpointing) |
| Complejidad del codigo | Baja | Media |
| Infraestructura | Docker simple | Cluster (Kubernetes + Spark Operator) |

---

## 3. Apache Airflow: Orquestacion de Pipelines

### Que es Airflow

Apache Airflow es un **orquestador de workflows** open source creado por Airbnb. Permite definir, programar y monitorear pipelines de datos como grafos acilicos dirigidos (DAGs).

### Por que necesitamos Airflow

El proyecto actual tiene un pipeline que corre "para siempre" de forma continua. Para produccion necesitamos:
- **Programar** jobs batch nocturnos (calcular estadisticas del dia anterior)
- **Dependencias** entre tareas (el job de Plata debe terminar antes de Oro)
- **Alertas** cuando un job falla
- **Reintentos automaticos** con backoff exponencial
- **Linaje** de datos: saber que jobs generaron cada dataset

### DAG de Ejemplo para este Proyecto

```python
# airflow/dags/wiki_medallion_dag.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@empresa.com"]
}

with DAG(
    dag_id="wiki_medallion_pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Cada dia a las 2am
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["kafka", "wikimedia", "medallion"]
) as dag:

    # Tarea 1: Verificar que hay datos en Kafka del dia anterior
    verificar_kafka = SparkSubmitOperator(
        task_id="verificar_datos_kafka",
        application="jobs/verificar_kafka.py",
        conf={"spark.executor.memory": "2g"}
    )

    # Tarea 2: Bronce -> Plata (limpieza y enriquecimiento)
    bronce_a_plata = SparkSubmitOperator(
        task_id="bronce_a_plata",
        application="jobs/bronce_to_silver.py",
        application_args=["--date", "{{ ds }}"],  # ds = fecha de ejecucion
        conf={"spark.executor.instances": "4"}
    )

    # Tarea 3: Plata -> Oro (agregaciones para dashboards)
    plata_a_oro_stats = SparkSubmitOperator(
        task_id="plata_a_oro_estadisticas",
        application="jobs/silver_to_gold_stats.py",
        application_args=["--date", "{{ ds }}"]
    )

    # Tarea 4: Plata -> Oro (features para ML)
    plata_a_oro_ml = SparkSubmitOperator(
        task_id="plata_a_oro_features_ml",
        application="jobs/silver_to_gold_ml_features.py",
        application_args=["--date", "{{ ds }}"]
    )

    # Tarea 5: Actualizar serving layer (PostgreSQL / Redis)
    actualizar_serving = PostgresOperator(
        task_id="actualizar_vistas_materializadas",
        postgres_conn_id="postgres_wikidb",
        sql="REFRESH MATERIALIZED VIEW mv_wiki_daily_stats;"
    )

    # Definir el orden de ejecucion (DAG):
    verificar_kafka >> bronce_a_plata >> [plata_a_oro_stats, plata_a_oro_ml] >> actualizar_serving
```

---

## 4. Apache Iceberg: El Formato de Tabla Abierto

### Que es Apache Iceberg

Apache Iceberg es un **formato de tabla abierto** para datasets masivos en object storage (S3, GCS, HDFS). No es una base de datos sino un protocolo que agrega capacidades transaccionales a los archivos Parquet en la nube.

### Capacidades Criticas

#### ACID Transactions sobre object storage
```sql
-- Antes de Iceberg: no se podia hacer esto de forma segura en S3
DELETE FROM wiki_changes WHERE bot = TRUE AND event_date = '2026-09-13';
UPDATE wiki_changes SET user_type = 'bot' WHERE user_name = 'ClueBot NG';

-- Con Iceberg: estas operaciones son totalmente seguras y atomicas
```

#### Time Travel (Viaje en el Tiempo)
```sql
-- Ver como eran los datos hace 3 dias
SELECT * FROM wiki_changes
AS OF TIMESTAMP '2026-09-10 00:00:00';

-- Ver una version especifica de la tabla
SELECT * FROM wiki_changes VERSION AS OF 1234567890;
```

#### Schema Evolution (Evolucion de Esquema)
```sql
-- Agregar una columna sin reescribir todos los datos
ALTER TABLE wiki_changes ADD COLUMN country STRING;

-- Renombrar una columna
ALTER TABLE wiki_changes RENAME COLUMN user_name TO editor_name;
```

### Como se Integra con este Proyecto

```
kafka-connect (o consumer Python) escribe a:
  s3://data-lake/bronce/wiki_changes/
    data/year=2026/month=09/day=13/
      part-00000-a2b3c4d5.parquet   (datos reales)
    metadata/
      v1.metadata.json               (esquema, snapshots)
      snap-1234567890-1-abcde.avro   (estado del snapshot)

Spark SQL puede consultar con:
  SELECT * FROM iceberg.bronce.wiki_changes
  WHERE event_date = '2026-09-13'
  -- Spark lee solo los archivos Parquet relevantes (partition pruning)
```

---

## 5. MLflow: Gestion del Ciclo de Vida de Modelos ML

### Que es MLflow

MLflow es una plataforma open source para gestionar el **ciclo de vida completo de modelos de Machine Learning**: desde el experimento hasta el despliegue en produccion.

### Caso de Uso en este Proyecto: Deteccion de Vandalismos

```python
# ml/train_vandalism_detector.py
import mlflow
import mlflow.sklearn
from pyspark.sql import SparkSession
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score

mlflow.set_tracking_uri("http://mlflow-server:5000")
mlflow.set_experiment("wiki-vandalism-detection")

with mlflow.start_run(run_name="GBM_v3_2026-09-13"):

    # Leer features de la capa Oro (tabla ml_edit_features)
    spark = SparkSession.builder.getOrCreate()
    df = spark.table("gold.ml_edit_features").toPandas()

    X = df[["user_edit_count", "user_age_days", "article_edit_count",
            "byte_diff_zscore", "edit_hour", "is_bot"]]
    y = df["is_vandalism"]

    # Hiperparametros (registrados en MLflow para comparacion)
    params = {
        "n_estimators": 500,
        "max_depth": 5,
        "learning_rate": 0.05,
        "min_samples_split": 20
    }
    mlflow.log_params(params)

    # Entrenar el modelo
    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # Calcular y registrar metricas
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    precision = precision_score(y_test, model.predict(X_test))
    recall = recall_score(y_test, model.predict(X_test))

    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)

    # Registrar el modelo en el Model Registry de MLflow
    mlflow.sklearn.log_model(
        model,
        artifact_path="vandalism_model",
        registered_model_name="WikiVandalismDetector"
    )
    # El modelo queda disponible en: http://mlflow-server:5000
```

### Flujo MLflow Completo

```
1. Ingenieria de Features (PySpark)
   Capa Oro (ml_edit_features)
            |
            v
2. Entrenamiento (MLflow Tracking)
   Experimentos, hiperparametros, metricas
            |
            v
3. Registro (MLflow Model Registry)
   Versiones: Staging -> Production
            |
            v
4. Despliegue (TensorFlow Serving / MLflow Serve)
   REST API: POST /predict {evento} -> {score_vandalism}
            |
            v
5. Monitoreo (MLflow + Grafana)
   Drift detection, performance en produccion
```

---

## 6. TensorFlow / PyTorch: Deep Learning sobre los Datos

### Caso de Uso: Modelo de Lenguaje para Analisis de Ediciones

```python
# ml/bert_edit_classifier.py — Clasificador de calidad de ediciones con BERT
import tensorflow as tf
from transformers import TFBertForSequenceClassification, BertTokenizer

# Leer datos de entrenamiento desde la capa Oro
# (comentarios de ediciones + etiqueta de calidad)
tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased')
model = TFBertForSequenceClassification.from_pretrained(
    'bert-base-multilingual-cased',
    num_labels=3  # 0=vandalism, 1=neutral, 2=good_faith_edit
)

# Fine-tuning con datos de Wikimedia
# El pipeline entrega los datos de entrenamiento via la capa Oro de Iceberg
with mlflow.start_run():
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=2e-5),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=['accuracy']
    )
    model.fit(train_dataset, validation_data=val_dataset, epochs=3)
    mlflow.tensorflow.log_model(model, "bert_edit_classifier")
```

---

## 7. Mapa de Escalabilidad: Del Lab a Produccion

| Componente | Laboratorio (Actual) | Produccion (Escalado) |
|---|---|---|
| Kafka | 1 broker, 3 particiones, KRaft | 5+ brokers, 100+ particiones, Schema Registry, Kafka Connect |
| Consumer | Python simple, batch=50 | PySpark Structured Streaming, N workers |
| Almacenamiento | PostgreSQL 16 (una maquina) | Apache Iceberg en S3 (petabytes) |
| Orquestacion | docker compose | Apache Airflow en Kubernetes |
| Procesamiento batch | No implementado | Apache Spark en EMR / Databricks |
| ML | No implementado | MLflow + TensorFlow Serving + Seldon |
| Visualizacion | Kafka UI (solo Kafka) | Apache Superset + Grafana + Metabase |
| Monitoreo | docker logs | Prometheus + Grafana + ELK Stack |
| Infraestructura | Docker local | Kubernetes (EKS / GKE / AKS) |
| Costo estimado | $0 (local) | $2,000-$50,000/mes (segun escala) |
