# Guia Tecnica Completa: Pipeline de Streaming con Apache Kafka

**Proyecto:** Lab Kafka — Ingesta y Desacoplamiento en Tiempo Real
**Tecnologia Central:** Apache Kafka + PostgreSQL + Python
**Nivel:** Intermedio — Avanzado

---

## Indice de la Guia

| # | Documento | Contenido |
|---|---|---|
| 01 | [01_ejecucion_pipeline.md](./01_ejecucion_pipeline.md) | Guia paso a paso para ejecutar el pipeline completo |
| 02 | [02_kafka_teoria_fundamentos.md](./02_kafka_teoria_fundamentos.md) | Teoria tecnica profunda de Kafka: brokers, topics, particiones, offsets |
| 03 | [03_arquitecturas_lambda_kappa.md](./03_arquitecturas_lambda_kappa.md) | Arquitecturas Lambda y Kappa: teoria, comparativa y casos de uso |
| 04 | [04_medallion_architecture.md](./04_medallion_architecture.md) | Capas Medallion: Bronce, Plata y Oro — donde esta el proyecto hoy |
| 05 | [05_escalabilidad_tecnologias.md](./05_escalabilidad_tecnologias.md) | Escalar con PySpark, Airflow, MLflow, TensorFlow, Apache Iceberg |
| 06 | [06_innovacion_y_relevancia.md](./06_innovacion_y_relevancia.md) | Innovacion tecnologica, modernidad y relevancia en la industria |

---

## Mapa del Proyecto en el Ecosistema de Datos

```
NIVEL ACTUAL DEL PROYECTO
          |
          v
[INGESTA EN TIEMPO REAL]  <-- Aqui estamos
Wikimedia SSE --> Kafka --> PostgreSQL

          |
          v (proximos pasos)
[PROCESAMIENTO] --> PySpark / Flink
[ALMACENAMIENTO] --> Apache Iceberg (capas Medallion)
[ORQUESTACION] --> Apache Airflow
[ML/IA] --> MLflow + TensorFlow / PyTorch
[VISUALIZACION] --> Apache Superset / Grafana
```

---

## Stack Tecnologico Actual vs Vision Futura

| Capa | Hoy | Vision Escalada |
|---|---|---|
| Ingesta | Kafka KRaft + Producer Python | Kafka + Schema Registry + Debezium |
| Transporte | Tópico wiki.changes (3 particiones) | Kafka con 100+ particiones, multi-cluster |
| Procesamiento | Consumer Python (batch 50) | PySpark Structured Streaming / Flink |
| Almacenamiento | PostgreSQL 16 | Apache Iceberg sobre S3/MinIO |
| Orquestacion | Docker Compose | Apache Airflow + Kubernetes |
| ML/IA | (no implementado aun) | MLflow + TensorFlow Serving |
| Gobierno | (no implementado aun) | Apache Atlas + Great Expectations |

---

## Prerequisitos para Ejecutar el Proyecto

- Docker Desktop >= 24.0 con WSL2 backend
- Docker Compose >= 2.24
- RAM disponible: minimo 3 GB (recomendado 4 GB)
- Conexion a internet (para el stream de Wikimedia)
- SO: Ubuntu (WSL2), macOS, o Linux nativo

---

## Orden de Lectura Recomendado

### Para entender el proyecto actual:
1. Lee `01_ejecucion_pipeline.md` — ejecuta el pipeline
2. Lee `02_kafka_teoria_fundamentos.md` — entiende Kafka a fondo
3. Lee `04_medallion_architecture.md` — ubica el proyecto en el ecosistema

### Para entender la vision a futuro:
4. Lee `03_arquitecturas_lambda_kappa.md` — arquitecturas de referencia
5. Lee `05_escalabilidad_tecnologias.md` — como escalar
6. Lee `06_innovacion_y_relevancia.md` — impacto en la industria
