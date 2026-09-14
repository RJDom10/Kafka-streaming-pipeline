# 🚀 Real-Time Event Streaming Pipeline with Apache Kafka & PostgreSQL

Pipeline de datos en tiempo real de grado industrial que ingesta eventos globales continuos de Wikimedia, los desacopla mediante **Apache Kafka (KRaft)**, los consume de forma concurrente con **Python** y los persiste en **PostgreSQL**, incorporando observabilidad visual con **Kafka UI**.

---

## 🏗️ Arquitectura del Sistema

```
 [ Wikimedia EventStreams ] (SSE Stream en vivo)
             │
             ▼
      [ producer.py ] (Python Ingestor)
             │
             ▼ (JSON Payload - Key: wiki)
   [ Apache Kafka (KRaft) ]
     └── Topic: wiki.changes (3 Particiones, ReplicationFactor: 1)
             │
             ├──► [ consumer.py ] (wiki-persister-group)
             │           │
             │           ▼ (Batch Insert / Idempotent)
             │     [ PostgreSQL 16 ] (Tabla: wiki_recent_changes)
             │
             └──► [ Kafka UI ] (Consola de Observabilidad: Puerto 8080)
```

---

## 📁 Estructura del Repositorio

* 📖 **[`docs/`](./docs/README.md)**: Documentación técnica exhaustiva línea por línea de cada archivo del proyecto (`producer.py`, `consumer.py`, `docker-compose.yml`, `init.sql`, etc.), detallando el flujo de ejecución completo.
* 📚 **[`guide/`](./guide/README.md)**: Guía técnica completa sobre fundamentos de Kafka, comparativa de Arquitecturas Lambda vs. Kappa, modelo Medallion (Bronce, Plata, Oro) y roadmap de escalabilidad industrial (Airflow, MLflow, Iceberg, PySpark).
* 🧪 **[`practica_1/`](./practica_1/README.md)**: Laboratorio práctico guiado de escalabilidad horizontal, grupos de consumidores, asignación de particiones, simulación de fallos (Chaos Engineering) y retos de aplicación autónoma.

---

## 🚀 Inicio Rápido (Quickstart)

### Requisitos
* Docker y Docker Compose instalados.
* Al menos 2.4 GB de memoria RAM libre.

### Despliegue del Clúster
```bash
# 1. Clonar el repositorio
git clone <URL_DEL_REPOSITORIO>
cd lab_kafka_test

# 2. Levantar todos los servicios
docker compose up -d

# 3. Verificar estado
docker compose ps
```

### Servicios Disponibles
* **Kafka Broker (KRaft):** `localhost:9092`
* **PostgreSQL:** `localhost:5432` (`db: wikidb`, `user: wikiuser`)
* **Kafka UI (Interfaz Web):** [http://localhost:8080](http://localhost:8080)

