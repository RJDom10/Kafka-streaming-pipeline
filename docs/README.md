# Documentación del Laboratorio Kafka

Bienvenido a la documentación completa del **Laboratorio 1: Ingesta y Desacoplamiento con Apache Kafka**.

## Índice de Documentos

| # | Archivo | Descripción |
|---|---|---|
| 01 | [01_docker-compose.md](./01_docker-compose.md) | Orquestador de servicios: análisis completo de cada servicio, variables de entorno, healthchecks y configuración de Kafka KRaft |
| 02 | [02_producer.md](./02_producer.md) | Script Python del productor: conexión SSE a Wikimedia, sanitización de eventos, publicación a Kafka con batching y callbacks |
| 03 | [03_consumer.md](./03_consumer.md) | Script Python del consumidor: lectura de Kafka, gestión de offsets, inserción en lote a PostgreSQL |
| 04 | [04_init_sql.md](./04_init_sql.md) | Esquema de base de datos: tabla wiki_recent_changes, tipos de datos, índices y queries de ejemplo |
| 05 | [05_dockerfile.md](./05_dockerfile.md) | Imagen Docker personalizada: capas, optimización de caché, diferencias entre RUN/CMD/ENTRYPOINT |
| 06 | [06_requirements.md](./06_requirements.md) | Dependencias Python: confluent-kafka, psycopg2-binary, requests — por qué cada una |
| 07 | [07_flujo_y_arquitectura.md](./07_flujo_y_arquitectura.md) | **Documento principal**: flujo completo de ejecución, importancia de Kafka, arquitectura, diagramas y comparativas |

## Orden de Lectura Recomendado

1. Empieza por **[07_flujo_y_arquitectura.md](./07_flujo_y_arquitectura.md)** para entender el panorama general
2. Lee **[01_docker-compose.md](./01_docker-compose.md)** para entender la infraestructura
3. Lee **[02_producer.md](./02_producer.md)** y **[03_consumer.md](./03_consumer.md)** para entender el código Python
4. Lee **[04_init_sql.md](./04_init_sql.md)** para entender la base de datos
5. Lee **[05_dockerfile.md](./05_dockerfile.md)** y **[06_requirements.md](./06_requirements.md)** para entender el empaquetado

## Variables Clave para Experimentar

| Variable | Archivo | Efecto |
|---|---|---|
| `--partitions` | docker-compose.yml (init-kafka) | Paralelismo del tópico |
| `--replication-factor` | docker-compose.yml (init-kafka) | Tolerancia a fallos |
| `KAFKA_JVM_PERFORMANCE_OPTS` | docker-compose.yml (kafka) | Memoria de la JVM |
| `BATCH_SIZE = 50` | consumer.py | Tamaño del batch de inserción |
| `linger.ms: 20` | producer.py | Buffer de agrupación de mensajes |
| `acks: "1"` | producer.py | Nivel de confirmación de entrega |
