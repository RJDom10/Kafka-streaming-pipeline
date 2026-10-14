# Comandos Utiles

## Docker

- `docker compose up -d`: Iniciar todos los servicios
- `docker compose ps`: Ver estado de servicios
- `docker compose logs -f <servicio>`: Ver logs en vivo (ej. `producer`)
- `docker compose down -v`: Detener servicios y borrar volumenes de datos (reset total)

## Spark & PySpark

- `docker compose exec spark-iceberg spark-submit <ruta>`: Ejecutar script Python en el cluster
- `docker compose exec spark-iceberg pyspark`: Abrir shell interactiva de PySpark
- `docker compose exec spark-iceberg spark-sql`: Abrir consola SQL interactiva

## Trino

- `docker compose exec trino trino`: Entrar al CLI SQL de Trino
- `docker compose exec trino trino --execute "SELECT 1;"`: Ejecutar query desde bash

## URLs Importantes

- **MinIO S3 Console:** [http://localhost:9001](http://localhost:9001) (admin/password)
- **Spark UI:** [http://localhost:4040](http://localhost:4040) (solo activo mientras corre un job)
- **Jupyter Notebook:** [http://localhost:8888](http://localhost:8888) (Token visible en logs de `spark-iceberg`)
- **Trino UI:** [http://localhost:8082](http://localhost:8082)
