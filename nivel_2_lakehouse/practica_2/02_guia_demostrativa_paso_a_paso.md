# Guia Demostrativa: Paso a Paso

Sigue esta guia para levantar la infraestructura y ejecutar el flujo completo del Lakehouse.

## Paso 1: Levantar Infraestructura

Abre una terminal en el directorio `nivel_2_lakehouse` y ejecuta:

```bash
docker compose up -d --build
```

Espera unos 30 segundos y verifica que los contenedores esten corriendo:

```bash
docker compose ps
```

Deberias ver: `lab2-kafka`, `lab2-minio`, `lab2-iceberg-rest`, `lab2-trino`, `lab2-spark`, `lab2-producer`.
(`lab2-init-kafka` y `lab2-mc-init` son efimeros y deben mostrar estado `Exited (0)`).

## Paso 2: Ingesta Streaming (Bronze)

Ejecuta el script de streaming en Spark:

```bash
docker compose exec spark-iceberg spark-submit /home/iceberg/scripts/bronze_streaming.py
```

El script se quedara corriendo. En otra terminal, puedes ver logs del producer para confirmar que fluyen datos:

```bash
docker compose logs -f producer
```

Deja el streaming (Bronze) corriendo por al menos **2-3 minutos** para acumular un buen volumen de datos. Luego detenlo con `Ctrl+C` en la terminal del `spark-submit`.

## Paso 3: Limpieza y Deduplicacion (Silver)

Una vez que tengas datos en Bronze, ejecuta la transformacion Silver:

```bash
docker compose exec spark-iceberg spark-submit /home/iceberg/scripts/silver_batch.py
```

Veras un reporte en consola indicando cuantos registros se procesaron, filtraron y deduplicaron.

## Paso 4: Metricas Agregadas (Gold)

Calcula las metricas horarias leyendo desde Silver:

```bash
docker compose exec spark-iceberg spark-submit /home/iceberg/scripts/gold_aggregations.py
```

## Paso 5: Consultar desde Trino

Abre la consola de Trino:

```bash
docker compose exec trino trino
```

Dentro de Trino, verifica las tablas disponibles:

```sql
SHOW TABLES FROM lakehouse.bronze;
SHOW TABLES FROM lakehouse.silver;
SHOW TABLES FROM lakehouse.gold;
```

Ejecuta una consulta en la capa Gold para ver los resultados:

```sql
SELECT * FROM lakehouse.gold.wiki_hourly_metrics ORDER BY total_edits DESC LIMIT 5;
```

Puedes salir de Trino escribiendo `quit`.
