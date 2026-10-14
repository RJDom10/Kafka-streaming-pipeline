# Retos Practicos

Estos retos pondran a prueba tu comprension de Apache Iceberg y Trino usando
la terminal SQL de Trino. Asegurate de tener datos en las tablas Silver y Bronze.

Accede a la consola de Trino:
```bash
docker compose exec trino trino
```

## Reto 1: Time Travel en Bronze

**Contexto:** Necesitas auditar cuantos registros habia en la tabla Bronze hace unos instantes, antes de que llegaran los ultimos lotes.
**Objetivo:** Usar la funcionalidad de Time Travel de Iceberg.
1. Lista el historial de snapshots de la tabla `lakehouse.bronze.wiki_raw_events`.
2. Identifica el `snapshot_id` del penultimo o primer snapshot.
3. Ejecuta una consulta `COUNT(*)` sobre la tabla, especificando el snapshot antiguo.
4. Compara el conteo con el snapshot mas reciente.

## Reto 2: Evolucion de Esquema (Schema Evolution)

**Contexto:** El equipo de BI necesita una nueva metrica, quieren clasificar
si una edicion fue pequena o grande directamente en la tabla Silver.
**Objetivo:** Modificar el esquema de una tabla Iceberg en caliente sin reescribir datos.
1. Agrega una nueva columna llamada `edit_size_category` de tipo `VARCHAR` a `lakehouse.silver.wiki_cleaned`.
2. Actualiza los registros existentes en Silver: si `byte_diff` es mayor a 500 o menor a -500, clasificalo como 'LARGE_CHANGE', de lo contrario 'SMALL_CHANGE'.
3. Consulta la tabla para verificar que la columna existe y tiene datos.

## Reto 3: Analisis Ad-hoc con Trino SQL

**Contexto:** Necesitamos un reporte rapido desde la tabla Silver para ver el
impacto real de los bots en las wikis principales.
**Objetivo:** Usar funciones agregadas en Trino.
1. Consulta la tabla `lakehouse.silver.wiki_cleaned`.
2. Calcula para cada `wiki`: el total de ediciones, el promedio de `byte_diff` de las ediciones hechas por *bots* vs *humanos*.
3. Muestra solo los top 5 wikis con mas ediciones totales.

## Reto 4: Mantenimiento - Compaction (Opcional, avanzado via Spark SQL)

**Contexto:** El streaming Bronze ha generado docenas de archivos parquet muy pequenos.
**Objetivo:** Reescribir los archivos de datos para optimizar lecturas.
Para este reto debes iniciar un shell de Spark SQL o Jupyter:
```bash
# Spark SQL CLI
docker compose exec spark-iceberg spark-sql
```
1. Usa el "procedure" de sistema de Iceberg llamado `rewrite_data_files` sobre la tabla `lakehouse.bronze.wiki_raw_events`.
2. Selecciona la estrategia `binpack`.

---
*Nota: Si te trabas, revisa la carpeta `soluciones/`.*
