# Documentacion: `spark/gold_aggregations.py`

## Proposito del Script

Este script implementa la **Capa Gold** de la Arquitectura Medallion.
Lee la tabla Silver (datos limpios) y calcula metricas agregadas por hora
y por wiki, produciendo una tabla dimensional lista para consulta directa
por motores SQL como Trino o herramientas de BI.

## Metricas Calculadas

| Metrica | Funcion Spark | Descripcion |
|---|---|---|
| `total_edits` | `count("*")` | Total de ediciones en esa hora para ese wiki |
| `bot_edits` | `count(when(bot == True, 1))` | Ediciones realizadas por bots automaticos |
| `human_edits` | `count(when(bot == False, 1))` | Ediciones realizadas por editores humanos |
| `total_bytes` | `sum("byte_diff")` | Suma total de bytes agregados/eliminados |
| `avg_byte_diff` | `avg("byte_diff")` | Promedio de bytes por edicion |
| `unique_editors` | `countDistinct("user_name")` | Editores unicos en esa hora |

## Truncamiento Temporal

```python
.withColumn("event_hour", date_trunc("hour", col("event_timestamp")))
```

`date_trunc("hour", ...)` trunca cada timestamp a la hora exacta:

```
2024-09-15 14:32:17 -> 2024-09-15 14:00:00
2024-09-15 14:58:02 -> 2024-09-15 14:00:00
2024-09-15 15:01:45 -> 2024-09-15 15:00:00
```

Esto permite agrupar todas las ediciones de una misma hora en un unico
registro por wiki, creando una serie temporal de metricas horarias.

## Modo de Escritura: overwritePartitions()

```python
gold_df.writeTo(GOLD_TABLE).overwritePartitions()
```

A diferencia de Bronze (append puro) y Silver (append con deduplicacion),
Gold usa **overwrite por particion**. Esto significa que si se re-ejecuta
el script, las metricas de las horas ya calculadas se reemplazan con los
valores actualizados, sin generar duplicados.

Esta operacion es **idempotente**: ejecutarla N veces produce el mismo
resultado que ejecutarla una sola vez.

## Consultas de Validacion

Despues de ejecutar el script Gold, se pueden validar los resultados
desde Trino:

```sql
-- Top 10 wikis mas activos
SELECT wiki, SUM(total_edits) AS ediciones_totales
FROM lakehouse.gold.wiki_hourly_metrics
GROUP BY wiki
ORDER BY ediciones_totales DESC
LIMIT 10;

-- Proporcion bots vs humanos global
SELECT
    SUM(bot_edits) AS total_bot,
    SUM(human_edits) AS total_human,
    ROUND(SUM(bot_edits) * 100.0 / SUM(total_edits), 2) AS pct_bots
FROM lakehouse.gold.wiki_hourly_metrics;
```
