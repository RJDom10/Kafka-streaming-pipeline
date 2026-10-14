# Arquitectura Medallion: Cuando y Por Que Usarla

## Origen del Patron

La Arquitectura Medallion fue popularizada por Databricks como un patron
de organizacion de datos en Data Lakehouses. Divide el flujo de datos en
tres capas con responsabilidades claras y progresivas.

## Las Tres Capas

### Bronze: La Capa de Ingesta

**Responsabilidad:** Capturar y preservar todos los datos crudos tal cual
llegan de las fuentes, sin transformacion ni filtrado.

**Principios:**
- Append-only: nunca se modifican ni eliminan registros
- Schema flexible: se almacena el payload original (JSON, Avro, etc.)
- Incluir metadatos de transporte (timestamps, offsets, fuente)
- Servir como log de auditoria inmutable

**En nuestro laboratorio:** `lakehouse.bronze.wiki_raw_events` almacena el
JSON crudo de cada evento junto con los metadatos de Kafka.

### Silver: La Capa de Conformacion

**Responsabilidad:** Transformar los datos crudos en un formato limpio,
tipado y deduplicado, listo para analisis o para alimentar la capa Gold.

**Principios:**
- Schema estricto con tipos fuertes (no mas strings genericos)
- Filtrado de registros malformados o irrelevantes
- Deduplicacion por clave natural de negocio
- Enriquecimiento con datos de referencia (joins con dimensiones)

**En nuestro laboratorio:** `lakehouse.silver.wiki_cleaned` contiene
columnas tipadas, sin nulos en campos clave y sin duplicados.

### Gold: La Capa de Presentacion

**Responsabilidad:** Producir datasets agregados, modelados y optimizados
para consulta directa por usuarios de negocio o herramientas de BI.

**Principios:**
- Metricas pre-calculadas (evitar recalcular en cada query)
- Granularidad definida por caso de uso (horaria, diaria, por region)
- Optimizado para lectura (columnas selectivas, particionamiento fino)
- Idempotente: re-ejecutar produce el mismo resultado

**En nuestro laboratorio:** `lakehouse.gold.wiki_hourly_metrics` contiene
metricas horarias por wiki, listas para consulta desde Trino.

## Cuando Usar Medallion

### Si se debe usar cuando:

- El volumen de datos excede lo que una base de datos relacional puede manejar
- Se necesita preservar los datos crudos para re-procesamiento o auditoria
- Multiples equipos o aplicaciones consumen los datos con distintos niveles
  de refinamiento
- Se requiere schema evolution sin downtime
- El pipeline necesita ser reproducible e idempotente

### No es necesario cuando:

- El volumen de datos cabe en una sola base de datos PostgreSQL (< 100 GB)
- No hay requisitos de auditoria o re-procesamiento
- Solo existe un consumidor de los datos
- La latencia de millisegundos es critica (usar Kafka Streams o Flink)

## Flujo de Re-procesamiento

Una ventaja clave de Medallion es la capacidad de re-procesar datos:

```
1. Se detecta un bug en la logica Silver
2. Se corrige el codigo de silver_batch.py
3. Se trunca la tabla Silver (DROP + CREATE)
4. Se re-ejecuta silver_batch.py -> Lee Bronze completo
5. Se re-ejecuta gold_aggregations.py -> Lee Silver re-procesado
6. Los datos Gold reflejan la logica corregida
```

Bronze nunca se toca. Sirve como **fuente de verdad inmutable**.
