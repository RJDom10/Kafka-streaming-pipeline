# Consultas Analíticas en Vivo: Exploración del Stream de Wikimedia

**Documento:** `02_consultas_analiticas_en_vivo.md`  
**Módulo:** Consultas y Analítica SQL  

---

## 🎯 Objetivo
Aprender a extraer valor y métricas de negocio en tiempo real a partir de los datos que fluyen continuamente desde Kafka hacia la tabla `wiki_recent_changes` en PostgreSQL.

---

## 1. Inspección Básica de Registros Recientes

### Consulta: Últimas 5 ediciones en vivo con formato legible
```sql
SELECT 
    id,
    wiki,
    user_name,
    CASE WHEN bot THEN '🤖 Bot' ELSE '👤 Humano' END AS tipo,
    change_type,
    byte_diff,
    TO_CHAR(event_timestamp, 'HH24:MI:SS') AS hora_evento
FROM wiki_recent_changes
ORDER BY id DESC
LIMIT 5;
```

---

## 2. Métricas de Tráfico Global y Lenguajes

### Consulta: Los 10 Wikis con mayor actividad en el clúster
Esta consulta agrupa millones de ediciones para mostrar qué proyectos de Wikimedia concentran la mayor atención global:

```sql
SELECT 
    wiki,
    COUNT(*) AS total_ediciones,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_global
FROM wiki_recent_changes
GROUP BY wiki
ORDER BY total_ediciones DESC
LIMIT 10;
```

> **💡 Consejo en Terminal:**  
> Ejecuta esta consulta con `\watch 3` en `psql` para ver cómo los porcentajes se recalculan en tiempo real mientras el productor sigue ingiriendo eventos.

---

## 3. Inteligencia de Comportamiento: Bots vs. Humanos

### Consulta: Comparativa de volumen y tamaño de edición
¿Quién escribe más contenido en Wikipedia? ¿Los scripts automatizados o las personas?

```sql
SELECT 
    CASE WHEN bot THEN '🤖 Bots Automatizados' ELSE '👤 Editores Humanos' END AS categoria,
    COUNT(*) AS total_ediciones,
    ROUND(AVG(byte_diff), 2) AS promedio_bytes_por_edicion,
    MAX(byte_diff) AS edicion_mas_grande_agregada,
    MIN(byte_diff) AS edicion_mas_grande_eliminada
FROM wiki_recent_changes
GROUP BY bot;
```

### 🔍 Interpretación analítica:
* Notarás que los **Bots** realizan una cantidad descomunal de ediciones pequeñas y estandarizadas (traducciones de enlaces, adición de categorías).
* Los **Humanos** suelen tener una varianza mucho más amplia en `byte_diff` (redacción de párrafos enteros o borrado de secciones).

---

## 4. Clasificación de Eventos (`change_type`)

### Consulta: Distribución por tipo de acción
```sql
SELECT 
    COALESCE(change_type, 'desconocido') AS tipo_accion,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM wiki_recent_changes
GROUP BY change_type
ORDER BY total DESC;
```
*(Generalmente `edit` representa >80%, seguido de `categorize`, `new` y `log`).*

---

## 5. Detección de Anomalías y Vandalismo Potencial

En ingeniería de datos y ciberseguridad, detectar anomalías súbitas en streaming es vital.

### Consulta: Ediciones con impacto masivo (agregados o eliminaciones de más de 5,000 caracteres)
```sql
SELECT 
    id,
    wiki,
    title,
    user_name,
    bot,
    byte_diff,
    event_timestamp
FROM wiki_recent_changes
WHERE ABS(byte_diff) > 5000
ORDER BY ABS(byte_diff) DESC
LIMIT 10;
```
*Si un usuario no bot tiene un `byte_diff = -35000`, es muy probable que haya vaciado un artículo completo (vandalismo o depuración).*

---

## 6. Velocidad de Ingestión (Agrupación Temporal en Ventanas)

### Consulta: Tasa de eventos por minuto durante los últimos 10 minutos
```sql
SELECT 
    DATE_TRUNC('minute', received_at) AS minuto_ingesta,
    COUNT(*) AS eventos_procesados,
    ROUND(COUNT(*) / 60.0, 2) AS eventos_por_segundo_promedio
FROM wiki_recent_changes
WHERE received_at >= NOW() - INTERVAL '10 minutes'
GROUP BY DATE_TRUNC('minute', received_at)
ORDER BY minuto_ingesta DESC;
```
*Esta consulta te muestra el rendimiento exacto de tu pipeline en ventanas temporales de streaming.*
