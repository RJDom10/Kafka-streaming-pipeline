# 📄 Documentación: `init.sql`

**Archivo:** `init.sql`
**Propósito:** Script SQL de inicialización de la base de datos. Se ejecuta **automáticamente una sola vez** cuando el contenedor de PostgreSQL se levanta por primera vez. Define el esquema (estructura) de la base de datos: la tabla principal y sus índices de optimización.

---

## ¿Cómo se ejecuta este archivo?

En `docker-compose.yml`, el servicio `postgres` tiene este volumen montado:
```yaml
- ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
```
PostgreSQL tiene un mecanismo especial: al arrancar por primera vez (cuando el directorio de datos está vacío), ejecuta automáticamente **en orden alfabético** todos los archivos `.sql` y `.sh` que encuentre dentro de `/docker-entrypoint-initdb.d/`. El sufijo `:ro` (read-only) evita que el contenedor modifique el archivo original.

> ⚠️ Si el volumen `postgres_data` ya existe de un arranque anterior, PostgreSQL NO ejecutará estos scripts de nuevo. Para reinicializar desde cero: `docker compose down -v` (elimina el volumen).

---

## Análisis Línea por Línea

### Creación de la Tabla Principal

```sql
CREATE TABLE IF NOT EXISTS wiki_recent_changes (
```
- `CREATE TABLE`: Instrucción SQL para crear una nueva tabla.
- `IF NOT EXISTS`: Cláusula de seguridad. Si la tabla ya existe (por algún motivo), no lanza error, simplemente no hace nada. Hace el script **idempotente**.
- `wiki_recent_changes`: Nombre descriptivo de la tabla. Sigue la convención `snake_case` estándar en PostgreSQL.

---

### Columnas de la Tabla

```sql
    id SERIAL PRIMARY KEY,
```
- `id`: Columna identificadora única.
- `SERIAL`: Tipo de dato especial en PostgreSQL (equivale a `INTEGER` con una secuencia autoincremental). Cada nuevo registro recibe automáticamente el siguiente número entero disponible (1, 2, 3...). No necesitas incluir este campo en el INSERT.
- `PRIMARY KEY`: Restricción que garantiza dos cosas: (1) el valor es único en toda la tabla y (2) no puede ser NULL. PostgreSQL crea automáticamente un índice B-tree en la clave primaria para búsquedas eficientes por `id`.

```sql
    wiki VARCHAR(50) NOT NULL,
```
- `wiki`: Identifica qué proyecto Wikimedia generó el evento. Ejemplos: `"enwiki"` (Wikipedia en inglés), `"eswiki"` (español), `"wikidata"`, `"commonswiki"`.
- `VARCHAR(50)`: String de longitud variable, máximo 50 caracteres. Suficiente para cualquier nombre de wiki.
- `NOT NULL`: Esta columna es obligatoria; no puede insertarse un registro sin este valor.

```sql
    title TEXT,
```
- `title`: Título de la página editada. Puede ser muy largo (artículos con nombres extensos), por eso se usa `TEXT` (longitud ilimitada) en lugar de `VARCHAR`.
- Sin `NOT NULL`: es nullable porque algunos tipos de cambios pueden no tener título.

```sql
    user_name VARCHAR(255),
```
- `user_name`: Nombre de usuario o dirección IP del editor. `VARCHAR(255)` es el máximo para nombres de usuario en Wikimedia.

```sql
    bot BOOLEAN DEFAULT FALSE,
```
- `bot`: Indicador de si el cambio fue realizado por un bot automatizado (True) o un humano (False).
- `DEFAULT FALSE`: Si no se especifica este campo en el INSERT, PostgreSQL asigna `FALSE` automáticamente.

```sql
    change_type VARCHAR(50),
```
- `change_type`: Tipo de cambio. Valores posibles del stream de Wikimedia: `"edit"` (edición), `"new"` (página nueva), `"log"` (acción de log), `"categorize"` (cambio de categoría).

```sql
    length_old INT,
    length_new INT,
```
- Tamaño de la página en bytes antes y después del cambio. `INT` (entero de 4 bytes, hasta 2.1 millones) es suficiente para el tamaño de páginas de Wikipedia.

```sql
    byte_diff INT,
```
- Diferencia neta en bytes: `length_new - length_old`. Valor positivo = se agregó contenido, negativo = se eliminó contenido. Este campo es **calculado** en el producer y guardado para evitar recalcularlo en cada consulta analítica.

```sql
    event_timestamp TIMESTAMPTZ,
```
- Timestamp del evento según Wikimedia.
- `TIMESTAMPTZ`: Timestamp **con zona horaria** (TimeStamp With Time Zone). PostgreSQL lo almacena internamente en UTC y lo convierte a la zona horaria local al consultar. Es la mejor práctica para timestamps en sistemas distribuidos globales.

```sql
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
```
- Timestamp de cuándo fue insertado el registro en la base de datos (no cuándo ocurrió el evento).
- `DEFAULT CURRENT_TIMESTAMP`: PostgreSQL llena automáticamente este campo con la hora actual del servidor al hacer el INSERT. No necesitas incluirlo en el INSERT.
- Esta columna es útil para medir la **latencia del pipeline**: `received_at - event_timestamp` te dice cuánto tiempo tardó el evento en llegar desde Wikimedia hasta tu base de datos.

---

### Índices de Optimización

```sql
CREATE INDEX IF NOT EXISTS idx_wiki_timestamp ON wiki_recent_changes (event_timestamp);
```
- **Índice sobre `event_timestamp`**: Optimiza queries con filtros de rango temporal como:
  ```sql
  WHERE event_timestamp BETWEEN '2026-09-13 00:00' AND '2026-09-13 23:59'
  ORDER BY event_timestamp DESC
  ```
  Sin índice: PostgreSQL lee todos los registros (Sequential Scan). Con índice B-tree: accede directamente a los registros del rango (Index Scan). La diferencia puede ser de segundos vs. milisegundos con millones de filas.

```sql
CREATE INDEX IF NOT EXISTS idx_wiki_bot ON wiki_recent_changes (bot);
```
- **Índice sobre `bot`**: Optimiza queries de análisis estadístico como:
  ```sql
  SELECT COUNT(*) FROM wiki_recent_changes WHERE bot = TRUE;
  SELECT wiki, COUNT(*) FROM wiki_recent_changes WHERE bot = FALSE GROUP BY wiki;
  ```
  Es un índice de baja cardinalidad (solo 2 valores: true/false). En tablas grandes (millones de filas) PostgreSQL puede decidir si usar el índice o no dependiendo del % de filas que cumplen el filtro.

```sql
CREATE INDEX IF NOT EXISTS idx_wiki_lang ON wiki_recent_changes (wiki);
```
- **Índice sobre `wiki`**: Optimiza queries de filtrado por idioma/proyecto:
  ```sql
  SELECT * FROM wiki_recent_changes WHERE wiki = 'enwiki' LIMIT 100;
  SELECT wiki, COUNT(*) FROM wiki_recent_changes GROUP BY wiki ORDER BY COUNT(*) DESC;
  ```

---

## Esquema Resultante

```
wiki_recent_changes
┌─────────────────────┬─────────────┬─────────────────────────────────────┐
│ Columna             │ Tipo        │ Descripción                         │
├─────────────────────┼─────────────┼─────────────────────────────────────┤
│ id (PK)             │ SERIAL      │ ID autoincremental único             │
│ wiki                │ VARCHAR(50) │ Proyecto Wikimedia (enwiki, eswiki) │
│ title               │ TEXT        │ Título de la página                 │
│ user_name           │ VARCHAR(255)│ Usuario o IP del editor             │
│ bot                 │ BOOLEAN     │ True si fue un bot                  │
│ change_type         │ VARCHAR(50) │ edit / new / log / categorize       │
│ length_old          │ INT         │ Bytes antes del cambio              │
│ length_new          │ INT         │ Bytes después del cambio            │
│ byte_diff           │ INT         │ Diferencia neta en bytes            │
│ event_timestamp     │ TIMESTAMPTZ │ Cuándo ocurrió el evento            │
│ received_at         │ TIMESTAMPTZ │ Cuándo llegó a la BD (auto)         │
└─────────────────────┴─────────────┴─────────────────────────────────────┘

Índices:
  - PRIMARY KEY → id (B-tree, único)
  - idx_wiki_timestamp → event_timestamp (B-tree, rangos temporales)
  - idx_wiki_bot → bot (B-tree, filtros booleanos)
  - idx_wiki_lang → wiki (B-tree, filtros por idioma)
```

---

## Queries de Ejemplo para el Laboratorio

```sql
-- ¿Cuántos cambios hubo en los últimos 5 minutos?
SELECT COUNT(*) FROM wiki_recent_changes
WHERE received_at >= NOW() - INTERVAL '5 minutes';

-- Top 10 wikis con más cambios
SELECT wiki, COUNT(*) as total
FROM wiki_recent_changes
GROUP BY wiki ORDER BY total DESC LIMIT 10;

-- ¿Qué % de los cambios son bots?
SELECT
    SUM(CASE WHEN bot = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_bots
FROM wiki_recent_changes;

-- Latencia del pipeline (tiempo Wikimedia → BD)
SELECT
    AVG(EXTRACT(EPOCH FROM (received_at - event_timestamp))) AS avg_latency_seconds
FROM wiki_recent_changes;
```
