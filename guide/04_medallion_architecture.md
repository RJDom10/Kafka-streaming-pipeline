# Arquitectura Medallion: Capas Bronce, Plata y Oro

**Documento:** `04_medallion_architecture.md`
**Objetivo:** Entender el patron de arquitectura Medallion (Bronze, Silver, Gold), como organiza los datos en capas de calidad creciente y donde se ubica el proyecto actual dentro de este patron.

---

## 1. Que es la Arquitectura Medallion

La **Arquitectura Medallion** (o Medallion Architecture) es un patron de diseno para organizar datos en un Data Lakehouse. Fue popularizado por **Databricks** y es hoy el estandar de facto en la industria para organizar pipelines de datos modernos.

El nombre viene de la metafora de las medallas olimpicas: los datos van mejorando de calidad en cada capa, como pasar de bronce a plata y finalmente a oro.

### La Premisa Central

Los datos crudos raramente son utiles directamente. Necesitan pasar por un proceso de:
1. **Ingestion** (datos crudos, sin procesar)
2. **Limpieza y validacion** (datos limpios y confiables)
3. **Transformacion y enriquecimiento** (datos listos para consumo de negocio)

---

## 2. Las Tres Capas

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     ARQUITECTURA MEDALLION                              │
│                                                                         │
│  Fuente         BRONCE            PLATA              ORO                │
│  (Raw)           (Bronze)          (Silver)           (Gold)            │
│                                                                         │
│  Wikimedia  ──>  Datos crudos  ──>  Datos limpios ──>  Datos de negocio│
│  Eventos         tal como          validados y         agregados y      │
│  SSE             llegan           normalizados         optimizados      │
│                                                                         │
│  Calidad: Baja ──────────────────────────────────────> Alta            │
│  Latencia: Baja ─────────────────────────────────────> Alta            │
│  Granularidad: Alta ─────────────────────────────────> Baja            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Capa Bronce (Bronze Layer) — "Raw / Landing Zone"

### Definicion

La capa Bronce es la **zona de aterrizaje de los datos crudos**. Se almacenan los datos exactamente como llegan de la fuente, sin ninguna transformacion. Es el "registro oficial" e inmutable de lo que ocurrio.

### Caracteristicas
- **Inmutabilidad:** Los datos nunca se modifican una vez ingeridos
- **Completitud:** Se almacena TODO, incluso datos incorrectos o duplicados
- **Formato:** Generalmente el formato original (JSON, CSV, Avro, Parquet crudo)
- **Sin esquema rigido:** No se aplican restricciones estrictas de tipos de datos
- **Retencion larga:** Se guarda por meses o anos para auditorias y reprocesamiento

### Que contendria Bronce en nuestro proyecto

```json
// Evento crudo de Wikimedia (tal como llega):
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "uri": "https://en.wikipedia.org/wiki/Python",
    "request_id": "abc123",
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "dt": "2026-09-13T23:00:00Z",
    "domain": "en.wikipedia.org",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 5678901234
  },
  "id": 1234567890,
  "type": "edit",
  "namespace": 0,
  "title": "Python (programming language)",
  "comment": "/* History */ corrected date",
  "timestamp": 1726271400,
  "user": "AliceContributor",
  "bot": false,
  "minor": false,
  "patrolled": true,
  "length": {"old": 82000, "new": 82050},
  "revision": {"old": 1234567, "new": 1234568},
  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",
  "server_script_path": "/w",
  "wiki": "enwiki",
  "parsedcomment": "<i>History</i> corrected date"
}
```

**En nuestro proyecto actual:** La capa Bronce seria Kafka mismo. Los mensajes en el topico `wiki.changes` son los datos mas crudos del pipeline (aunque ya estan ligeramente filtrados por `sanitize_event()`).

### Tecnologias tipicas para Bronce
- Apache Kafka (retencion de 7-30 dias)
- Amazon S3 / Azure Data Lake / Google Cloud Storage
- Apache Iceberg (formato de tabla abierto sobre object storage)
- Delta Lake (formato de Databricks)

---

## 4. Capa Plata (Silver Layer) — "Cleansed / Enriched"

### Definicion

La capa Plata es donde los datos crudos se **limpian, validan, normalizan y enriquecen**. Es la capa de confianza: si alguien en la organizacion quiere datos confiables para hacer analytics, los toma de aqui.

### Caracteristicas
- **Validacion de esquema:** Los tipos de datos son correctos y consistentes
- **Deduplicacion:** Se eliminan eventos duplicados
- **Normalizacion:** Formatos estandarizados (fechas en ISO 8601, strings en UTF-8)
- **Enriquecimiento:** Se unen con otras fuentes (ej: datos demograficos del usuario)
- **Particionado:** Organizado por fecha/region para consultas eficientes

### Que contendria Plata en nuestro proyecto

```sql
-- Tabla Plata: wiki_changes_clean
CREATE TABLE silver.wiki_changes_clean (
    event_id        VARCHAR(50) NOT NULL,  -- Deduplicacion por ID unico
    wiki            VARCHAR(50) NOT NULL,
    wiki_language   VARCHAR(10),           -- Enriquecimiento: "en", "es", "fr"
    wiki_project    VARCHAR(50),           -- Enriquecimiento: "wikipedia", "wiktionary"
    title           TEXT NOT NULL,
    title_normalized TEXT,                 -- Titulo sin caracteres especiales
    user_name       VARCHAR(255),
    user_type       VARCHAR(20),           -- Enriquecimiento: "registered", "anonymous", "bot"
    is_bot          BOOLEAN NOT NULL DEFAULT FALSE,
    change_type     VARCHAR(50) NOT NULL,
    namespace_id    INT,                   -- 0=articulo, 1=discusion, etc.
    namespace_name  VARCHAR(100),
    length_old      INT NOT NULL DEFAULT 0,
    length_new      INT NOT NULL DEFAULT 0,
    byte_diff       INT NOT NULL,
    is_revert       BOOLEAN DEFAULT FALSE, -- Enriquecimiento: es una reversion?
    event_date      DATE NOT NULL,         -- Particion por fecha
    event_hour      SMALLINT NOT NULL,     -- Particion por hora
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_date);        -- Particionado por fecha para eficiencia
```

**En nuestro proyecto actual:** La tabla `wiki_recent_changes` en PostgreSQL es un hibrido entre Bronce y Plata. Tiene algo de limpieza (sanitize_event) pero no tiene deduplicacion, enriquecimiento ni particionado por fecha.

---

## 5. Capa Oro (Gold Layer) — "Business / Aggregated"

### Definicion

La capa Oro es donde los datos se **agregan, transforman y optimizan para casos de uso especificos** de negocio. Es la capa que consumen directamente los dashboards, modelos de ML, APIs y reportes ejecutivos.

### Caracteristicas
- **Pre-agregados:** Los calculos costosos ya estan hechos (COUNT, SUM, AVG por dia/wiki/usuario)
- **Desnormalizados:** Optimizados para lectura rapida, no para almacenamiento eficiente
- **Especificos por dominio:** Una tabla de Oro para analytics, otra para ML, otra para el dashboard
- **Actualizacion periodica:** Por batch (cada hora/dia) o por micro-batch (cada minuto)

### Que contendria Oro en nuestro proyecto

```sql
-- Tabla Oro 1: Estadisticas por Wiki y Dia (para dashboards)
CREATE TABLE gold.wiki_daily_stats (
    event_date      DATE NOT NULL,
    wiki            VARCHAR(50) NOT NULL,
    wiki_language   VARCHAR(10),
    wiki_project    VARCHAR(50),
    total_edits     BIGINT NOT NULL,
    human_edits     BIGINT NOT NULL,
    bot_edits       BIGINT NOT NULL,
    bot_pct         DECIMAL(5,2),
    unique_editors  INT NOT NULL,
    net_bytes_added BIGINT NOT NULL,
    avg_edit_size   DECIMAL(10,2),
    new_articles    INT NOT NULL,
    reverted_edits  INT NOT NULL,
    PRIMARY KEY (event_date, wiki)
);

-- Tabla Oro 2: Top Editores por Semana (para ranking)
CREATE TABLE gold.weekly_top_editors (
    week_start      DATE NOT NULL,
    wiki            VARCHAR(50) NOT NULL,
    user_name       VARCHAR(255) NOT NULL,
    total_edits     INT NOT NULL,
    net_bytes       INT NOT NULL,
    rank_in_wiki    INT NOT NULL,
    PRIMARY KEY (week_start, wiki, user_name)
);

-- Tabla Oro 3: Features para ML (prediccion de vandalismos)
CREATE TABLE gold.ml_edit_features (
    event_id            VARCHAR(50) PRIMARY KEY,
    user_edit_count     INT,        -- Cuantas ediciones tiene este usuario
    user_age_days       INT,        -- Hace cuantos dias se registro
    article_edit_count  INT,        -- Cuantas veces fue editado este articulo
    byte_diff_zscore    DECIMAL,    -- Anomalia estadistica en el cambio de bytes
    edit_hour           SMALLINT,   -- Hora del dia (para patrones temporales)
    is_vandalism        BOOLEAN,    -- Etiqueta (para entrenamiento supervisado)
    predicted_vandalism DECIMAL     -- Score del modelo ML
);
```

---

## 6. El Proyecto Actual: Donde Estamos en el Medallion

```
ARQUITECTURA MEDALLION — ESTADO ACTUAL DEL PROYECTO

FUENTE       BRONCE              PLATA              ORO
Wikimedia ──> KAFKA             (NO implementado)  (NO implementado)
SSE           wiki.changes      wiki_changes_clean  wiki_daily_stats
              (eventos crudos                        weekly_top_editors
              sanitizados)                           ml_edit_features
                 |
                 v
              POSTGRESQL
              wiki_recent_changes
              (hibrido Bronce/Plata
               muy basico)

NIVEL ACTUAL: Entre Bronce y Plata Inicial
```

### Lo que tenemos implementado:
- **Ingesta en tiempo real** desde Wikimedia (fuente)
- **Buffer desacoplado** con Kafka (pre-Bronce)
- **Almacenamiento inicial** en PostgreSQL (Bronce rudimentario)
- **Sanitizacion basica** (sanitize_event = limpieza muy minima)
- **Indices de busqueda** (optimizacion basica de Plata)

### Lo que falta para completar cada capa:

| Capa | Que falta |
|---|---|
| **Bronce completo** | Guardar el evento crudo COMPLETO de Wikimedia en Apache Iceberg/S3 |
| **Plata** | Deduplicacion, enriquecimiento (idioma, tipo de proyecto), particionado por fecha |
| **Oro** | Tablas pre-agregadas para dashboards, features para ML, vistas materializadas |

---

## 7. Implementacion Futura: Stack Medallion Completo

```
ARQUITECTURA MEDALLION FUTURA

Wikimedia SSE
     |
     v
[PRODUCER Python]
     |
     v (stream en tiempo real)
[KAFKA - wiki.changes]
     |
     ├──> [Consumer Bronce] ──> Apache Iceberg (S3/MinIO)
     |                          /bronce/wiki_changes_raw/
     |                          year=2026/month=09/day=13/
     |                          part-00000.parquet
     |
     └──> [Consumer Plata - PySpark Structured Streaming]
                |
                v
          Limpieza + Validacion + Enriquecimiento
                |
                v
          Apache Iceberg (S3/MinIO)
          /plata/wiki_changes_clean/
          event_date=2026-09-13/
          part-00000.parquet
                |
                v
     [Job Batch diario - Spark]
                |
                v
          Apache Iceberg (S3/MinIO)
          /oro/wiki_daily_stats/
          /oro/weekly_top_editors/
          /oro/ml_edit_features/
                |
                v
     [Serving Layer - Apache Superset]
          (Dashboards interactivos)
```

---

## 8. Beneficios del Patron Medallion

| Beneficio | Descripcion |
|---|---|
| **Rastreabilidad** | Si hay un error en Oro, puedes trazar hacia atras hasta Bronce |
| **Reprocesamiento** | Puedes re-ejecutar la transformacion Bronce->Plata si hay un bug |
| **Gobernanza** | Cada capa tiene controles de calidad claros |
| **Performance** | Oro esta optimizado para consultas especificas |
| **Flexibilidad** | Diferentes equipos consumen diferentes capas segun sus necesidades |
| **Costos** | Bronce en almacenamiento barato (S3); Oro en cache rapido (Redis) |
