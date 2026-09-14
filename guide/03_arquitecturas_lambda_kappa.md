# Arquitecturas Lambda y Kappa: Teoria, Comparativa y Aplicacion

**Documento:** `03_arquitecturas_lambda_kappa.md`
**Objetivo:** Entender las dos grandes arquitecturas de referencia para sistemas de datos a gran escala, sus trade-offs y como este proyecto se ubica en ese contexto.

---

## 1. El Problema que Ambas Resuelven

Antes de explorar las arquitecturas, hay que entender el problema que ambas intentan resolver:

### El Problema Fundamental del Big Data

Cuando los volumenes de datos son masivos y llegan continuamente, surgen dos necesidades que entran en conflicto:

1. **Baja Latencia (Speed):** Los usuarios quieren resultados en segundos (ej: dashboard en tiempo real con las ediciones de Wikipedia del ultimo minuto).

2. **Alta Precision (Accuracy):** Los reportes historicos deben ser exactos y completos (ej: cuantas ediciones tuvo Wikipedia en total durante Septiembre 2026).

**El dilema:** Los sistemas rapidos suelen ser aproximados; los sistemas precisos suelen ser lentos.

---

## 2. Arquitectura Lambda

### 2.1 Historia y Origen

La Arquitectura Lambda fue propuesta por **Nathan Marz** (creador de Apache Storm) en su libro "Big Data" (2015). Su nombre viene de la letra griega lambda (λ) que representa la estructura de capas paralelas.

### 2.2 Principio Fundamental

Lambda propone resolver el dilema velocidad/precision con **tres capas paralelas**:

```
                    ┌─────────────────────────────────────┐
                    │           DATOS CRUDOS              │
                    │    (stream continuo de eventos)     │
                    └────────────┬────────────┬───────────┘
                                 │            │
                    ┌────────────▼────────┐  ┌▼──────────────────┐
                    │    BATCH LAYER      │  │    SPEED LAYER     │
                    │   (Capa de Lote)    │  │  (Capa de Velocidad)│
                    │                     │  │                    │
                    │  Procesa TODO el    │  │ Procesa solo los   │
                    │  historial de datos │  │ ULTIMOS minutos/   │
                    │  con alta precision │  │ horas en tiempo    │
                    │  (horas/dias)       │  │ real (segundos)    │
                    │                     │  │                    │
                    │  Tools: Spark,      │  │ Tools: Kafka,      │
                    │  Hadoop, Hive       │  │ Storm, Flink       │
                    └────────────┬────────┘  └──────┬────────────┘
                                 │                  │
                    ┌────────────▼──────────────────▼────────────┐
                    │              SERVING LAYER                  │
                    │          (Capa de Servicio)                 │
                    │                                             │
                    │  Combina resultados del batch (historico)   │
                    │  con resultados del speed (reciente)        │
                    │  para responder queries con baja latencia   │
                    │                                             │
                    │  Tools: HBase, Cassandra, ElasticSearch     │
                    └─────────────────────────────────────────────┘
```

### 2.3 Las Tres Capas en Detalle

#### Batch Layer (Capa de Lote)
- **Que hace:** Procesa el historial completo de datos periodicamente (cada hora, dia, semana)
- **Caracteristica clave:** Alta precision, pero alta latencia
- **Ejemplo en nuestro contexto:** Cada noche a las 2am, un job de Spark lee todos los eventos de Wikimedia del dia y calcula estadisticas definitivas por wiki, usuario, tipo de cambio
- **Tecnologias tipicas:** Apache Spark, Apache Hadoop (MapReduce), Hive

#### Speed Layer (Capa de Velocidad)
- **Que hace:** Procesa los datos MAS RECIENTES en tiempo real para dar respuestas inmediatas
- **Caracteristica clave:** Baja latencia, pero datos aproximados/parciales
- **Ejemplo:** Muestra cuantas ediciones hubo en los ultimos 5 minutos (aproximado, sin el procesamiento completo)
- **Tecnologias tipicas:** Apache Kafka, Apache Flink, Apache Storm

#### Serving Layer (Capa de Servicio)
- **Que hace:** Almacena los resultados pre-calculados del batch layer y los complementa con los del speed layer para responder queries rapidamente
- **Ejemplo:** Un dashboard que muestra "ediciones de hoy" (speed) + "estadisticas del mes" (batch)
- **Tecnologias tipicas:** Apache HBase, Cassandra, Redis, Elasticsearch

### 2.4 Ejemplo Practico: Wikipedia Analytics

```
Pregunta: "Cuantas ediciones hubo en enwiki en los ultimos 30 dias?"

Batch Layer (procesamiento nocturno):
  - Resultado: exacto, definitivo, pero de ayer: 1,234,567 ediciones

Speed Layer (tiempo real):
  - Resultado: ediciones de hoy hasta ahora: +23,456 ediciones

Serving Layer combina:
  - Total = 1,234,567 (batch) + 23,456 (speed) = ~1,258,023
  - Latencia de la query: < 100ms
```

### 2.5 Ventajas y Desventajas de Lambda

| Ventajas | Desventajas |
|---|---|
| Combina precision historica con velocidad en tiempo real | Mantener dos pipelines (batch + streaming) es muy complejo |
| Tolerante a fallos: si el speed layer falla, el batch lo corrige | La misma logica de negocio debe implementarse dos veces |
| Maduro y probado en produccion | Mayor costo operacional (mas infraestructura) |
| Permite reprocesar el historico si hay un bug | Mayor riesgo de inconsistencias entre capas |
| Excelente para analytics historico + tiempo real | Debugging mas complejo |

---

## 3. Arquitectura Kappa

### 3.1 Historia y Origen

La Arquitectura Kappa fue propuesta por **Jay Kreps** (cocreador de Kafka, CEO de Confluent) en 2014 como una critica y simplificacion de Lambda. Su argumento central: **¿Por que mantener dos sistemas si puedes hacer todo con streaming?**

El nombre viene de la letra griega kappa (κ).

### 3.2 Principio Fundamental

Kappa tiene una premisa radical: **TODO es un stream**. No hay distincion entre batch y streaming; el procesamiento en lote es simplemente un caso especial de streaming (un stream con inicio y fin definidos).

```
                    ┌─────────────────────────────────────┐
                    │           DATOS CRUDOS              │
                    │    (stream continuo de eventos)     │
                    └──────────────────┬──────────────────┘
                                       │
                    ┌──────────────────▼──────────────────┐
                    │         STREAM PROCESSING LAYER     │
                    │                                     │
                    │  Una SOLA capa de procesamiento     │
                    │  que maneja TANTO tiempo real       │
                    │  COMO reprocesamiento historico     │
                    │                                     │
                    │  El "batch" = leer el stream desde  │
                    │  el principio (offset 0) con un     │
                    │  nuevo consumer group               │
                    │                                     │
                    │  Tools: Kafka + Flink / Spark       │
                    │         Streaming / ksqlDB          │
                    └──────────────────┬──────────────────┘
                                       │
                    ┌──────────────────▼──────────────────┐
                    │           SERVING LAYER             │
                    │   (misma capa que en Lambda)        │
                    └─────────────────────────────────────┘
```

### 3.3 Como se Hace el "Reprocesamiento" en Kappa

En Kappa, si hay un bug en la logica de procesamiento:
1. Corriges el codigo del stream processor
2. Creas un nuevo consumer group que empieza a leer desde offset 0
3. El nuevo consumer procesa TODO el historial con la logica corregida
4. Cuando alcanza el offset actual, reemplazas el consumer viejo por el nuevo

```
Kafka Log (retencion: 30 dias):
Offset: 0 ─────────────────────────────────────────> N

Version 1 del consumer (con bug):
  Lee desde offset 5000 hasta N (solo datos recientes)

Version 2 del consumer (corregida):
  Nuevo consumer group empieza en offset 0
  Lee TODOS los datos historicos con la logica correcta
  Cuando llega a N, reemplaza a Version 1
```

### 3.4 Ventajas y Desventajas de Kappa

| Ventajas | Desventajas |
|---|---|
| Una sola capa: menos codigo, menos complejidad | El stream processor debe ser capaz de manejar datos historicos masivos |
| La logica de negocio se implementa una sola vez | Requiere retener el historial completo en Kafka (caro en storage) |
| Kafka actua como "fuente de verdad" del historial | Menor madurez que Lambda para casos de uso muy complejos |
| Mas facil de mantener y hacer debugging | No siempre es posible "repetir" el stream si los datos cambian |
| Menor costo operacional (un sistema menos) | Algunos calculos batch complejos son dificiles de expresar como streaming |

---

## 4. Lambda vs Kappa: Comparativa Directa

| Criterio | Lambda | Kappa |
|---|---|---|
| Numero de pipelines | 2 (batch + speed) | 1 (streaming) |
| Complejidad | Alta | Media |
| Tolerancia a fallos | Alta (batch corrige errores del speed) | Media (el stream debe ser robusto) |
| Latencia de resultados | Segundos (speed) + horas (batch) | Segundos |
| Precision historica | Exacta (batch recalcula) | Exacta (reprocesamiento en stream) |
| Costo de storage | Menor (datos historicos en HDFS/S3 comprimidos) | Mayor (Kafka retiene todo el historial) |
| Cuando usarla | Analytics complejo con historial masivo | Cuando todo puede modelarse como streaming |
| Empresas que la usan | Netflix, Uber (historicamente) | LinkedIn, Confluent, muchas startups |

---

## 5. Donde Esta Este Proyecto en Estas Arquitecturas

### Posicion Actual

```
ARQUITECTURA KAPPA SIMPLIFICADA
        |
        v

Wikimedia SSE ──> [Producer] ──> [Kafka] ──> [Consumer] ──> [PostgreSQL]
   (fuente)          |           (stream         |           (serving)
                  Speed Layer     central)     Speed Layer
```

El proyecto actual implementa la **Speed Layer de una Arquitectura Kappa**:
- Kafka es el log central (fuente de verdad)
- El consumer es el stream processor (simple, sin transformaciones complejas)
- PostgreSQL es el serving layer

### Que Falta para una Kappa Completa

```
Kafka (retencion extendida: 30+ dias)
    |
    ├──> Consumer actual (tiempo real -> PostgreSQL)
    |
    └──> Consumer de reprocesamiento (nuevo group, desde offset 0)
              |
              v
         Apache Flink / Spark Streaming
              |
              v
         Apache Iceberg (almacenamiento optimizado)
              |
              v
         Serving Layer avanzado (Trino / Presto para SQL)
```

---

## 6. Cuándo Elegir Cada Arquitectura

### Usa Lambda cuando:
- Tienes datos historicos masivos que ya existen en un data warehouse
- Necesitas analytics complejos que son dificiles de expresar en streaming
- Tu equipo tiene expertise en batch processing (Spark/Hive)
- El negocio tolera que los resultados exactos lleguen con horas de delay

### Usa Kappa cuando:
- Tu caso de uso es principalmente streaming/tiempo real
- Quieres simplificar la arquitectura (menos sistemas = menos problemas)
- Puedes retener suficiente historial en Kafka (o usar tiered storage)
- El procesamiento puede expresarse naturalmente como transformaciones de streams

### La tendencia actual (2024-2026):
La industria se esta moviendo hacia Kappa enriquecida con **Apache Iceberg** como capa de almacenamiento, que permite:
- Retener historial masivo eficientemente (no en Kafka sino en object storage)
- Consultas SQL eficientes sobre datos historicos
- Time travel: consultar como eran los datos en cualquier punto del pasado
