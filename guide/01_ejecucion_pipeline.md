# Guia de Ejecucion del Pipeline

**Documento:** `01_ejecucion_pipeline.md`
**Objetivo:** Guia completa paso a paso para levantar, verificar y operar el pipeline de streaming Kafka-Wikimedia-PostgreSQL en un entorno local con Docker.

---

## Prerequisitos del Sistema

### 1. Verificar Docker y Docker Compose

```bash
# Verificar version de Docker (necesitas >= 24.0)
docker --version
# Ejemplo de salida: Docker version 27.5.1, build 9f9e405

# Verificar Docker Compose (necesitas >= 2.24)
docker compose version
# Ejemplo de salida: Docker Compose version v2.31.0

# Verificar que el daemon de Docker esta corriendo
docker info | grep "Server Version"
```

### 2. Verificar Recursos Disponibles

```bash
# Ver RAM disponible (necesitas al menos 3 GB libres)
free -h

# Ver espacio en disco (necesitas al menos 3 GB para las imagenes)
df -h /

# Ver cuantos contenedores estan corriendo actualmente
docker ps
```

### 3. Verificar Conectividad a Wikimedia

```bash
# Test de conexion al stream de Wikimedia
curl -s --max-time 5 https://stream.wikimedia.org/v2/stream/recentchange | head -c 200
```
Si ves una respuesta con `event:` o `data:`, la conexion funciona.

---

## Estructura del Proyecto

```
lab_kafka_test/
├── docker-compose.yml     <- Orquestador de todos los servicios
├── Dockerfile             <- Imagen Python compartida
├── requirements.txt       <- Dependencias Python
├── producer.py            <- Captura eventos de Wikimedia y los publica en Kafka
├── consumer.py            <- Lee de Kafka e inserta en PostgreSQL
├── init.sql               <- Schema inicial de la base de datos
├── docs/                  <- Documentacion detallada de cada archivo
└── guide/                 <- Esta guia tecnica (lectura actual)
```

---

## Paso 1: Clonar / Preparar el Proyecto

```bash
# Si ya tienes el proyecto, navega al directorio raiz
cd ~/lab_kafka_test

# Verificar que todos los archivos necesarios estan presentes
ls -la
# Debes ver: docker-compose.yml, Dockerfile, producer.py, consumer.py, init.sql, requirements.txt
```

---

## Paso 2: Construir las Imagenes Docker

```bash
# Construye la imagen personalizada de Python (producer + consumer)
# La primera vez toma 2-4 minutos por la descarga de dependencias
docker compose build

# Verificar que las imagenes se construyeron correctamente
docker images | grep lab_kafka_test
# Debes ver: lab_kafka_test-producer y lab_kafka_test-consumer
```

**Que hace este paso:**
- Descarga la imagen base `python:3.11-slim` (~130 MB)
- Instala `confluent-kafka`, `psycopg2-binary`, `requests`
- Copia `producer.py` y `consumer.py` a la imagen

---

## Paso 3: Levantar Todo el Stack

```bash
# Levanta todos los servicios en segundo plano (-d = detached)
docker compose up -d

# Alternativa: con build en el mismo comando
docker compose up --build -d
```

### Orden de Arranque (gestionado automaticamente por depends_on):

```
t=0s   -> kafka arranca (KRaft: broker + controller en un proceso)
t=10s  -> kafka HEALTHY (pasa el healthcheck)
t=10s  -> postgres arranca (ejecuta init.sql: CREATE TABLE + CREATE INDEX)
t=10s  -> init-kafka arranca (crea el topico wiki.changes con 3 particiones)
t=10s  -> kafka-ui arranca (http://localhost:8080)
t=15s  -> postgres HEALTHY
t=15s  -> init-kafka DONE (exit 0)
t=15s  -> producer arranca (conecta a Wikimedia SSE)
t=15s  -> consumer arranca (se suscribe a wiki.changes)
```

---

## Paso 4: Verificar el Estado de los Servicios

```bash
# Ver estado de todos los contenedores
docker compose ps

# Salida esperada:
# NAME              STATUS                   PORTS
# lab1-consumer     Up X minutes
# lab1-init-kafka   Exited (0) ...           <- CORRECTO: se apaga al terminar
# lab1-kafka        Up X minutes (healthy)   0.0.0.0:9092->9092/tcp
# lab1-kafka-ui     Up X minutes             0.0.0.0:8080->8080/tcp
# lab1-postgres     Up X minutes (healthy)   0.0.0.0:5432->5432/tcp
# lab1-producer     Up X minutes
```

### Verificaciones Criticas:
- `lab1-kafka`: debe decir **(healthy)** — si dice `starting`, espera 30 segundos mas
- `lab1-postgres`: debe decir **(healthy)**
- `lab1-init-kafka`: debe decir **Exited (0)** — exit code 0 = exito
- `lab1-producer` y `lab1-consumer`: deben decir **Up**

---

## Paso 5: Verificar el Topico de Kafka

```bash
# Describir el topico wiki.changes
docker exec lab1-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic wiki.changes

# Salida esperada:
# Topic: wiki.changes  PartitionCount: 3  ReplicationFactor: 1
#   Partition: 0  Leader: 1  Replicas: 1  Isr: 1
#   Partition: 1  Leader: 1  Replicas: 1  Isr: 1
#   Partition: 2  Leader: 1  Replicas: 1  Isr: 1

# Listar TODOS los topicos del broker
docker exec lab1-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

---

## Paso 6: Monitorear el Producer en Tiempo Real

```bash
# Ver logs del producer (flujo continuo de eventos de Wikimedia)
docker compose logs -f producer

# Salida tipica:
# [enwiki] ClueBot NG -> Vandalism article (−234 bytes)
# [wikidatawiki] QuickStatements -> Q12345678 (97 bytes)
# [eswiki] UsuarioBot -> Categoria:Matematicas (0 bytes)
# [commonswiki] CommonsDelinker -> File:Image.jpg (−45 bytes)

# Ctrl+C para salir de los logs (NO detiene el contenedor)
```

**Interpretar la salida:**
- `[wiki]` = proyecto Wikimedia (idioma/proyecto)
- `Usuario` = quien hizo el cambio
- `Titulo` = pagina editada
- `(N bytes)` = cambio neto en bytes

---

## Paso 7: Monitorear el Consumer en Tiempo Real

```bash
# Ver logs del consumer (inserciones en PostgreSQL)
docker compose logs -f consumer

# Salida tipica:
# Conectado exitosamente a PostgreSQL (wikidb).
# Consumidor suscrito a 'wiki.changes'. Esperando eventos...
# 50 registros insertados en Postgres. (Offset: 50)
# 50 registros insertados en Postgres. (Offset: 100)
# 50 registros insertados en Postgres. (Offset: 150)
```

Cada linea de "50 registros" confirma un batch de INSERT exitoso en PostgreSQL.

---

## Paso 8: Verificar Datos en PostgreSQL

```bash
# Conectarse a PostgreSQL interactivamente
docker exec -it lab1-postgres psql -U wikiuser -d wikidb

# Una vez dentro del shell psql:

-- Contar total de registros
SELECT COUNT(*) FROM wiki_recent_changes;

-- Ver los 5 registros mas recientes
SELECT id, wiki, LEFT(title,40) AS title, user_name, bot, byte_diff
FROM wiki_recent_changes
ORDER BY received_at DESC
LIMIT 5;

-- Distribucion por idioma
SELECT wiki, COUNT(*) AS total
FROM wiki_recent_changes
GROUP BY wiki
ORDER BY total DESC
LIMIT 10;

-- Ratio bots vs humanos
SELECT
  CASE WHEN bot THEN 'Bot' ELSE 'Humano' END AS tipo,
  COUNT(*) AS total,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS porcentaje
FROM wiki_recent_changes
GROUP BY bot;

-- Salir del shell psql
\q
```

---

## Paso 9: Kafka UI — Interfaz Visual

Abre tu navegador en: **http://localhost:8080**

### Que puedes explorar:

**Brokers:**
- Ve al menu "Brokers" para ver el estado del cluster KRaft
- Puedes ver particiones, replicas y el nodo controller

**Topics:**
- Menu "Topics" > selecciona "wiki.changes"
- "Messages": permite ver los mensajes en tiempo real
- "Consumers": muestra el consumer group y el lag de cada particion

**Consumer Groups:**
- Menu "Consumer Groups" > "wiki-persister-group"
- Columna "LAG": numero de mensajes pendientes de procesar
- Un lag bajo (< 100) indica que el consumer esta al dia

---

## Paso 10: Verificar el Consumer Group y el Lag

```bash
# Ver estado del consumer group desde la linea de comandos
docker exec lab1-kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --group wiki-persister-group \
  --describe

# Salida tipica:
# GROUP                TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# wiki-persister-group wiki.changes 0          5200            5210            10
# wiki-persister-group wiki.changes 1          5350            5365            15
# wiki-persister-group wiki.changes 2          680             685             5
```

**Interpretar el LAG:**
- LAG = mensajes en Kafka que aun no han sido procesados por el consumer
- LAG < 100 = consumer esta practica mente en tiempo real (NORMAL)
- LAG > 1000 = consumer esta retrasado (puede indicar un problema de rendimiento)
- LAG = 0 = consumer esta completamente al dia

---

## Comandos de Operacion

### Detener el Pipeline (preservando datos)
```bash
docker compose stop
```

### Reanudar el Pipeline
```bash
docker compose start
```

### Reiniciar un servicio especifico
```bash
docker compose restart producer
docker compose restart consumer
```

### Detener y eliminar contenedores (PRESERVA los datos de PostgreSQL)
```bash
docker compose down
```

### Reset completo (ELIMINA todos los datos)
```bash
docker compose down -v
# El flag -v elimina los volumenes, incluido postgres_data
# La proxima vez que levantes, PostgreSQL estara vacio
```

### Ver logs de todos los servicios juntos
```bash
docker compose logs -f --tail=20
```

### Ver cuantos mensajes tiene el topico
```bash
docker exec lab1-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 \
  --topic wiki.changes \
  --time -1
```

---

## Troubleshooting

### Problema: `lab1-kafka` tarda en pasar a `healthy`
```bash
# Ver logs de Kafka para diagnosticar
docker logs lab1-kafka --tail 30
# Espera hasta 60 segundos; la JVM de Kafka necesita tiempo para inicializar
```

### Problema: `lab1-producer` en estado `Restarting`
```bash
docker logs lab1-producer --tail 20
# Causa comun: Kafka aun no paso el healthcheck cuando el producer intento conectarse
# Solucion: docker compose restart producer  (ya deberia funcionar)
```

### Problema: `lab1-consumer` no inserta datos
```bash
docker logs lab1-consumer --tail 30
# Verificar que el topico wiki.changes existe:
docker exec lab1-kafka kafka-topics --bootstrap-server kafka:29092 --list
```

### Problema: Sin datos en PostgreSQL
```bash
# Verificar que init.sql se ejecuto correctamente
docker logs lab1-postgres | grep -i "init"
# Debe mostrar: running /docker-entrypoint-initdb.d/init.sql
```

### Problema: Puerto 8080 ocupado (Kafka UI)
```bash
# Cambiar el puerto en docker-compose.yml:
# ports:
#   - "8090:8080"   # Cambia 8080 por 8090
docker compose up -d kafka-ui
```
