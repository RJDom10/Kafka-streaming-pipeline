# Guia Exhaustiva de Puertos, Interfaces de Usuario y Conectividad de Red

## 1. Topologia de Red del Lakehouse (`lab2-network`)

La arquitectura del Nivel 2 opera sobre una red virtual aislada de tipo puente (*bridge*) gestionada por Docker denominada `lab2-network`. 

Esta red proporciona dos planos de comunicacion diferenciados:

1. **Plano de Red Interno (Inter-Contenedores)**:
   * Todos los contenedores se comunican entre si utilizando los nombres de host (*service hostnames*) registrados en el servidor DNS interno integrado de Docker Engine.
   * Los contenedores acceden directamente a los puertos de escucha nativos de cada servicio (por ejemplo, `kafka:29092`, `minio:9000`, `iceberg-rest:8181`, `trino:8080`).
   * No existe restriccion de puertos entre contenedores dentro de la misma red virtual.

2. **Plano de Red Externo (Host / Alumnos / Clientes)**:
   * Solo aquellos servicios que requieren interaccion directa con el usuario, navegadores web o herramientas externas (como DBeaver o librerias cliente de Python) exponen puertos hacia el sistema operativo anfitrion (*host binding*).
   * La direccion de enlace predeterminada es `0.0.0.0:<puerto_host>`, accesible mediante `localhost:<puerto_host>` o la direccion IP local del equipo.

```
+---------------------------------------------------------------------------------------------------+
| MAQUINA ANFITRIONA (HOST / WINDOWS / WSL)                                                         |
|                                                                                                   |
|  [Navegador Web]    [DBeaver / DataGrip]    [Terminal Host / CLI]    [Python Script Local]        |
|        |                     |                       |                        |                   |
|        | :9001 (MinIO UI)    | :8082 (Trino JDBC)    | :9092 (Kafka Client)   | :8888 (Jupyter)   |
+--------+---------------------+-----------------------+------------------------+-------------------+
         |                     |                       |                        |
=========|=====================|=======================|========================|====================
DOCKER BRIDGE: lab2-network    |                       |                        |
=========|=====================|=======================|========================|====================
         v                     v                       v                        v
+------------------+  +------------------+  +------------------+  +-------------------------------+
| lab2-minio       |  | lab2-trino       |  | lab2-kafka       |  | lab2-spark                    |
| - 9000 (S3 API)  |  | - 8080 (Web/JDBC)|  | - 29092 (Interno)|  | - 8888 (Jupyter Notebook)     |
| - 9001 (Web UI)  |  +------------------+  | - 9092 (Host)    |  | - 4040 / 4041 (Spark App UI)  |
+------------------+                        +------------------+  +-------------------------------+
         ^                                                                  |
         | S3 File I/O (9000)                                               | REST Catalog (8181)
         +------------------------------------+-----------------------------+
                                              |
                                              v
                                    +--------------------+
                                    | lab2-iceberg-rest  |
                                    | - 8181 (REST API)  |
                                    +--------------------+
```

---

## 2. Matriz Exhaustiva de Puertos Activos

| Puerto Host | Puerto Contenedor | Servicio / Contenedor | Protocolo / Tipo | Autenticacion | URL o Modo de Acceso | Proposito Principal |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`9092`** | `9092` | `kafka` (`lab2-kafka`) | TCP Binario (Kafka) | Ninguna (PLAINTEXT) | `localhost:9092` | Conexion de clientes Kafka externos (ej. kcat, Python, Kafka UI). |
| **`9000`** | `9000` | `minio` (`lab2-minio`) | HTTP (S3 REST API) | Key / Secret | `http://localhost:9000` | API compatible con AWS S3 para lectura/escritura de Parquet y metadatos. |
| **`9001`** | `9001` | `minio` (`lab2-minio`) | HTTP (Consola Web) | `admin` / `password` | `http://localhost:9001` | **Interfaz grafica de MinIO** para explorar buckets, carpetas y archivos. |
| **`8181`** | `8181` | `iceberg-rest` (`lab2-iceberg-rest`) | HTTP (REST API) | Ninguna (en lab) | `http://localhost:8181/v1/config` | API del catalogo REST de Apache Iceberg para gestion transaccional. |
| **`8888`** | `8888` | `spark-iceberg` (`lab2-spark`) | HTTP (Web UI) | Ninguna (token libre) | `http://localhost:8888` | **Servidor Jupyter Notebook** interactivo con soporte nativo para PySpark. |
| **`4040`** | `4040` | `spark-iceberg` (`lab2-spark`) | HTTP (Web UI) | Ninguna | `http://localhost:4040` | **Spark Application Web UI**: visualizacion de DAGs, Stages y Streaming. |
| **`8082`** | `8080` | `trino` (`lab2-trino`) | HTTP (Web UI & JDBC) | Usuario libre | `http://localhost:8082` | **Consola Web de Trino** y endpoint JDBC/ODBC para clientes analiticos. |

---

## 3. Analisis Tecnico Detallado por Servicio y Puerto

### 3.1 Puerto 9092 — Apache Kafka Broker

* **Servicio**: `kafka` (Contenedor: `lab2-kafka`)
* **Protocolo**: Protocolo binario serializado sobre TCP.
* **Configuracion de Red**:
  Kafka implementa un mapeo de listeners dual obligatorio para operar de forma correcta dentro de contenedores:
  * `PLAINTEXT://kafka:29092`: Utilizado exclusivamente para el trafico interno entre contenedores (`producer` $	o$ `kafka`, `spark-iceberg` $	o$ `kafka`).
  * `PLAINTEXT_HOST://localhost:9092`: Publicado al host para que aplicaciones corriendo en la maquina del usuario puedan consumir o publicar mensajes directamente.
* **Utilidad Didactica**:
  Permite a los alumnos verificar que el broker recibe eventos en tiempo real sin ingresar al contenedor.
* **Comandos de Prueba desde el Host**:
  ```bash
  # Ver informacion del cluster usando kcat (si esta instalado en el host)
  kcat -b localhost:9092 -L

  # Consumir los ultimos 5 mensajes del topico wiki.changes
  kcat -b localhost:9092 -C -t wiki.changes -c 5
  ```

---

### 3.2 Puerto 9000 — MinIO Object Storage (S3 API)

* **Servicio**: `minio` (Contenedor: `lab2-minio`)
* **Protocolo**: HTTP basado en la especificacion estandar de la API REST de Amazon S3.
* **Credenciales**:
  * **Access Key ID**: `admin`
  * **Secret Access Key**: `password`
* **Configuracion Critica**:
  Opera bajo la directiva **`s3.path-style-access=true`**. Esto fuerza a que las peticiones se dirijan a `http://minio:9000/warehouse/...` en lugar del estilo de alojamiento virtual (*virtual-hosted style*) `http://warehouse.minio:9000/...`, el cual fallaria a nivel de resolucion DNS en redes privadas de Docker.
* **Utilidad Didactica**:
  Es el punto de montaje utilizado por el driver `iceberg-aws-bundle` de Spark y el conector nativo S3 de Trino para transferir archivos de datos Parquet.
* **Comandos de Prueba desde el Host**:
  ```bash
  # Listar buckets con AWS CLI apuntando a MinIO
  aws --endpoint-url http://localhost:9000 s3 ls --profile default
  ```

---

### 3.3 Puerto 9001 — MinIO Web Console (Interfaz Grafica)

* **Servicio**: `minio` (Contenedor: `lab2-minio`)
* **Protocolo**: HTTP (Interfaz Web React)
* **Direccion URL**: `http://localhost:9001`
* **Credenciales de Acceso**:
  * **Username**: `admin`
  * **Password**: `password`
* **Utilidad Didactica y Funciones Disponibles**:
  1. **Navegacion de Buckets**: Los estudiantes pueden abrir el bucket `warehouse` y visualizar la estructura de carpetas:
     * `warehouse/bronze/wiki_raw_events/metadata/`: Contiene los archivos de metadatos `.metadata.json`, `.avro` (manifests) y `.avro` (manifest lists).
     * `warehouse/bronze/wiki_raw_events/data/`: Contiene los archivos `.parquet` particionados por fecha.
     * `warehouse/silver/wiki_cleaned/`: Almacena la tabla Silver depurada.
     * `warehouse/gold/wiki_hourly_metrics/`: Almacena las particiones de la tabla Gold.
  2. **Inspeccion de Checkpoints**: En el bucket `checkpoints` se observan los offsets confirmados del streaming de Spark.
  3. **Descarga de Archivos**: Los alumnos pueden descargar un archivo `.parquet` individual e inspeccionarlo localmente con utilidades como DuckDB, parquet-tools o Python Pandas.

---

### 3.4 Puerto 8181 — Apache Iceberg REST Catalog

* **Servicio**: `iceberg-rest` (Contenedor: `lab2-iceberg-rest`)
* **Protocolo**: HTTP (REST API Open Source)
* **Direccion URL**: `http://localhost:8181`
* **Endpoint de Salud y Diagnostico**: `http://localhost:8181/v1/config`
* **Funcionamiento Interno**:
  El catalogo REST de Apache Iceberg es el arbitro central de consistencia transaccional. Cuando PySpark o Trino desean leer o modificar una tabla:
  1. Hacen una peticion GET al endpoint `http://iceberg-rest:8181/v1/namespaces/<ns>/tables/<tbl>`.
  2. El catalogo responde con la ubicacion exacta en MinIO del archivo de metadatos mas reciente (`vN.metadata.json`).
  3. Tras una escritura atomica, el escritor solicita una actualizacion (*commit*) al catalogo REST para mover el puntero al nuevo snapshot mediante un mecanismo de comparacion e intercambio (*Compare-And-Swap / CAS*).
* **Comandos de Prueba desde el Host**:
  ```bash
  # Consultar la configuracion del catalogo REST
  curl -s http://localhost:8181/v1/config | jq .
  ```

---

### 3.5 Puerto 8888 — Jupyter Notebook (Entorno Interactivo PySpark)

* **Servicio**: `spark-iceberg` (Contenedor: `lab2-spark`)
* **Protocolo**: HTTP (Interfaz Web Jupyter Server)
* **Direccion URL**: `http://localhost:8888`
* **Autenticacion**: Configurado con token desactivado para acceso inmediato de los estudiantes en clase.
* **Utilidad Didactica**:
  Provee a los alumnos un laboratorio interactivo para:
  * Probar transformaciones de PySpark celda por celda.
  * Graficar resultados analiticos con librerias como Matplotlib, Seaborn o Plotly.
  * Explorar tablas de Iceberg sin necesidad de compilar scripts `.py` ni usar la terminal de comandos.

---

### 3.6 Puerto 4040 — Spark Application Web UI

* **Servicio**: `spark-iceberg` (Contenedor: `lab2-spark`)
* **Protocolo**: HTTP (Dashboard de Monitoreo de Apache Spark)
* **Direccion URL**: `http://localhost:4040` (o `http://localhost:4041` si el puerto base se encuentra ocupado temporalmente por otro proceso).
* **Utilidad Didactica y Metricas Clave**:
  La interfaz Web de Spark solo esta disponible mientras un script de Spark (`spark-submit` o Jupyter) se encuentra en ejecucion activa. Muestra:
  1. **Pestana Jobs & Stages**: Tiempos de ejecucion de tareas paralelas, tamano de datos barajados (*shuffle read/write*) y visualizacion del grafo aciclico dirigido (DAG).
  2. **Pestana Structured Streaming**: Graficas en tiempo real de la tasa de procesamiento de eventos por segundo (*input rate* vs. *processing rate*) y la latencia de cada micro-batch hacia la capa Bronze.
  3. **Pestana Executors**: Consumo de memoria RAM (Storage Memory vs. Execution Memory) y recoleccion de basura (*Garbage Collection* de Java).

---

### 3.7 Puerto 8082 — Trino Web UI & Endpoint JDBC

* **Servicio**: `trino` (Contenedor: `lab2-trino`)
* **Protocolo**: HTTP (Web UI) y protocolo de comunicacion cliente Trino sobre HTTP (compatible con JDBC/ODBC).
* **Mapeo de Puertos**: Mapeado desde el puerto interno estandar `8080` de Trino hacia el puerto `8082` del host, evitando cualquier colision con aplicaciones web o proxies que frecuentemente ocupan el puerto 8080 en equipos de estudiantes.
* **Direccion de la Consola Web**: `http://localhost:8082`
* **Utilidad Didactica de la Consola Web de Trino**:
  1. **Monitoreo en Tiempo Real**: Visualizacion de todas las consultas SQL activas, terminadas y fallidas.
  2. **Detalle de Plan de Ejecucion**: Permite a los alumnos hacer clic sobre una consulta y ver la distribucion de tareas por escenario (*Stages*), el volumen de filas procesadas por segundo y la memoria consumida.
  3. **Deteccion de Cuellos de Botella**: Identifica si una consulta tardo mas tiempo esperando lectura de MinIO o realizando una operacion de ordenamiento en memoria.
* **Parametros para Conexion JDBC desde DBeaver / DataGrip**:
  * **Host**: `localhost`
  * **Port**: `8082`
  * **Driver Type**: `Trino`
  * **Catalog**: `lakehouse`
  * **Schema**: `gold`
  * **Username**: `alumno`
  * **Password**: *(Vacio / Sin contrasena)*

---

## 4. Comandos de Diagnostico y Resolucion de Conflictos

### 4.1 Comprobar el Estado de Todos los Puertos
Desde la terminal del host (WSL o Linux):
```bash
# Inspeccionar puertos en escucha vinculados a Docker
ss -tulpn | grep -E '9092|9000|9001|8181|8888|4040|8082'
```

### 4.2 Probar Conectividad HTTP Rapida
```bash
# Verificar MinIO S3 API
curl -sI http://localhost:9000/minio/health/live

# Verificar Iceberg REST Catalog
curl -s http://localhost:8181/v1/config

# Verificar Trino Web UI
curl -sI http://localhost:8082/ui/
```

### 4.3 Resolucion de Conflictos de Puertos
Si algun puerto (por ejemplo, el 8082 o el 9001) ya esta siendo utilizado por otro software local (por ejemplo, servidores Apache/Nginx o bases de datos locales):
1. Abrir `docker-compose.yml`.
2. Modificar la seccion `ports:` del servicio en conflicto cambiando el puerto del lado izquierdo (host):
   ```yaml
   ports:
     - "8085:8080" # Cambia el puerto de Trino en el host al 8085
   ```
3. Reiniciar el servicio:
   ```bash
   docker compose up -d trino
   ```

---

## 5. Buenas Practicas de Seguridad para Produccion

En este laboratorio local universitario se omiten intencionalmente contrasenas complejas y certificados TLS para facilitar la comprension conceptual y el aprendizaje rapido. Para trasladar esta arquitectura a un entorno de produccion en la nube (AWS, GCP, Azure o infraestructura On-Premise), deben aplicarse las siguientes directivas:

1. **Cifrado en Transito (TLS/mTLS)**:
   * Habilitar HTTPS en MinIO, Iceberg REST Catalog y Trino mediante certificados validos.
   * Configurar `SSL` o `SASL_SSL` en los listeners de Apache Kafka.
2. **Cifrado en Reposo**:
   * Habilitar el cifrado del lado del servidor (*Server-Side Encryption* - SSE-S3 o SSE-KMS) en los buckets de MinIO para proteger los archivos Parquet.
3. **Autenticacion y Control de Acceso Basado en Roles (RBAC)**:
   * Integrar Trino con LDAP, Active Directory u OpenID Connect (OAuth2 / Okta).
   * Configurar politicas IAM granulares en MinIO para restringir que el rol de Trino unicamente posea permisos de lectura (`s3:GetObject`), reservando los permisos de escritura (`s3:PutObject`) para los motores de procesamiento como Apache Spark.
