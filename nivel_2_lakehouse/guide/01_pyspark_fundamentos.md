# Apache Spark y PySpark: Fundamentos de Procesamiento Distribuido

## Que es Apache Spark

Apache Spark es un motor de procesamiento distribuido disenado para analisis
de datos a gran escala. A diferencia de MapReduce (Hadoop), Spark procesa
datos **en memoria** (RAM) en lugar de escribir resultados intermedios a disco,
lo que lo hace entre 10x y 100x mas rapido para cargas de trabajo iterativas.

## Arquitectura de Spark

```
┌──────────────────────────────────────────────────────────┐
│                     SPARK APPLICATION                     │
│                                                          │
│   ┌──────────┐                                           │
│   │  DRIVER   │  (Planifica, coordina, programa)         │
│   │  Program  │                                          │
│   └─────┬────┘                                           │
│         |                                                │
│   ┌─────v─────────────────────────────────────┐          │
│   │          CLUSTER MANAGER                   │          │
│   │   (Standalone / YARN / Mesos / K8s)       │          │
│   └─────┬──────────┬──────────┬───────────────┘          │
│         |          |          |                           │
│   ┌─────v────┐ ┌───v──────┐ ┌v──────────┐               │
│   │ EXECUTOR │ │ EXECUTOR │ │ EXECUTOR  │               │
│   │ (Worker) │ │ (Worker) │ │ (Worker)  │               │
│   │ Task 1   │ │ Task 2   │ │ Task 3    │               │
│   │ Task 4   │ │ Task 5   │ │ Task 6    │               │
│   └──────────┘ └──────────┘ └───────────┘               │
└──────────────────────────────────────────────────────────┘
```

### Driver

El proceso principal que ejecuta la funcion `main()` del programa. Es
responsable de:
- Crear la `SparkSession`
- Definir el plan logico de transformaciones (DAG)
- Coordinar la ejecucion distribuida en los executors
- Recopilar los resultados finales

### Executor

Procesos JVM que ejecutan las tareas (tasks) asignadas por el driver.
Cada executor:
- Recibe particiones de datos para procesar
- Ejecuta las transformaciones definidas
- Almacena datos en cache cuando se solicita
- Reporta resultados y estado al driver

### En Nuestro Laboratorio

Usamos Spark en **modo local** dentro de un unico contenedor Docker.
El driver y los executors comparten la misma JVM. Esto simplifica la
infraestructura del laboratorio pero no cambia la logica del codigo:
los mismos scripts funcionarian en un cluster de produccion con
decenas de nodos.

## PySpark: La API de Python para Spark

PySpark permite escribir aplicaciones Spark en Python. Internamente,
PySpark traduce las operaciones de Python a llamadas JVM a traves de
Py4J, un puente Python-Java.

### DataFrame API

El DataFrame de PySpark es conceptualmente similar a un DataFrame de
Pandas, pero con diferencias criticas:

| Aspecto | Pandas DataFrame | PySpark DataFrame |
|---|---|---|
| Ubicacion de datos | RAM de una sola maquina | Distribuido en N nodos |
| Evaluacion | Inmediata (eager) | Diferida (lazy) |
| Escala | GB | TB / PB |
| API | Mutaciones in-place | Transformaciones inmutables |

### Lazy Evaluation

PySpark no ejecuta las transformaciones inmediatamente. En su lugar,
construye un **plan logico** (DAG) que se optimiza y ejecuta solo cuando
se invoca una **accion** (`count()`, `show()`, `write()`, `collect()`).

```python
# Estas 3 lineas NO ejecutan nada aun (transformaciones)
df = spark.read.table("bronze.events")
filtered = df.filter(col("wiki") == "enwiki")
projected = filtered.select("title", "user_name")

# ESTA linea dispara la ejecucion real (accion)
projected.show()
```

### Transformaciones vs Acciones

| Tipo | Ejemplos | Comportamiento |
|---|---|---|
| Transformacion | `select()`, `filter()`, `groupBy()`, `join()` | Construye el plan, no ejecuta |
| Accion | `count()`, `show()`, `write()`, `collect()` | Ejecuta el plan completo |

## SparkSession: El Punto de Entrada

Toda aplicacion PySpark comienza creando una `SparkSession`:

```python
spark = SparkSession.builder \
    .appName("MiApp") \
    .config("spark.some.config", "value") \
    .getOrCreate()
```

La SparkSession encapsula:
- `SparkContext`: conexion al cluster
- `SQLContext`: motor SQL
- Configuraciones del catalogo, filesystem, etc.

## Particiones en Spark

Spark divide los datos en **particiones** que se procesan en paralelo.
Cada particion es un fragmento del dataset que cabe en la memoria de
un executor.

- Mas particiones = mas paralelismo (pero mas overhead de coordinacion)
- Menos particiones = menos paralelismo (pero menos overhead)
- Regla general: 2-4 particiones por core de CPU disponible

En nuestro laboratorio, Kafka tiene 3 particiones y Spark lee cada una
como una particion del DataFrame, lo que proporciona un paralelismo
natural de 3.
