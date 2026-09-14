# Laboratorio de Retos Prácticos: Ejercicios de Aplicación Autónoma

**Documento:** `03_laboratorio_retos_practicos.md`  
**Práctica 1:** Escalabilidad Horizontal en Apache Kafka  

---

## 🎯 Instrucciones Generales
Este laboratorio está diseñado para que pongas a prueba tus conocimientos de forma práctica. Cada reto plantea un **objetivo técnico del mundo real**, un **criterio de aceptación (cómo saber si tu implementación es correcta)** y pistas clave para guiarte sin revelar la solución directa.

---

## 🧪 Reto 1: Medición de Rendimiento — Comparativa de Throughput (1 vs 3 Consumidores)

### 📌 Planteamiento del problema:
Como ingeniero de datos, debes evaluar cuantitativamente el impacto de triplicar las réplicas del consumidor en la tasa efectiva de inserción en PostgreSQL (`wiki_recent_changes`).

### 🎯 Tu Misión:
1. Con **1 solo consumidor activo**, mide cuántos registros ingresan a la tabla `wiki_recent_changes` en un intervalo estricto de **10 o 20 segundos**.
2. Escala a **3 consumidores en paralelo** (`docker compose up -d --scale consumer=3`).
3. Mide nuevamente la cantidad de registros insertados en el mismo intervalo de tiempo.
4. Calcula el rendimiento en **eventos por segundo (eps)** para ambos escenarios y responde:
   * *¿El throughput hacia PostgreSQL se triplica linealmente?*
   * *¿El sistema está limitado por la capacidad de consumo (Consumer-Bounded) o por la tasa de generación de Wikimedia (Source-Bounded)?*
   * *¿Qué impacto observas en el Consumer Lag de las 3 particiones al pasar a 3 consumidores?*

### ✅ Criterio de Aceptación:
* Obtener dos métricas cuantitativas reproducibles: `EPS_1_consumidor` y `EPS_3_consumidores`.
* Redactar una breve conclusión técnica justificando los resultados en función de la tasa de ingesta de Wikimedia.

---

## 🧪 Reto 2: Crear un Segundo Grupo de Consumo Independiente (Patrón Pub/Sub Puro)

### 📌 Planteamiento del problema:
El equipo de Data Science requiere consumir los eventos en tiempo real de `wiki.changes` para entrenar un modelo de detección de anomalías. **Exigen acceso a los mismos eventos en vivo, pero no deben conectarse a PostgreSQL ni interferir con el consumidor existente (`wiki-persister-group`).**

### 🎯 Tu Misión:
1. Ejecutar un consumidor efímero usando la herramienta oficial `kafka-console-consumer` dentro del contenedor `lab1-kafka`.
2. Asignarle un nuevo `group.id` exclusivo (por ejemplo: `grupo-ciencia-datos`).
3. Configurar el consumidor para que lea al menos 5 o 10 mensajes mostrando la clave del evento (el nombre del wiki).
4. Demostrar en Kafka UI (`http://localhost:8080`) que:
   * Existen **dos grupos de consumidores distintos** leyendo el mismo tópico en paralelo.
   * Los offsets de cada grupo avanzan de forma independiente sin afectarse mutuamente.
   * El persistidor de PostgreSQL continúa insertando datos sin ninguna alteración.

### ✅ Criterio de Aceptación:
* Ambos grupos (`wiki-persister-group` y `grupo-ciencia-datos`) deben figurar en la pestaña **Consumers** de Kafka UI con sus respectivos miembros y métricas de lag aisladas.

---

## 🧪 Reto 3: Desastre y Reconstrucción — Reseteo de Offsets a `earliest`

### 📌 Planteamiento del problema:
Para fines de auditoría forense y recuperación de desastres, se requiere reprocesar todos los eventos almacenados en Kafka desde el primer mensaje disponible en la ventana de retención.

### 🎯 Tu Misión:
1. Detener el servicio de consumidores (`docker compose stop consumer`).
2. Verificar el estado del grupo con el comando `kafka-consumer-groups --state`. *(Pista: Kafka exige que el grupo alcance el estado `Empty` antes de permitir modificar los punteros).*
3. Ejecutar primero una simulación con `--dry-run` para verificar los nuevos offsets.
4. Aplicar el reseteo definitivo de todas las particiones al offset inicial usando `--to-earliest --execute`.
5. Reanudar el consumidor (`docker compose start consumer`) y comprobar en los logs cómo lee a máxima velocidad desde el offset 0.

> [!CAUTION]
> **Pregunta Clave de Arquitectura:** Si tu consumidor vuelve a leer decenas de miles de eventos que ya habían sido insertados previamente en PostgreSQL:
> * ¿Por qué la base de datos no arroja errores de clave primaria duplicada (`duplicate key value violates unique constraint`)?
> * ¿Qué papel juega la **idempotencia** (`ON CONFLICT (id) DO NOTHING` o `DO UPDATE`) en sistemas de streaming resilientes?

### ✅ Criterio de Aceptación:
* Ejecutar exitosamente el reseteo comprobando que el `NEW-OFFSET` en las 3 particiones queda en `0`.
* Verificar mediante `docker compose logs consumer` que los registros se re-ingestan sin que el contenedor falle ni se interrumpa la ejecución.

---

## 🧪 Reto 4: Autoevaluación Teórico-Práctica

Responde a las siguientes 4 preguntas conceptuales para validar tu entendimiento:

1. **¿Qué sucede con los mensajes de una partición si el consumidor asignado a ella muere y todavía no se completa el tiempo de `session.timeout.ms`?**
   * *A)* Se pierden permanentemente.
   * *B)* Se almacenan en el disco del broker (log inmutable append-only) y se reanuda su consumo una vez que el rebalanceo reasigna la partición.
   * *C)* Se redirigen automáticamente a Zookeeper.

2. **Si un tópico tiene 6 particiones y tu clúster cuenta con 4 consumidores en el mismo grupo, ¿cómo se distribuyen las particiones entre los miembros?**
   * *A)* 2 consumidores atienden 2 particiones cada uno, y los otros 2 atienden 1 partición cada uno ($2+2+1+1 = 6$).
   * *B)* Solo 4 particiones se leen; las otras 2 quedan bloqueadas.
   * *C)* Todos leen las 6 particiones al mismo tiempo.

3. **¿Cuál es la diferencia fundamental entre el parámetro `session.timeout.ms` y `max.poll.interval.ms`? ¿Qué hilo de ejecución supervisa cada uno?**

4. **Si observas en Kafka UI que el Lag de la Partición 1 es de 50,000 mensajes mientras que el Lag de las Particiones 0 y 2 es de apenas 5 mensajes, ¿cuáles son las 2 causas arquitectónicas más probables?**
