# Teoría Técnica Profunda: Grupos de Consumidores, Particiones y Protocolo de Rebalanceo

**Documento:** `01_teoria_profunda_consumer_groups.md`  
**Práctica 1:** Escalabilidad Horizontal en Apache Kafka  

---

## 1. La Partición: Unidad Fundamental de Concurrencia y Orden

En Apache Kafka, un tópico **no** es un bloque indivisible de almacenamiento. Un tópico está segmentado internamente en una o más **particiones** (en nuestro laboratorio, el tópico `wiki.changes` tiene **3 particiones**: `P0`, `P1` y `P2`).

### 1.1 El Orden en Kafka es Parcial, no Global
Un error común entre principiantes es asumir que Kafka garantiza el orden cronológico total de todos los mensajes de un tópico.
* **Garantía real:** Kafka garantiza el orden estricto de entrega **únicamente dentro de una misma partición**.
* Si el mensaje `A` llega a `P0` con offset 100 y el mensaje `B` llega a `P1` con offset 100, no existe una relación de ordenamiento temporal inherente entre ambos garantizada por el broker al momento del consumo concurrente.

### 1.2 ¿Por qué particionar?
1. **Escalabilidad de Almacenamiento:** Una partición puede residir en un disco o broker físico distinto, permitiendo que un tópico almacene petabytes distribuidos.
2. **Escalabilidad de Procesamiento:** Permite que múltiples procesos independientes lean datos en paralelo sin bloquearse mutuamente.

---

## 2. Grupos de Consumidores (`group.id`): Unificando Colas y Pub/Sub

Kafka unifica dos modelos clásicos de mensajería (Colas Punto a Punto y Publicador/Suscriptor) mediante un único parámetro de configuración: el **`group.id`**.

```
                           ┌───────────────────────────────┐
                           │   Tópico: wiki.changes        │
                           │  [ P0 ]    [ P1 ]    [ P2 ]   │
                           └────┬─────────┬─────────┬──────┘
                                │         │         │
               ┌────────────────┴─────────┼─────────┴────────────────┐
               │                          │                          │
               ▼                          ▼                          ▼
     ┌───────────────────┐      ┌───────────────────┐      ┌───────────────────┐
     │   Consumidor 1    │      │   Consumidor 2    │      │   Consumidor 3    │
     │   (Atiende P0)    │      │   (Atiende P1)    │      │   (Atiende P2)    │
     └───────────────────┘      └───────────────────┘      └───────────────────┘
                               GRUPO: "wiki-persister-group"
```

### Regla 1: Reparto dentro del mismo grupo (Modelo Cola / Scale-Out)
* Todos los consumidores que configuran el mismo `group.id = "wiki-persister-group"` cooperan entre sí.
* **Regla estricta:** Cada partición de un tópico es asignada a **exactamente un consumidor** dentro del mismo grupo en un momento dado. Dos consumidores del mismo grupo **nunca** leerán la misma partición simultáneamente.

### Regla 2: Múltiples grupos independientes (Modelo Pub/Sub)
* Si mañana levantas otro servicio con `group.id = "analitica-ml-group"`, este nuevo grupo recibirá **todos y cada uno de los mensajes**, con sus propios punteros (offsets) independientes, sin interferir con `wiki-persister-group`.

---

## 3. La Matemática del Escalado Horizontal

Dadas $P$ particiones en un tópico y $C$ consumidores activos en el grupo:

| Relación | Comportamiento del Clúster | Estado del Sistema |
|---|---|---|
| **$C < P$** (ej. 1 consumidor, 3 particiones) | El consumidor activo asume la lectura de todas las $P$ particiones secuencialmente. | Subóptimo en throughput. |
| **$C = P$** (ej. 3 consumidores, 3 particiones) | Cada consumidor atiende exactamente 1 partición ($1:1$). | **Punto óptimo de paralelismo.** |
| **$C > P$** (ej. 4 consumidores, 3 particiones) | 3 consumidores atienden 1 partición cada uno. El $4^\circ$ consumidor queda en estado **IDLE (ocioso / standby)**. | Desperdicio de recursos computacionales. |

> [!WARNING]
> **Lección de Diseño:** Si tu tópico tiene 3 particiones y tu pipeline no da abasto para procesar el volumen de datos, **no sirve de nada añadir un 4º contenedor de consumidor**. El 4º consumidor no recibirá ningún dato. La única solución es **incrementar el número de particiones del tópico** antes de escalar los consumidores.

---

## 4. El Coordinador de Grupo (Group Coordinator)

¿Quién decide qué consumidor lee qué partición? ¿Cómo se entera el clúster si un consumidor se cayó?

### 4.1 Elección del Broker Coordinador
1. Kafka utiliza un tópico interno compactado llamado `__consumer_offsets` (por defecto con 50 particiones).
2. Para cualquier grupo, el broker calcula:
   $$\text{Partición Interna} = \text{abs}(\text{hash}(\text{group.id})) \pmod{50}$$
3. El broker que sea el **líder** de esa partición interna se convierte automáticamente en el **Group Coordinator** de ese grupo de consumidores.

### 4.2 El Consumidor Líder
Cuando los consumidores se conectan al coordinador, este elige al primero de ellos como el **Consumidor Líder (Consumer Leader)**.
* El coordinador **no** calcula el mapeo de particiones; simplemente coordina las membresías.
* El **Consumidor Líder** ejecuta el algoritmo de asignación (`PartitionAssignor`) y le devuelve el plan de asignación al coordinador, quien luego lo difunde a los demás miembros del grupo.

---

## 5. El Protocolo de Rebalanceo (Rebalance Protocol)

Un **Rebalance** es el proceso mediante el cual las particiones de un tópico se redistribuyen entre los miembros disponibles de un grupo.

### ¿Cuándo se dispara un Rebalance?
1. Un nuevo consumidor se une al grupo (`scale up`).
2. Un consumidor existente abandona el grupo de forma ordenada (`scale down` o shutdown).
3. Un consumidor muere de forma inesperada (falla de hardware, OOM, red cortada).
4. Se agregan nuevas particiones al tópico.

### 5.1 Fases del Protocolo Eager (Tradicional)
1. **Pausa (Stop the World):** Todos los consumidores del grupo pausan su lectura y renuncian a sus particiones actuales.
2. **JoinGroup:** Todos los consumidores envían una solicitud al coordinador para registrarse de nuevo.
3. **SyncGroup:** El líder reparte las particiones y el coordinador notifica la nueva asignación.
4. **Resume:** Los consumidores retoman la lectura desde el último offset confirmado.

### 5.2 Estrategias de Asignación Modernas
* **`RangeAssignor` (Clásica por defecto):** Asigna particiones contiguas por tópico.
* **`RoundRobinAssignor`:** Reparte equitativamente particiones de forma intercalada.
* **`CooperativeStickyAssignor` (Moderna / Recomendada):** Introduce el *Cooperative Rebalance*. En lugar de pausar a todos los consumidores ("stop the world"), solo reasigna las particiones que realmente necesitan moverse, permitiendo que los consumidores no afectados sigan procesando sin interrupción.

---

## 6. Monitoreo de Vida: Heartbeats y Timeouts Críticos

El cliente de Kafka mantiene la conexión activa usando dos hilos de ejecución separados:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Proceso del Consumidor                     │
│                                                                 │
│  [ Hilo 1: Heartbeat Thread ] ──(latido cada 3s)──► Broker      │
│  Mantiene la membresía viva.                                    │
│                                                                 │
│  [ Hilo 2: Processing Loop ]                                    │
│  poll() ──► procesar evento ──► commit_offsets()                │
└─────────────────────────────────────────────────────────────────┘
```

### Parámetros Vitales:
1. **`heartbeat.interval.ms` (ej. 3,000 ms):** Con qué frecuencia el hilo secundario envía un "estoy vivo" al coordinador.
2. **`session.timeout.ms` (ej. 45,000 ms):** Si el coordinador no recibe un heartbeat durante este lapso, asume que el contenedor murió y dispara el rebalanceo.
3. **`max.poll.interval.ms` (ej. 300,000 ms / 5 min):** Si tu código Python se queda atascado procesando un lote excesivamente grande o haciendo una consulta lenta a PostgreSQL y tarda más de este intervalo en llamar al siguiente `.poll()`, Kafka asume que el consumidor se congeló y lo expulsa del grupo, aun si el hilo de heartbeat sigue activo.

---

## 7. Métrica de Oro: El Lag del Consumidor (Consumer Lag)

El **Lag** es la diferencia aritmética entre los mensajes producidos y los mensajes confirmados por el consumidor:

$$\text{Lag} = \text{Log-End-Offset (LEO)} - \text{Current Offset}$$

* **$\text{Lag} \approx 0$:** El sistema procesa los datos tan rápido como se generan (tiempo real puro).
* **$\text{Lag}$ creciendo constantemente:** Alerta crítica. La tasa de ingesta supera la capacidad de procesamiento de los consumidores. Requiere escalar horizontalmente o depurar cuellos de botella en la base de datos receptora.


