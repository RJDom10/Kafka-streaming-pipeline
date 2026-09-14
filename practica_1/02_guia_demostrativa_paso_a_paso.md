# Guía Demostrativa Paso a Paso: Escalado y Rebalanceo en Tiempo Real

**Documento:** `02_guia_demostrativa_paso_a_paso.md`  
**Práctica 1:** Escalabilidad Horizontal en Apache Kafka  

---

## 🎯 Objetivo de la Demostración
Guiarte paso a paso por la ejecución práctica del ciclo de vida completo de un grupo de consumidores: desde el estado base con 1 consumidor, escalando a 2, 3 y 4 consumidores, hasta provocar una falla deliberada y observar la auto-recuperación del clúster.

---

## 📋 Verificación Inicial del Entorno

Antes de comenzar, abre tu terminal y verifica que el clúster esté operativo:

```bash
docker compose ps
```

**Salida esperada:**
* `kafka` ➔ `healthy` (puerto 9092)
* `postgres` ➔ `healthy` (puerto 5432)
* `kafka-ui` ➔ `Up` (puerto 8080)
* `producer` ➔ `Up`
* `consumer` ➔ `Up` (1 réplica inicial)

Abre en tu navegador la consola visual:
👉 **[http://localhost:8080](http://localhost:8080)**

---

## 🔹 Fase 1: Medición de la Línea Base (1 Consumidor)

### Paso 1.1: Inspeccionar el grupo desde la consola CLI
Ejecuta el siguiente comando para consultar el estado del grupo directamente en el broker de Kafka:

```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group wiki-persister-group
```

### 🔍 Qué observar en la salida:
Verás 3 líneas de salida, una por cada partición del tópico:
```text
GROUP               TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
wiki-persister-group wiki.changes    0          15230           15234           4               consumer-wiki-1 /172.20.0.x     consumer-wiki-1
wiki-persister-group wiki.changes    1          15180           15182           2               consumer-wiki-1 /172.20.0.x     consumer-wiki-1
wiki-persister-group wiki.changes    2          15301           15305           4               consumer-wiki-1 /172.20.0.x     consumer-wiki-1
```

> **Hallazgo Clave:** Observa la columna `CONSUMER-ID`. Las 3 particiones (0, 1 y 2) tienen exactamente el **mismo ID de consumidor**. Un solo proceso en Python está haciendo malabares para leer las 3 particiones secuencialmente.

### Paso 1.2: Validar en Kafka UI
1. Entra a `http://localhost:8080`.
2. En el menú de navegación lateral, haz clic en **Consumers**.
3. Selecciona el grupo **`wiki-persister-group`**.
4. Confirma visualmente:
   * **State:** `STABLE`
   * **Members:** `1`
   * **Assigned Partitions:** `3`

---

## 🔹 Fase 2: Escalar a 2 Consumidores (Scale-Up Parcial)

Vamos a duplicar la potencia de procesamiento levantando una segunda réplica del servicio consumidor.

### Paso 2.1: Ejecutar el escalado
```bash
docker compose up -d --scale consumer=2
```

**Salida esperada:**
```text
[+] Running 2/2
 ✔ Container lab_kafka_test-consumer-1  Running
 ✔ Container lab_kafka_test-consumer-2  Started
```

### Paso 2.2: Ver los logs del Rebalanceo en tiempo real
Abre los logs de ambos consumidores para ver la negociación del protocolo:

```bash
docker compose logs consumer --tail=30 -f
```

*(Presiona `Ctrl + C` para salir de los logs una vez que veas el resultado).*

### 🔍 Qué observar en los logs:
Verás cómo el contenedor original (`consumer-1`) recibe una revocación de particiones y el nuevo (`consumer-2`) recibe su parte:
```text
consumer-1 | [DEBUG] Revocando particiones asignadas...
consumer-1 | [INFO] Particiones reasignadas: [0, 1]
consumer-2 | [INFO] Conectado a Kafka como miembro del grupo 'wiki-persister-group'
consumer-2 | [INFO] Particiones asignadas: [2]
```

### Paso 2.3: Verificar con el CLI
```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group wiki-persister-group
```

Ahora verás **dos identificadores distintos** en `CONSUMER-ID`:
* `consumer-1` atiende particiones 0 y 1.
* `consumer-2` atiende la partición 2.

---

## 🔹 Fase 3: Escalar a 3 Consumidores (Paralelismo Óptimo 1:1)

Dado que nuestro tópico tiene 3 particiones, el punto de saturación y paralelismo perfecto es exactamente 3 consumidores.

### Paso 3.1: Ejecutar el comando de escala
```bash
docker compose up -d --scale consumer=3
```

### Paso 3.2: Comprobar el reparto perfecto en Kafka UI
Refresca la página en **Kafka UI** (`http://localhost:8080/ui/clusters/local/consumer-groups/wiki-persister-group`):
* **Miembros activos:** `3`
* Cada miembro tiene asignada exactamente **1 partición**:
  * Miembro A ➔ Partición 0
  * Miembro B ➔ Partición 1
  * Miembro C ➔ Partición 2
* **Lag por partición:** Notarás que el lag disminuye drásticamente a valores entre 0 y 2, porque ningún consumidor tiene que cambiar de contexto entre distintas particiones.

---

## 🔹 Fase 4: Sobreescalado (¿Qué pasa con 4 consumidores?)

¿Qué ocurre si un ingeniero novato piensa: *"Tengo mucho lag, voy a poner 10 consumidores"*?

### Paso 4.1: Escalar a 4 consumidores
```bash
docker compose up -d --scale consumer=4
```

### Paso 4.2: Inspeccionar la asignación CLI
```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group wiki-persister-group --state
```

Y luego:
```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group wiki-persister-group --members
```

### 🔍 Qué observar:
1. Habrá **4 miembros** registrados en el grupo.
2. Tres miembros tendrán asignada 1 partición cada uno (`P0`, `P1`, `P2`).
3. El cuarto miembro aparecerá con `Partitions: None` (o lista vacía).
4. Si revisas los logs del cuarto contenedor (`docker compose logs lab_kafka_test-consumer-4`), verás que llama a `.poll()`, pero jamás procesa ningún registro. **Está en espera pasiva (standby ocioso).**

---

## 🔹 Fase 5: Chaos Engineering (Simular Muerte de un Nodo)

Vamos a simular una falla catastrófica de infraestructura matando abruptamente uno de los contenedores que está trabajando activamente.

### Paso 5.1: Matar el consumidor 2 sin previo aviso
```bash
docker stop lab_kafka_test-consumer-2
```

### Paso 5.2: Cronometrar la detección de la falla
1. Durante los primeros segundos (mientras transcurre el `session.timeout.ms`), el broker asume que el consumidor 2 sigue vivo.
2. Apenas se cumple el timeout, el broker detecta la ausencia de heartbeats y **dispara el rebalanceo de emergencia**.
3. Revisa los logs de los sobrevivientes:
```bash
docker compose logs consumer-1 --tail=20
```

Verás:
```text
consumer-1 | [INFO] Rebalanceo detectado por ausencia de miembro.
consumer-1 | [INFO] Nueva asignación de particiones recibida: [0, 1]
```
La partición que atendía el nodo caído fue rescatada inmediatamente por uno de los otros contenedores. **Cero mensajes perdidos y el pipeline sigue operando con total normalidad.**

---

## 🔹 Fase 6: Restaurar el Entorno al Estado Original

Una vez completada la demostración, regresamos el clúster a su configuración estándar de 1 consumidor:

```bash
docker compose up -d --scale consumer=1 --remove-orphans
```

Verifica que solo queda 1 contenedor de consumidor:
```bash
docker compose ps consumer
```


