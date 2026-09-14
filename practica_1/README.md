# Práctica 1: Escalabilidad Horizontal, Grupos de Consumidores y Rebalanceo en Apache Kafka

**Módulo:** Fundamentos y Operación de Sistemas Distribuidos de Streaming  
**Nivel:** Intermedio — Práctico  
**Tecnologías:** Apache Kafka, Docker Compose, Python (Kafka Consumer), Kafka UI, PostgreSQL  

---

## 🎯 Objetivo General de la Práctica
Comprender y dominar en la práctica el mecanismo fundamental que le otorga a Apache Kafka su alta disponibilidad y escalabilidad horizontal: **los Grupos de Consumidores (Consumer Groups)** y el protocolo de **Rebalanceo Automático (Partition Rebalance)**.

Al completar esta práctica, el alumno/ingeniero será capaz de:
1. Explicar la relación matemática entre el número de particiones de un tópico y la concurrencia máxima de consumidores.
2. Escalar horizontalmente servicios de ingesta en vivo sin interrumpir el flujo de datos.
3. Diagnosticar eventos de rebalanceo a través de logs de contenedores y métricas en Kafka UI.
4. Simular escenarios de contingencia (Chaos Engineering) provocando caídas de nodos y observando la recuperación automática sin pérdida de eventos.
5. Resolver retos técnicos prácticos para validar el entendimiento profundo del stack.

---

## 📂 Contenido del Módulo `practica_1/`

Esta carpeta contiene la ruta pedagógica completa, dividida en 4 documentos de estudio y aplicación:

| Archivo | Contenido y Enfoque |
|---|---|
| [`01_teoria_profunda_consumer_groups.md`](./01_teoria_profunda_consumer_groups.md) | **Teoría Técnica Rigurosa:** Arquitectura interna de particiones, Group Coordinator, Heartbeat thread, timeouts críticos, estrategias de asignación (Range vs RoundRobin vs CooperativeSticky) y cálculo de Lag. |
| [`02_guia_demostrativa_paso_a_paso.md`](./02_guia_demostrativa_paso_a_paso.md) | **Guía Demostrativa Paso a Paso:** Flujo guiado con comandos exactos, salidas esperadas de terminal, navegación en Kafka UI y explicación de qué ocurre en el clúster segundo a segundo. |
| [`03_laboratorio_retos_practicos.md`](./03_laboratorio_retos_practicos.md) | **Ejercicios Prácticos y Retos de Laboratorio:** 4 retos prácticos con requerimientos específicos, criterios de éxito, trampas comunes y soluciones detalladas. |
| [`04_scripts_y_comandos_utiles.md`](./04_scripts_y_comandos_utiles.md) | **Caja de Herramientas:** One-liners de Docker y Kafka CLI para inspeccionar grupos, medir lag por partición y resetear offsets en caliente. |

---

## 🛠️ Requisitos Previos
* Docker y Docker Compose en funcionamiento.
* El pipeline actual levantado y saludable (`docker compose ps` con `kafka`, `postgres`, `producer`, `consumer` y `kafka-ui` en estado `Up`).
* Acceso web a Kafka UI en el puerto `8080` (`http://localhost:8080`).

---

## 🧭 Mapa de Navegación Sugerido
1. **Paso 1:** Lee [`01_teoria_profunda_consumer_groups.md`](./01_teoria_profunda_consumer_groups.md) para asimilar los conceptos teóricos y evitar mitos comunes sobre el escalado de Kafka.
2. **Paso 2:** Sigue la [`02_guia_demostrativa_paso_a_paso.md`](./02_guia_demostrativa_paso_a_paso.md) ejecutando los comandos en tu terminal y contrastándolos con Kafka UI.
3. **Paso 3:** Enfréntate a los retos de [`03_laboratorio_retos_practicos.md`](./03_laboratorio_retos_practicos.md) para validar tu habilidad operativa de forma autónoma.


