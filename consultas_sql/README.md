# Módulo de Consultas y Analítica SQL sobre Datos de Streaming

**Proyecto:** Pipeline de Streaming Wikimedia + Kafka + PostgreSQL  
**Objetivo:** Conectarse a la base de datos relacional persistida, dominar herramientas de consulta (CLI y visuales) y realizar analítica exploratoria y retos SQL sobre datos ingeridos en tiempo real.

---

## 📂 Contenido del Módulo `consultas_sql/`

| Archivo | Contenido |
|---|---|
| [`01_guia_conexion_paso_a_paso.md`](./01_guia_conexion_paso_a_paso.md) | **Guía de Conexión Exhaustiva:** Las 3 opciones detalladas: Terminal nativa (`psql`), Extensiones de IDE y Clientes gráficos (DBeaver, DataGrip, pgAdmin). Incluye trucos de terminal como `\watch` para refresco automático. |
| [`02_consultas_analiticas_en_vivo.md`](./02_consultas_analiticas_en_vivo.md) | **Consultas Analíticas en Vivo:** Queries reales explicadas: distribución de tráfico por wiki, ratio bots vs humanos, volumen de bytes y detección de picos. |
| [`03_retos_sql_laboratorio.md`](./03_retos_sql_laboratorio.md) | **Retos Prácticos de SQL:** 5 ejercicios analíticos de negocio con criterios de aceptación y pistas para resolver de forma autónoma. |

---

## ⚡ ¿Son estas consultas en "Tiempo Real"?

### La distinción técnica clave:
* **Ingesta en Tiempo Real (Push continuo):** Sí. El consumidor de Kafka inserta lotes de eventos continuamente en la tabla `wiki_recent_changes`. La latencia entre la edición real en Wikipedia y su inserción en PostgreSQL es de **menos de 3 segundos**.
* **Consulta bajo Demanda (Pull / Request-Response):** Por defecto, SQL tradicional responde a una petición puntual del usuario.
* **El truco maestro de `psql` (`\watch`):** Para convertir una consulta SQL tradicional en un **dashboard en vivo que se auto-actualiza en tiempo real**, puedes usar el comando nativo `\watch 2` dentro de `psql`. PostgreSQL re-ejecutará la consulta cada 2 segundos y redibujará la pantalla automáticamente.
