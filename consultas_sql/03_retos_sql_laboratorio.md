# Retos de Laboratorio SQL: Ejercicios de Análisis Avanzado

**Documento:** `03_retos_sql_laboratorio.md`  
**Módulo:** Consultas y Analítica SQL  

---

## 🎯 Instrucciones Generales
Pon a prueba tus habilidades de SQL analítico sobre los datos que tu pipeline sigue ingiriendo en tiempo real.  
Cada reto cuenta con un **escenario de negocio**, las **columnas esperadas en el resultado** y **pistas técnicas** para guiar tu consulta.

*(Las soluciones completas se encuentran resguardadas en la carpeta privada `soluciones/solucion_retos_sql.md`).*

---

## 🧪 Reto SQL 1: El Top 5 de Editores Humanos Más Prolíficos

### 📌 Escenario:
El equipo editorial de Wikipedia desea reconocer a los colaboradores más activos del día, pero deben excluirse estrictamente todas las cuentas automatizadas (bots).

### 🎯 Tu Misión:
Escribe una consulta SQL que devuelva:
1. El nombre del usuario (`user_name`).
2. La cantidad total de ediciones realizadas.
3. La suma total de bytes agregados/eliminados en valor absoluto (`SUM(ABS(byte_diff))`).
4. Filtrar únicamente usuarios humanos (`bot = false`).
5. Limitar a los 5 primeros lugares con mayor número de ediciones.

---

## 🧪 Reto SQL 2: Detección de Artículos Controversiales o en Tendencia

### 📌 Escenario:
Cuando ocurre un suceso importante o hay una "guerra de ediciones", un artículo específico recibe múltiples modificaciones en muy poco tiempo en un mismo wiki.

### 🎯 Tu Misión:
Escribe una consulta SQL que identifique artículos con alta concentración de actividad:
1. Columnas: `wiki`, `title`, total de ediciones (`COUNT(*)`), y cantidad de editores distintos que han participado (`COUNT(DISTINCT user_name)`).
2. Filtrar únicamente artículos que tengan al menos **3 ediciones** registradas (`HAVING COUNT(*) >= 3`).
3. Ordenar por número de ediciones descendente y mostrar los 10 primeros.

---

## 🧪 Reto SQL 3: Auditoría Forense — Detección de Bots No Declarados

### 📌 Escenario:
Algunos desarrolladores ponen a correr scripts automáticos sin marcar la casilla oficial de bot en la API de Wikipedia.

### 🎯 Tu Misión:
Diseña una consulta que busque posibles "bots encubiertos":
1. Usuarios donde el campo `bot` sea `false`.
2. Cuyos nombres contengan el texto `bot` (sin importar mayúsculas o minúsculas, ej: `AutoBot`, `Helper_bot`, `CleanerBot`).
3. Muestra el `user_name`, el `wiki` donde operan y el total de ediciones que han efectuado.

---

## 🧪 Reto SQL 4: Comparativa Multilingüe — Los 4 Grandes Idiomas

### 📌 Escenario:
La fundación Wikimedia desea comparar la actividad entre los 4 idiomas principales: Inglés (`enwiki`), Español (`eswiki`), Francés (`frwiki`) y Alemán (`dewiki`).

### 🎯 Tu Misión:
Construye una consulta que filtre solo esos 4 wikis y calcule para cada uno:
1. Nombre del wiki.
2. Total de ediciones.
3. Porcentaje de ediciones hechas por bots dentro de ese idioma específico.
4. Promedio de bytes modificados (`AVG(byte_diff)`).
5. Ordenar por volumen total de mayor a menor.

---

## 🧪 Reto SQL 5: Métrica de Ingeniería — Medición de Latencia de Ingestión

### 📌 Escenario:
Como ingeniero de datos, debes medir con exactitud el SLA (Service Level Agreement) del pipeline: ¿cuántos segundos pasan desde que un usuario presiona "Publicar cambios" en Wikipedia (`event_timestamp`) hasta que el consumidor de Kafka lo persiste en PostgreSQL (`received_at`)?

### 🎯 Tu Misión:
Escribe una consulta de latencia que calcule:
1. Latencia promedio en segundos (`AVG(...)`).
2. Latencia mínima en segundos.
3. Latencia máxima en segundos.
4. Filtrar registros donde `received_at >= event_timestamp` para evitar distorsiones por desfase horario.

> **Pista Técnica:** En PostgreSQL puedes restar dos fechas de tipo timestamp y obtener los segundos totales usando la función:  
> `EXTRACT(EPOCH FROM (received_at - event_timestamp))`
