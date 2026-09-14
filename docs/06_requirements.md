# 📄 Documentación: `requirements.txt`

**Archivo:** `requirements.txt`
**Propósito:** Lista exacta de las dependencias Python (librerías de terceros) que el proyecto necesita, con sus versiones exactas fijadas. Es el contrato de dependencias del proyecto.

---

## ¿Por qué fijar versiones exactas?

```
confluent-kafka==2.4.0
psycopg2-binary==2.9.9
requests==2.32.3
```

El operador `==` fija la versión **exacta**. Esto garantiza que:
1. **Reproducibilidad**: Cualquier persona (o servidor de CI/CD) que ejecute `pip install -r requirements.txt` obtendrá exactamente las mismas versiones.
2. **Estabilidad**: No hay sorpresas si el autor de una librería publica una nueva versión con cambios que rompan la compatibilidad.
3. **Auditoría de seguridad**: Las herramientas de seguridad pueden escanear versiones exactas para detectar vulnerabilidades conocidas.

> Si usaras `confluent-kafka>=2.0.0`, podría instalarse la versión `3.x` en el futuro con cambios incompatibles.

---

## Análisis de Cada Dependencia

### `confluent-kafka==2.4.0`

**¿Qué es?** El cliente oficial de Confluent para Apache Kafka en Python.

**¿Por qué esta y no otras alternativas?**

| Librería | Pros | Contras |
|---|---|---|
| `confluent-kafka` | Basada en `librdkafka` (C), muy alta performance, oficial de Confluent, soporte empresarial | Requiere compilar o tener binarios de librdkafka |
| `kafka-python` | Pura Python, fácil de instalar | 3-5x más lenta, menos features |
| `aiokafka` | Async/await nativo | Solo para código async |

**Componentes que usa el proyecto:**
- `Producer`: Clase del productor (`producer.py`)
- `Consumer`: Clase del consumidor (`consumer.py`)
- `KafkaError`: Constantes de errores (`consumer.py`)

**¿Qué incluye internamente?** `confluent-kafka` es un wrapper Python sobre `librdkafka`, una librería en **C** ultra-optimizada. Maneja internamente:
- Compresión de mensajes (snappy, gzip, lz4, zstd)
- Batching y linger
- Reintentos automáticos
- Gestión de metadatos del clúster
- Rebalanceo de consumer groups
- Commits de offsets

---

### `psycopg2-binary==2.9.9`

**¿Qué es?** El adaptador PostgreSQL más usado y maduro en el ecosistema Python.

**¿Qué hace?** Permite ejecutar queries SQL contra PostgreSQL desde Python. Maneja:
- Conexiones TCP a PostgreSQL
- Serialización/deserialización de tipos Python↔SQL
- Gestión de transacciones
- Prepared statements (queries parametrizadas)
- Cursors para iterar resultados

**¿Por qué `psycopg2-binary` y no `psycopg2`?**

| Variante | Descripción | Instalación |
|---|---|---|
| `psycopg2` | Fuente: requiere compilar contra libpq | Necesita `libpq-dev`, `gcc`, `python3-dev` en el sistema |
| `psycopg2-binary` | Incluye libpq pre-compilada | Solo `pip install`, sin dependencias del sistema |

En un contenedor `python:3.11-slim` (sin compiladores), la variante `-binary` es la única opción práctica sin agregar capas extra al Dockerfile.

**Funciones que usa el proyecto:**
- `psycopg2.connect()`: Crear conexión
- `connection.cursor()`: Crear cursor para ejecutar queries
- `psycopg2.extras.execute_batch()`: Inserción eficiente en lote (de `psycopg2.extras`)
- `cursor.close()`, `connection.close()`: Liberar recursos
- `connection.commit()`, `connection.rollback()`: Control de transacciones

---

### `requests==2.32.3`

**¿Qué es?** La librería HTTP más popular de Python. Su eslogan: "HTTP for Humans".

**¿Para qué se usa en este proyecto?** Exclusivamente en `producer.py` para conectarse al endpoint SSE de Wikimedia:
```python
response = requests.get(WIKIMEDIA_STREAM_URL, headers=headers, stream=True, timeout=30)
```

**Características clave usadas:**
- `stream=True`: Mantiene la conexión HTTP abierta y permite leer la respuesta línea a línea indefinidamente (crucial para Server-Sent Events)
- `headers={}`: Añade el User-Agent requerido por Wikimedia
- `timeout=30`: Timeout de conexión inicial

**¿Por qué `requests` y no `httpx` o `urllib`?**
- `urllib` (stdlib): Más verbosa, sin soporte nativo para streaming limpio
- `httpx`: Más moderna, soporte async, pero overkill para este uso síncrono
- `requests`: Ampliamente conocida, simple, excelente soporte para streaming HTTP

---

## Cómo instalar manualmente (fuera de Docker)

```bash
# Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt

# Verificar instalación
pip list | grep -E "confluent|psycopg2|requests"
```

## Actualizar versiones (con cuidado)

```bash
# Ver si hay versiones nuevas disponibles
pip install pip-review
pip-review --local

# Actualizar una dependencia específica
pip install confluent-kafka==2.5.0
pip freeze > requirements.txt  # Actualiza el archivo
```
