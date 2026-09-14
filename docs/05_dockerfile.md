# 📄 Documentación: `Dockerfile`

**Archivo:** `Dockerfile`
**Propósito:** Define la **imagen Docker personalizada** que comparten el producer y el consumer. Es el blueprint (plano) para construir un contenedor Python con todas las dependencias del proyecto preinstaladas.

---

## ¿Qué es un Dockerfile?

Un `Dockerfile` es un archivo de instrucciones que Docker lee para construir una imagen. Una imagen es como una "fotografía" de un sistema de archivos con un sistema operativo, runtime y aplicación listos para ejecutarse. Cada instrucción del Dockerfile crea una **capa** (layer) en la imagen. Docker cachea estas capas para acelerar builds posteriores.

---

## Análisis Línea por Línea

```dockerfile
FROM python:3.11-slim
```
- `FROM`: Instrucción base. Toda imagen Docker parte de otra imagen (excepto `scratch`).
- `python:3.11-slim`: Imagen oficial de Python 3.11 en su variante `slim`.
  - **Por qué `3.11`**: Versión estable con excelente rendimiento. Las f-strings tipadas que usa el código (`f"..."`) son nativas desde Python 3.6.
  - **Por qué `slim`**: La variante estándar `python:3.11` ocupa ~900 MB. La variante `slim` ocupa ~130 MB porque elimina herramientas de desarrollo que no se necesitan en producción (compiladores, debuggers, docs). Solo incluye lo mínimo para ejecutar Python.
  - Alternativa aún más pequeña: `python:3.11-alpine` (~50 MB), pero Alpine usa `musl` en vez de `glibc` lo que puede causar problemas con `psycopg2-binary` que necesita `glibc`.

```dockerfile
WORKDIR /app
```
- `WORKDIR`: Establece el directorio de trabajo para todas las instrucciones posteriores (`RUN`, `COPY`, `CMD`, etc.).
- Si `/app` no existe, Docker lo crea automáticamente.
- Todas las rutas relativas en instrucciones posteriores son relativas a `/app`.
- **¿Por qué no usar `/`?** Por convención y seguridad: `/` es el root del sistema, colocar archivos ahí directamente es mala práctica. `/app` es el directorio estándar de aplicaciones en contenedores.

```dockerfile
COPY requirements.txt .
```
- `COPY ORIGEN DESTINO`: Copia archivos desde el **contexto de build** (tu máquina local) al sistema de archivos de la imagen.
- `requirements.txt` es el origen (relativo al directorio donde está el Dockerfile).
- `.` es el destino (el directorio actual dentro del contenedor, que es `/app` gracias al WORKDIR).
- **¿Por qué copiar requirements ANTES del código fuente?** Optimización de caché de capas de Docker. Si copias el código y requirements juntos, cualquier cambio en el código invalida la caché del pip install. Al separarlo, Docker solo reinstala las dependencias cuando `requirements.txt` cambia; si solo cambia el código Python, reutiliza la capa cacheada del pip install (mucho más rápido).

```dockerfile
RUN pip install --no-cache-dir -r requirements.txt
```
- `RUN`: Ejecuta un comando **durante el build** de la imagen (no en tiempo de ejecución).
- `pip install -r requirements.txt`: Instala todas las librerías listadas en `requirements.txt`.
- `--no-cache-dir`: **Importante para reducir el tamaño de la imagen.** Por defecto `pip` guarda un caché de los paquetes descargados en `~/.cache/pip`. En una imagen Docker este caché no sirve de nada (no harás `pip install` de nuevo en el contenedor en ejecución) y ocupa espacio innecesario.

```dockerfile
COPY producer.py consumer.py .
```
- Copia los dos scripts Python al directorio `/app` del contenedor.
- Se hace **después** del pip install para aprovechar el caché de Docker (explicado arriba).
- Ambos archivos se copian juntos porque los dos servicios (producer y consumer) usan esta misma imagen; el `docker-compose.yml` decide cuál ejecutar con el parámetro `command`.

```dockerfile
CMD ["python"]
```
- `CMD`: Comando **por defecto** que se ejecuta cuando se inicia el contenedor.
- Aquí simplemente arranca el intérprete Python interactivo (`python` sin argumentos).
- **Este comando NUNCA se ejecuta en la práctica** porque `docker-compose.yml` lo sobreescribe con:
  ```yaml
  # Para el producer:
  command: python -u producer.py
  # Para el consumer:
  command: python -u consumer.py
  ```
- El `CMD ["python"]` es un placeholder que deja claro que esta imagen está diseñada para ejecutar Python, pero la instrucción específica la da el compose.

---

## Diferencia entre RUN, CMD y ENTRYPOINT

| Instrucción | Cuándo ejecuta | Puede sobreescribirse |
|---|---|---|
| `RUN` | Durante el build de la imagen | No aplica |
| `CMD` | Al iniciar el contenedor (por defecto) | Sí, con `command:` en compose |
| `ENTRYPOINT` | Al iniciar el contenedor (siempre) | Solo con `--entrypoint` |

---

## Capas de la Imagen Resultante

```
┌─────────────────────────────────────────────────────────┐
│ Capa 4: COPY producer.py consumer.py                    │ ← Tus scripts
├─────────────────────────────────────────────────────────┤
│ Capa 3: RUN pip install requirements.txt                │ ← confluent-kafka, psycopg2, requests
├─────────────────────────────────────────────────────────┤
│ Capa 2: COPY requirements.txt                           │ ← Lista de dependencias
├─────────────────────────────────────────────────────────┤
│ Capa 1: FROM python:3.11-slim                           │ ← Python 3.11 + OS base
└─────────────────────────────────────────────────────────┘
```

Cuando modificas `producer.py` y haces rebuild, Docker:
1. ✅ Reutiliza caché: FROM (capa 1)
2. ✅ Reutiliza caché: COPY requirements.txt (capa 2)
3. ✅ Reutiliza caché: pip install (capa 3) ← ¡Ahorra 30-60 segundos!
4. 🔄 Reconstruye: COPY producer.py consumer.py (capa 4)

---

## Contenido del Contenedor Final

```
/app/
  ├── requirements.txt        ← Copiado en COPY requirements.txt
  ├── producer.py             ← Copiado en COPY producer.py consumer.py
  └── consumer.py             ← Copiado en COPY producer.py consumer.py

/usr/local/lib/python3.11/    ← Librerías instaladas por pip
  ├── confluent_kafka/        ← Cliente Kafka
  ├── psycopg2/               ← Driver PostgreSQL
  └── requests/               ← HTTP client
```
