# Guía Exhaustiva de Conexión a PostgreSQL: De la Terminal a Clientes Gráficos

**Documento:** `01_guia_conexion_paso_a_paso.md`  
**Módulo:** Consultas y Analítica SQL  

---

## 🔑 Parámetros Universales de Conexión

Independientemente del método que elijas, los parámetros de conexión definidos en `docker-compose.yml` son:

| Parámetro | Valor |
|---|---|
| **Host** | `localhost` o `127.0.0.1` |
| **Puerto** | `5432` |
| **Base de Datos** | `wikidb` |
| **Usuario** | `wikiuser` |
| **Contraseña** | `wikipassword` |
| **Contenedor Docker** | `lab1-postgres` |

---

# 🚀 OPCIÓN 1: Consola Interactiva `psql` (Recomendada al 100%)

Esta es la opción estándar preferida por ingenieros de datos y administradores de sistemas. No requiere instalar nada en tu computadora local porque utiliza el binario de PostgreSQL que ya corre dentro del contenedor Docker.

### 1.1 Comando de Conexión Directa
En tu terminal (WSL, Linux, macOS o PowerShell):
```bash
docker exec -it lab1-postgres psql -U wikiuser -d wikidb
```

Aparecerá el prompt interactivo:
```text
psql (16.x)
Type "help" for help.

wikidb=#
```

---

### 1.2 Comandos Esenciales de Navegación en `psql`

| Comando | Acción |
|---|---|
| `\dt` | Lista todas las tablas del esquema actual (`wiki_recent_changes`). |
| `\d wiki_recent_changes` | Muestra columnas, tipos de datos y restricciones de la tabla. |
| `\d+ wiki_recent_changes` | Versión detallada: muestra tamaño en disco, índices y comentarios. |
| `\timing` | Activa/desactiva el cronómetro interno. Muestra cuánto tarda cada consulta en milisegundos. |
| `\x auto` | Modo expandido: si una fila tiene muchas columnas, la muestra en vertical para que no se desborde la pantalla. |
| `\q` | Sale de la sesión y regresa a tu terminal. |

---

### 1.3 🌟 El Arma Secreta de la Terminal: El Comando `\watch` (Dashboard en Tiempo Real)

¿Sabías que `psql` puede convertir cualquier consulta SQL en un panel en vivo que se refresca solo?

1. Escribe cualquier consulta SQL (¡sin punto y coma final!):
   ```sql
   SELECT COUNT(*) FROM wiki_recent_changes
   ```
2. Presiona Enter y escribe:
   ```text
   \watch 2
   ```
3. **¿Qué sucede?** La pantalla se limpia y cada **2 segundos** la consulta se vuelve a ejecutar automáticamente, mostrando el conteo actualizado con la hora exacta en el encabezado.
4. Para detener la visualización continua, solo presiona `Ctrl + C`.

---

# 🖥️ OPCIÓN 2: Extensión Integrada en VS Code / Antigravity IDE

Si prefieres no salir del editor y tener autocompletado visual:

### 2.1 Instalación
1. Presiona `Ctrl + Shift + X` para abrir el panel de extensiones.
2. Busca: **`Database Client`** (autor: *cweijan*) o **`PostgreSQL`** (autor: *Chris Kolkman*).
3. Haz clic en **Install**.

### 2.2 Configuración
1. En la barra lateral izquierda aparecerá un nuevo ícono de base de datos (un barril). Haz clic en él.
2. Haz clic en **Create Connection** (o el botón `+`) y selecciona **PostgreSQL**.
3. Completa los campos:
   * **Host:** `127.0.0.1`
   * **Port:** `5432`
   * **Username:** `wikiuser`
   * **Password:** `wikipassword`
   * **Database:** `wikidb`
4. Haz clic en **Test Connection** (debe mostrar un mensaje de éxito verde) y luego en **Save**.

### 2.3 Uso
* Podrás explorar el árbol: `wikidb` ➔ `public` ➔ `Tables` ➔ `wiki_recent_changes`.
* Haz clic derecho sobre la tabla y selecciona **Query Table** o **New Query** para abrir un archivo `.sql` con botón de ejecución interactivo.

---

# 🎨 OPCIÓN 3: Clientes Gráficos Dedicados (DBeaver / DataGrip / TablePlus / pgAdmin)

Son aplicaciones de escritorio completas especializadas en análisis de datos, visualización de esquemas y exportación a CSV/Excel.

### 3.1 Pasos en DBeaver (Cliente Open Source Gratuito)
1. Descarga e instala DBeaver Community (si no lo tienes).
2. Menú superior: **Base de Datos** ➔ **Nueva Conexión**.
3. Selecciona **PostgreSQL** y haz clic en **Siguiente**.
4. En la pestaña *Principal*:
   * **Host:** `localhost`
   * **Puerto:** `5432`
   * **Base de datos:** `wikidb`
   * **Autenticación:** *Base de datos nativa*
   * **Usuario:** `wikiuser`
   * **Contraseña:** `wikipassword`
5. Presiona **Probar conexión...** (si DBeaver te pide descargar los controladores JDBC de Postgres, pulsa *Descargar* automáticamente).
6. Presiona **Finalizar**.
7. Presiona `F3` o `Ctrl + ]` para abrir un Editor SQL y ejecutar tus consultas.

---

## ⚖️ Comparativa: ¿Cuál opción te conviene más?

| Criterio | Opción 1: Terminal (`psql`) | Opción 2: Extensión IDE | Opción 3: DBeaver / GUI |
|---|---|---|---|
| **Consumo de memoria RAM** | **~0 MB extra** (corre en el contenedor) | Ligero (~50 MB) | Pesado (~300-600 MB) |
| **Velocidad de inicio** | **Instantáneo** (1 segundo) | Rápido | Lento (tiempo de arranque de la app) |
| **Modo Streaming en vivo** | **Sí** (comando nativo `\watch`) | No (manual) | No (manual o plugins de pago) |
| **Facilidad para gráficos** | Solo texto / ASCII | Básico | Excelente (gráficos de barras y torta) |
| **Relevancia profesional** | **Imprescindible** (en producción o clouds no hay GUI) | Cómodo para desarrollo | Ideal para analistas de negocio |
