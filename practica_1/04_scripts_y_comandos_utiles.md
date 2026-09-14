# Caja de Herramientas y Scripts Útiles de Monitoreo

**Documento:** `04_scripts_y_comandos_utiles.md`  
**Práctica 1:** Escalabilidad Horizontal en Apache Kafka  

---

Este documento recopila comandos prácticos de una sola línea (*one-liners*) y pequeños scripts para monitorear el clúster, medir lag y depurar rebalanceos durante la práctica.

---

## 🛠️ 1. Comandos de Administración de Consumidores (Kafka CLI)

### 1.1 Listar todos los grupos de consumidores activos
```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list
```

### 1.2 Inspeccionar miembros, particiones asignadas y hosts
```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group wiki-persister-group --members
```

### 1.3 Ver estado de salud y tipo de protocolo del grupo
```bash
docker exec -it lab1-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group wiki-persister-group --state
```

---

## 📊 2. Scripts de Medición Continua (Monitoreo en Vivo)

### 2.1 Monitorear el Lag cada 2 segundos en tiempo real (PowerShell)
Ejecuta esto en una terminal para tener un panel de control en vivo del Lag de cada partición:

```powershell
while ($true) {
    Clear-Host
    Write-Host "=== ESTADO EN TIEMPO REAL: wiki-persister-group ===" -ForegroundColor Cyan
    docker exec lab1-kafka kafka-consumer-groups `
      --bootstrap-server localhost:9092 `
      --describe --group wiki-persister-group
    Start-Sleep -Seconds 2
}
```

### 2.2 Monitorear inserciones continuas en PostgreSQL por segundo (PowerShell)
Muestra la tasa exacta de registros que entran por segundo a la base de datos:

```powershell
$prev = [int](docker exec lab1-postgres psql -U wiki_user -d wiki_db -t -A -c "SELECT COUNT(*) FROM wiki_recent_changes;")
while ($true) {
    Start-Sleep -Seconds 1
    $curr = [int](docker exec lab1-postgres psql -U wiki_user -d wiki_db -t -A -c "SELECT COUNT(*) FROM wiki_recent_changes;")
    $delta = $curr - $prev
    $prev = $curr
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Total Registros: $curr | Throughput: $delta eventos/segundo" -ForegroundColor Green
}
```

---

## 🔄 3. Comandos Rápidos de Escalado con Docker Compose

### 3.1 Escalar a N consumidores
```bash
# Escalar a 2
docker compose up -d --scale consumer=2

# Escalar al óptimo (3)
docker compose up -d --scale consumer=3

# Regresar a 1
docker compose up -d --scale consumer=1 --remove-orphans
```

### 3.2 Filtrar logs para ver únicamente eventos de asignación de particiones
```bash
docker compose logs consumer | grep -E "assigned|revoked|Partitions|rebalance"
```

---

## 🔍 4. Atajos Directos en Kafka UI

Si prefieres la interfaz gráfica en tu navegador:
* **Consola Principal:** `http://localhost:8080`
* **Vista del Tópico `wiki.changes`:** `http://localhost:8080/ui/clusters/local/all-topics/wiki.changes`
* **Mensajes en vivo:** `http://localhost:8080/ui/clusters/local/all-topics/wiki.changes/messages`
* **Detalle del Grupo de Consumidores:** `http://localhost:8080/ui/clusters/local/consumer-groups/wiki-persister-group`


