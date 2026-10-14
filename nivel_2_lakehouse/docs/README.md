# Documentacion Tecnica — Nivel 2: Streaming Lakehouse

Esta carpeta contiene la documentacion tecnica exhaustiva de cada archivo
del proyecto, explicando linea por linea su funcionamiento, las decisiones
de diseno y las implicaciones arquitectonicas.

## Contenido

| Archivo | Descripcion |
|---|---|
| [01_docker-compose.md](./01_docker-compose.md) | Orquestacion de los 8 servicios Docker del Lakehouse |
| [02_bronze_streaming.md](./02_bronze_streaming.md) | Script PySpark de ingesta streaming Kafka a Bronze |
| [03_silver_batch.md](./03_silver_batch.md) | Script PySpark de transformacion batch Bronze a Silver |
| [04_gold_aggregations.md](./04_gold_aggregations.md) | Script PySpark de agregacion batch Silver a Gold |
| [05_arquitectura_medallion_implementada.md](./05_arquitectura_medallion_implementada.md) | Analisis completo de la Arquitectura Medallion implementada |
| [06_guia_puertos_y_servicios_red.md](./06_guia_puertos_y_servicios_red.md) | Guia exhaustiva de puertos, interfaces web y conectividad de red |

## Orden de Lectura Sugerido

1. Comenzar por `05_arquitectura_medallion_implementada.md` para obtener la vision completa.
2. Continuar con `01_docker-compose.md` y `06_guia_puertos_y_servicios_red.md` para entender la infraestructura y redes.
3. Seguir con los scripts en orden: Bronze (02), Silver (03), Gold (04).
