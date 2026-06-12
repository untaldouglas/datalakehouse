# Decisiones de Arquitectura

## Por qué Nessie en lugar de Polaris

Ambos son catálogos Iceberg open source, pero para este prototipo se eligió **Nessie** por:

1. **Control de versiones estilo Git** — Nessie permite crear branches de datos, útil para probar transformaciones sin afectar producción.
2. **Integración nativa con Dremio** — Dremio y Nessie son proyectos del mismo ecosistema (Dremio Inc.), con documentación y conectores maduros.
3. **Menor complejidad operacional** — Nessie corre como un servicio stateless con backend RocksDB, ideal para contenedores.
4. **PyIceberg 0.7+** tiene soporte nativo para Nessie como tipo de catálogo.

> **Para usar Polaris en su lugar**: Polaris expone una REST Catalog API compatible con Iceberg. Solo hay que cambiar el `type: "nessie"` por `type: "rest"` en `common/lakehouse.py` y apuntar al endpoint de Polaris. La lógica ETL no cambia.

## Por qué PostgreSQL como Semantic Layer

Metabase Community Edition no tiene conector nativo para Dremio. Las opciones analizadas fueron:

| Opción | Pros | Contras |
|--------|------|---------|
| **PostgreSQL semantic layer** ✓ | Nativo en Metabase, ACID, fast for aggregates | Un hop extra en el pipeline |
| Dremio JDBC (driver custom) | Consulta directa al lakehouse | Compilación manual del driver, versión-dependiente |
| Arrow Flight SQL | Estándar moderno | Metabase CE no soporta Flight |
| Metabase Enterprise | Soporte completo | Costo de licencia |

La decisión fue usar **PostgreSQL como capa semántica**, materializada por Airflow desde Iceberg. Esto además permite:
- Dashboards ultra-rápidos (no pasan por el query engine del lakehouse)
- Consultas históricas en Dremio para análisis avanzado
- Independencia entre herramienta BI y lakehouse

## Frecuencia de Actualización

El schedule `0 0,12 * * *` (cada 12 horas) se implementa así:

```
00:00 → DAG 01 (Moodle Bronze) + DAG 02 (ERPNext Bronze) en paralelo
        ↓ ExternalTaskSensor (espera a que ambos completen)
        DAG 03 (Silver Transform)
        ↓ ExternalTaskSensor
        DAG 04 (Gold + PostgreSQL materialize)

12:00 → mismo ciclo
```

La ventana de extracción incremental usa `ETL_LOOKBACK_HOURS=13` (1h de overlap) para no perder registros en caso de latencia del scheduler.

## Estructura de Buckets en MinIO

```
lakehouse/          ← Bucket principal para tablas Iceberg
  bronze/
    moodle_users/   ← Particionado por _etl_loaded_at
    moodle_grades/
    erpnext_fees/
    ...
  silver/
    students/
    fees/
    grades/
  gold/
    (en PostgreSQL, no en MinIO)

nessie-data/        ← Metadata de Nessie (RocksDB backup opcional)
bronze/             ← Buckets adicionales por capa (referencia externa)
silver/
gold/
```

## Modelo de Datos — Lineage

```
Moodle MySQL                 ERPNext MariaDB
   mdl_user                    tabStudent
   mdl_course                  tabFees
   mdl_grade_grades            tabPayment Entry
   mdl_user_enrolments         tabProgram
        │                          │
        ▼ (DAG 01, 02)             ▼
   bronze.moodle_*           bronze.erpnext_*
        │                          │
        └──────────┬───────────────┘
                   ▼ (DAG 03)
              silver.students
              silver.fees
              silver.payments
              silver.grades
                   │
                   ▼ (DAG 04)
         kpi_financiero_mensual    ← Metabase Dashboard Gerencial
         kpi_academico_periodo     ← Metabase Dashboard Académico
         fact_ingresos_matricula
         fact_calificaciones
         dim_alumno
```

## Escalabilidad

Para escalar este prototipo a producción:

1. **Dremio** → Agregar executor nodes (Dremio soporta clustering en Dremio Enterprise o usar la versión OSS con múltiples executors vía `services.executor.enabled: true` en nodos adicionales)
2. **MinIO** → Migrar a MinIO Distributed Mode o AWS S3 (solo cambiar endpoint en `.env`)
3. **Nessie** → Cambiar backend de `ROCKSDB` a `JDBC` (PostgreSQL) para HA
4. **Airflow** → Cambiar `LocalExecutor` a `CeleryExecutor` con Redis para paralelismo real
5. **Metabase** → Metabase Cloud o Enterprise para mejor performance y RBAC

## Seguridad (para producción)

- Todos los passwords en `.env` deben ser secrets gestionados (AWS Secrets Manager, HashiCorp Vault)
- Nessie debe habilitarse con autenticación (`NESSIE_SERVER_AUTHENTICATION_ENABLED=true`)
- MinIO debe configurarse con TLS
- Dremio debe configurarse detrás de un reverse proxy (Nginx/Traefik) con TLS
- Los usuarios de BD deben tener permisos mínimos (solo `SELECT` para el ETL user)
