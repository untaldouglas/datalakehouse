# Universidad Data Lakehouse

> Prototipo funcional de Data Lakehouse universitario: integra Moodle (LMS) y ERPNext (ERP)
> con Apache Iceberg + Nessie + MinIO como capa de almacenamiento,
> Dremio OSS como motor de consultas y Metabase para dashboards gerenciales y académicos.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Stack](https://img.shields.io/badge/Stack-Iceberg%20%7C%20Nessie%20%7C%20Dremio%20%7C%20Airflow-blue)](#arquitectura)

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────────┐
│  FUENTES                          ORQUESTACIÓN (cada 12h)            │
│                                                                      │
│  Moodle 4.3 (MySQL)  ──────────▶  Airflow 2.9  ─── DAG 1 → Bronze   │
│  ERPNext v15 (MariaDB) ─────────▶              ─── DAG 2 → Bronze   │
│                                                ─── DAG 3 → Silver   │
│                                                ─── DAG 4 → Gold/PG  │
│                                        │                             │
│                                        ▼                             │
│  ╔═══════════════════════════════════════════════════════════════╗   │
│  ║                    DATA LAKEHOUSE                             ║   │
│  ║                                                               ║   │
│  ║   MinIO (S3) ←── Iceberg Parquet ──→ Nessie 0.108 (catálogo) ║   │
│  ║   └─ lakehouse/                      └─ REST catalog /iceberg/║   │
│  ║       ├─ bronze/  (datos crudos)                              ║   │
│  ║       └─ silver/  (datos limpios)                             ║   │
│  ║                                                               ║   │
│  ║   Dremio OSS 25.0  (SQL ad-hoc sobre todas las capas)        ║   │
│  ╚═══════════════════════════════════════════════════════════════╝   │
│                                        │                             │
│                                        ▼                             │
│  PostgreSQL  universidad_analytics     ←── Gold layer (KPIs)         │
│  └─ Metabase 0.62  (dashboards)                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Capas del Lakehouse

| Capa | Tablas | Descripción |
|------|--------|-------------|
| **Bronze** | `moodle_users`, `moodle_courses`, `moodle_grades`, `moodle_enrolments`, `erpnext_students`, `erpnext_fees`, `erpnext_payments`, `erpnext_programs` | Datos crudos sin transformar, con `_etl_loaded_at` |
| **Silver** | `students`, `fees`, `payments`, `grades` | Limpios, tipados, con campos derivados (`is_overdue`, `grade_pct`, `days_overdue`…) |
| **Gold** | Materializado en PostgreSQL | KPIs precalculados: `kpi_financiero_mensual`, `kpi_academico_periodo`, facts y dims |

---

## Prerrequisitos

| Requisito | Mínimo | Recomendado |
|-----------|--------|-------------|
| Docker Desktop | ≥ 24 | última versión |
| RAM asignada a Docker | 12 GB | 16 GB |
| Espacio en disco | 8 GB | 15 GB |
| Python local | 3.9+ | 3.11 |
| Puertos libres | 3000, 8090, 9000, 9001, 9047, 19120 | — |

> **macOS/Windows**: en Docker Desktop → Settings → Resources → Memory → ajustar a 12-16 GB.

---

## Inicio Rápido (primera vez)

```bash
# 1. Clonar
git clone https://github.com/tu-usuario/universidad-datalakehouse.git
cd universidad-datalakehouse

# 2. Configurar entorno
cp .env.example .env
make gen-keys          # genera AIRFLOW_FERNET_KEY y AIRFLOW_SECRET_KEY automáticamente
# Opcional: edita .env para cambiar UNIVERSITY_NAME, NUM_STUDENTS, puertos…

# 3. Bootstrap completo (todo en un comando, ~10-15 min)
make bootstrap
```

`make bootstrap` ejecuta en secuencia:
1. Construye y levanta todos los contenedores
2. Espera a que estén saludables
3. Carga los datos de prueba (5 000 alumnos por defecto)
4. Ejecuta el pipeline ETL completo (4 DAGs)
5. Configura Dremio (fuente Nessie + vistas SQL)
6. Configura Metabase (2 dashboards con 8 cards c/u)

Al finalizar verás las URLs y credenciales de acceso.

---

## Acceso a los Servicios

| Servicio | URL | Usuario | Contraseña |
|----------|-----|---------|------------|
| **Airflow** | http://localhost:8090 | `admin` | `Admin1234!` |
| **Metabase** | http://localhost:3000 | `admin@universidad.edu` | `Admin1234!` |
| **Dremio** | http://localhost:9047 | `admin` | `Admin1234!` |
| **MinIO** | http://localhost:9001 | `minioadmin` | `minioadmin123` |
| **Nessie API** | http://localhost:19120/api/v2/config | — | sin auth |

> Todos los valores vienen de `.env` y pueden cambiarse antes del primer `make up`.

---

## Comandos Principales

```bash
# Estado y salud
make health            # Resumen completo: servicios + tablas + KPIs
make status            # Estado de los contenedores Docker
make etl-status        # Estado de las últimas ejecuciones de los 4 DAGs

# Pipeline ETL
make trigger-etl       # Dispara los 4 DAGs manualmente
make reset-etl         # Limpia historial y re-dispara el ETL
make wait-etl          # Bloquea hasta que todos los DAGs terminen (útil en CI)

# Diagnóstico del lakehouse
make iceberg-tables    # Lista tablas Iceberg con número de filas
make semantic-check    # Verifica KPIs clave en PostgreSQL
make nessie-tables     # Lista entradas en la rama main de Nessie
make minio-ls          # Lista objetos en el bucket lakehouse

# Servicios
make up                # Iniciar todos los servicios
make down              # Detener (conserva datos)
make restart           # Detener + iniciar
make logs              # Ver logs en tiempo real
make logs-service s=dremio  # Logs de un servicio específico

# Configuración (re-ejecutar si algo falla en bootstrap)
make seed              # Cargar datos de prueba
make setup-dremio      # Configurar Dremio
make setup-metabase    # Configurar Metabase

# Recuperación
make nessie-reset      # Reinicia Nessie con volumen limpio (luego: make reset-etl)
make clean             # Borra TODO (datos incluidos) — ¡irreversible!
make reset             # clean + bootstrap desde cero
```

---

## Pipeline ETL

| DAG | Schedule | Fuente → Destino | Tareas |
|-----|----------|------------------|--------|
| `01_moodle_to_bronze` | `0 0,12 * * *` | MySQL → Iceberg bronze | extract_users, courses, grades, enrolments |
| `02_erpnext_to_bronze` | `0 0,12 * * *` | MariaDB → Iceberg bronze | extract_programs, students, fees, payments |
| `03_bronze_to_silver` | `0 0,12 * * *` | Bronze → Silver Iceberg | transform_students, fees, payments, grades |
| `04_silver_to_gold` | `0 0,12 * * *` | Silver → PostgreSQL | materialize dims, financial KPIs, academic KPIs |

Los DAGs 03 y 04 usan `ExternalTaskSensor` para esperar a que los upstream terminen.

Para cambiar el intervalo de ejecución, edita en `.env`:
```bash
ETL_SCHEDULE_INTERVAL="0 */6 * * *"   # cada 6 horas
ETL_SCHEDULE_INTERVAL="@daily"         # una vez al día
```

---

## Dashboards Metabase

### Dashboard Gerencial — Ventas y Cobranza (http://localhost:3000/dashboard/2)
- Ingresos totales por programa (año actual)
- Evolución mensual de ingresos facturados vs. cobrados
- Tasa de cobranza y tasa de morosidad por programa y ciclo
- Alumnos morosos actuales por programa
- Cartera vencida total
- Distribución por modo de pago
- Ingreso promedio por alumno activo
- Avance vs. meta anual

### Dashboard Académico — Indicadores Moodle (http://localhost:3000/dashboard/3)
- Tasa de aprobación por programa
- Promedio de notas por programa y ciclo
- Top 10 cursos con mayor reprobación
- Distribución de calificaciones
- Total de alumnos por programa
- Alumnos por cohorte de ingreso
- Distribución por género
- Cursos con mayor rendimiento académico

---

## Dremio — Consultas Ad-hoc

Conéctate a http://localhost:9047 y usa SQL sobre cualquier capa Iceberg.

**Importante**: por ser tablas versionadas en Nessie, siempre incluye `AT BRANCH "main"`:

```sql
-- Alumnos activos con programa
SELECT s.name, s.student_name, p.program_name
FROM lakehouse.bronze.erpnext_students AT BRANCH "main" s
LEFT JOIN lakehouse.bronze.erpnext_programs AT BRANCH "main" p
  ON s.program = p.name
WHERE s.enabled = 1
LIMIT 100;

-- KPI financiero desde silver
SELECT program_code,
       SUM(total_amount)   AS facturado,
       SUM(paid_amount)    AS cobrado,
       SUM(pending_amount) AS pendiente
FROM lakehouse.silver.fees AT BRANCH "main"
GROUP BY program_code;

-- Usar las vistas analíticas predefinidas (sin AT BRANCH)
SELECT * FROM analytics.financial_summary;
SELECT * FROM analytics.grade_summary;
SELECT * FROM analytics.students LIMIT 50;
```

---

## Estructura del Proyecto

```
universidad-datalakehouse/
├── docker-compose.yml              # Orquestación de 10 servicios
├── .env.example                    # Template de configuración (copiar a .env)
├── Makefile                        # Comandos operacionales
├── LICENSE                         # MIT
│
├── pipelines/                      # Imagen Airflow + código ETL
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── common/
│       │   └── lakehouse.py        # Catálogo PyIceberg, schemas, upsert_iceberg()
│       ├── 01_moodle_bronze_dag.py
│       ├── 02_erpnext_bronze_dag.py
│       ├── 03_silver_transform_dag.py
│       └── 04_gold_materialize_dag.py
│   └── scripts/
│       ├── seed_data.py            # Generador de datos de prueba
│       └── setup_dremio.py         # Configura Dremio por REST API
│
├── services/
│   ├── dremio/dremio.conf
│   ├── erpnext/init/               # Schema SQL del módulo Education de ERPNext
│   ├── moodle/init/                # Schema MySQL de Moodle
│   ├── metabase/init/
│   │   ├── 00_create_databases.sql # Crea usuario analytics y BD universidad_analytics
│   │   └── 01_semantic_schema.sql  # Tablas de facts, dims y KPIs
│   └── nessie/
│       └── application.properties  # Credenciales S3 para Nessie (montado en el contenedor)
│
├── metabase/setup/
│   └── configure_metabase.py       # Crea dashboards vía Metabase REST API
│
└── docs/
    ├── architecture.md             # Decisiones de arquitectura y trade-offs
    ├── TROUBLESHOOTING.md          # Problemas conocidos y soluciones
    └── OPERACIONES.md              # Guía operacional día a día
```

---

## Configuración Personalizada

### Cambiar datos de la universidad
```bash
# En .env:
UNIVERSITY_NAME="Mi Universidad Real"
NUM_STUDENTS=1000      # 500–50000 funciona bien
TIMEZONE=America/Bogota
```

### Conectar a un ERPNext real
```bash
# En .env (descomentar y ajustar):
ERPNEXT_DB_HOST=tu-servidor-erpnext.internal
ERPNEXT_DB_PORT=3306
ERPNEXT_DB_NAME=tu_site_name
ERPNEXT_DB_USER=erpnext
ERPNEXT_DB_PASSWORD=password_seguro
```
Luego reinicia el scheduler: `make restart-service s=airflow-scheduler`

### Conectar a un Moodle real
```bash
MOODLE_DB_HOST=tu-servidor-moodle.internal
MOODLE_DB_NAME=moodle
MOODLE_DB_USER=moodle_ro
MOODLE_DB_PASSWORD=password_seguro
```

### Ajustar memoria (para equipos con menos RAM)
En `docker-compose.yml`, reduce:
```yaml
dremio:   mem_limit: 3g   # (viene en 5g)
metabase: mem_limit: 768m  # (viene en 1g)
```
Y asegúrate de que `DREMIO_MAX_MEMORY_SIZE_MB` sea ≤ 80% del mem_limit en MB.

---

## Resolución de Problemas

Ver [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) para la guía completa.

**Diagnóstico rápido:**
```bash
make health           # Vista general de todo el sistema
make etl-status       # Estado de los 4 DAGs
make iceberg-tables   # Confirma que los datos llegaron al lakehouse
make semantic-check   # Confirma que los KPIs están en PostgreSQL
```

**Problemas comunes:**

| Síntoma | Solución rápida |
|---------|-----------------|
| Contenedor `nessie` no levanta | `make nessie-reset` |
| DAGs en estado `failed` | `make reset-etl` → revisa logs en Airflow |
| Metabase no muestra datos | Verifica que `make etl-status` muestra `success` en los 4 DAGs |
| Dremio no tiene la fuente Nessie | `make setup-dremio` |
| Falta de memoria (exit code -9) | Reduce paralelo en `docker-compose.yml` o aumenta RAM de Docker |

---

## Versiones de los Componentes

| Componente | Versión | Notas |
|------------|---------|-------|
| Nessie | 0.108.0 | Mínimo requerido para Iceberg REST catalog |
| PyIceberg | 0.7.1 | Usa REST catalog (no native nessie type) |
| Apache Iceberg | 1.x | Formato de tabla |
| Airflow | 2.9.1 | LocalExecutor |
| Dremio OSS | 25.0 | |
| Metabase | 0.62 | Community Edition |
| MinIO | latest | |

> **Compatibilidad crítica**: PyIceberg 0.7.x eliminó el tipo de catálogo `nessie` nativo.
> La conexión a Nessie se hace via REST catalog (Nessie 0.108.0+ expone `/iceberg/v1/config`).
> Ver [docs/architecture.md](docs/architecture.md) para detalles.

---

## Licencia

[MIT License](LICENSE) — libre para uso, modificación y distribución.

*Desarrollado como proyecto open source para la comunidad de educación superior latinoamericana.*
