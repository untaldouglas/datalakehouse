# Universidad Data Lakehouse

> Prototipo funcional de Data Lakehouse universitario con integración Moodle + ERPNext,  
> usando Apache Iceberg, Nessie, MinIO y Dremio como motor de datos,  
> y Metabase para dashboards gerenciales y académicos.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](docker-compose.yml)

---

## Arquitectura

```
┌────────────────────────────────────────────────────────────────────┐
│  FUENTES TRANSACCIONALES          ORQUESTACIÓN (cada 12h)          │
│                                                                    │
│  ┌─────────────────┐              ┌──────────────────────────────┐ │
│  │  Moodle 4.3     │──────────────▶  Apache Airflow 2.9          │ │
│  │  (MySQL 8)      │              │  DAG 1: moodle → bronze      │ │
│  └─────────────────┘              │  DAG 2: erpnext → bronze     │ │
│                                   │  DAG 3: bronze → silver      │ │
│  ┌─────────────────┐              │  DAG 4: silver → gold + PG   │ │
│  │  ERPNext v15    │──────────────▶                              │ │
│  │  (MariaDB 10.6) │              └──────────────┬───────────────┘ │
│  └─────────────────┘                             │                 │
│                                                  ▼                 │
│  ╔══════════════════════════════════════════════════════════════╗  │
│  ║                  DATA LAKEHOUSE                              ║  │
│  ║                                                              ║  │
│  ║  MinIO (S3)   ←──── Iceberg Tables ────→   Nessie Catalog   ║  │
│  ║  ┌──────────┐       ┌──────────────┐       ┌─────────────┐  ║  │
│  ║  │ bronze/  │       │ silver/      │       │ Git-like    │  ║  │
│  ║  │ silver/  │       │ gold/        │       │ versioning  │  ║  │
│  ║  │ gold/    │       │ (Parquet)    │       │ for data    │  ║  │
│  ║  └──────────┘       └──────────────┘       └─────────────┘  ║  │
│  ║                                                              ║  │
│  ║                    Dremio OSS 25.0                           ║  │
│  ║              (Query Engine · ad-hoc SQL)                     ║  │
│  ╚══════════════════════════════════════════════════════════════╝  │
│                                   │                                │
│                                   ▼                                │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  PostgreSQL Semantic Layer (universidad_analytics)          │   │
│  │  kpi_financiero_mensual · kpi_academico_periodo · facts     │   │
│  └──────────────────────────┬─────────────────────────────────┘   │
│                             │                                      │
│                             ▼                                      │
│  ┌──────────────────────────────────────┐                         │
│  │  Metabase                            │                         │
│  │  · Dashboard Gerencial (financiero)  │                         │
│  │  · Dashboard Académico (Moodle)      │                         │
│  └──────────────────────────────────────┘                         │
└────────────────────────────────────────────────────────────────────┘
```

## Capas del Lakehouse

| Capa       | Tablas Iceberg                                          | Descripción |
|------------|----------------------------------------------------------|-------------|
| **Bronze** | `moodle_users`, `moodle_courses`, `moodle_grades`, `moodle_enrolments`, `erpnext_students`, `erpnext_fees`, `erpnext_payments`, `erpnext_programs` | Datos crudos, sin transformar |
| **Silver** | `students`, `fees`, `payments`, `grades` | Datos limpios, tipados, con campos derivados (is_overdue, grade_pct…) |
| **Gold**   | Materializado en PostgreSQL semantic layer | KPIs pre-calculados listos para dashboards |

---

## Prerrequisitos

- **Docker Desktop** ≥ 24 (o Docker Engine + Compose v2)
- **RAM disponible**: mínimo 12 GB asignados a Docker (16 GB recomendado)
- **Espacio en disco**: ~8 GB
- **Puertos libres**: 3000, 8080, 8090, 9000, 9001, 9047, 19120

---

## Inicio Rápido

```bash
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/universidad-datalakehouse.git
cd universidad-datalakehouse

# 2. Configurar el entorno
cp .env.example .env
# Editar .env con tus valores (opcional — los valores por defecto funcionan)

# 3. Iniciar todos los servicios
make up

# 4. Esperar a que Moodle termine su instalación inicial (~5-10 min)
make logs-service s=moodle
# Continuar cuando veas "Moodle installation completed"

# 5. Cargar datos de prueba (5000 alumnos)
make seed

# 6. Configurar Dremio (fuentes + vistas virtuales)
make setup-dremio

# 7. Ejecutar el pipeline ETL por primera vez
make trigger-full-etl

# 8. Configurar dashboards en Metabase
python metabase/setup/configure_metabase.py

# 9. Abrir todo
make open-all
```

---

## Acceso a los Servicios

| Servicio     | URL                              | Usuario   | Contraseña   |
|-------------|----------------------------------|-----------|--------------|
| **Moodle**  | http://localhost:8080            | admin     | Admin1234!   |
| **Airflow** | http://localhost:8090            | admin     | Admin1234!   |
| **MinIO**   | http://localhost:9001            | minioadmin | minioadmin123 |
| **Dremio**  | http://localhost:9047            | admin     | Admin1234!   |
| **Metabase**| http://localhost:3000            | (configurar en primer acceso) |
| **Nessie**  | http://localhost:19120/api/v2    | —         | (sin auth)   |

> Todos los valores están en `.env` y pueden cambiarse antes de levantar los servicios.

---

## Pipeline ETL (Airflow DAGs)

| DAG | Fuente | Destino | Descripción |
|-----|--------|---------|-------------|
| `01_moodle_to_bronze` | MySQL (Moodle) | Iceberg `bronze.*` | Extracción incremental cada 12h |
| `02_erpnext_to_bronze` | MariaDB (ERPNext) | Iceberg `bronze.*` | Extracción incremental cada 12h |
| `03_bronze_to_silver` | Iceberg `bronze.*` | Iceberg `silver.*` | Limpieza, tipos, campos derivados |
| `04_silver_to_gold` | Iceberg `silver.*` | PostgreSQL `universidad_analytics` | KPIs y facts para Metabase |

El schedule por defecto es `0 0,12 * * *` (medianoche y mediodía). Se puede cambiar en `.env`:

```bash
ETL_SCHEDULE_INTERVAL="0 */6 * * *"   # Cada 6 horas
ETL_SCHEDULE_INTERVAL="0 8 * * *"     # Solo a las 8am
```

---

## Dashboards en Metabase

### Dashboard Gerencial — Ventas y Cobranza
KPIs financieros de la universidad:
- Ingresos totales facturados vs. cobrados por programa y mes
- Tasa de cobranza y tasa de morosidad
- Cartera vencida por programa
- Distribución por modo de pago (efectivo, transferencia, tarjeta…)
- Ingreso promedio por alumno activo
- Avance de ingresos vs. meta anual

### Dashboard Académico — Indicadores Moodle
Indicadores de desempeño académico:
- Tasa de aprobación/reprobación por programa
- Promedio de notas por curso y ciclo
- Top 10 cursos con mayor reprobación
- Distribución de calificaciones (rangos)
- Alumnos activos por programa y género
- Matrícula por cohorte de ingreso

---

## Estructura del Proyecto

```
universidad-datalakehouse/
├── docker-compose.yml          # Orquestación de todos los servicios
├── .env.example                # Template de configuración
├── .env                        # Configuración activa (no commitear)
├── Makefile                    # Comandos rápidos
├── LICENSE                     # MIT
├── README.md
│
├── pipelines/                  # Apache Airflow + Python
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│   │   ├── common/lakehouse.py # Utilidades PyIceberg compartidas
│   │   ├── 01_moodle_bronze_dag.py
│   │   ├── 02_erpnext_bronze_dag.py
│   │   ├── 03_silver_transform_dag.py
│   │   └── 04_gold_materialize_dag.py
│   └── scripts/
│       ├── seed_data.py        # Generador de 5000 alumnos
│       └── setup_dremio.py     # Configura Dremio por API REST
│
├── services/
│   ├── dremio/dremio.conf      # Configuración de Dremio OSS
│   ├── erpnext/init/           # Schema SQL del módulo Education
│   └── metabase/init/          # Schema PostgreSQL semantic layer
│
├── metabase/setup/
│   └── configure_metabase.py  # Crea dashboards vía Metabase API
│
└── docs/
    └── architecture.md         # Decisiones de arquitectura
```

---

## Comandos Makefile

```bash
make up              # Iniciar todos los servicios
make down            # Detener (conserva datos)
make status          # Ver estado de contenedores
make logs            # Ver logs en tiempo real
make logs-service s=dremio  # Logs de un servicio específico
make seed            # Cargar datos de prueba (5000 alumnos)
make setup-dremio    # Configurar Dremio (ejecutar una vez)
make trigger-etl     # Disparar pipeline ETL manualmente
make nessie-tables   # Ver tablas Iceberg en Nessie
make minio-ls        # Ver buckets en MinIO
make open-all        # Abrir todos los servicios en el browser
make clean           # Eliminar TODO (incluye datos) — ¡cuidado!
```

---

## Configuración Avanzada

### Cambiar el número de alumnos de prueba
```bash
# En .env:
NUM_STUDENTS=1000   # Para pruebas rápidas
NUM_STUDENTS=10000  # Para carga más realista
```

### Conectar a un ERPNext real (producción)
El prototipo usa una base de datos MariaDB con el esquema de ERPNext simulado.  
Para conectar a un ERPNext real, actualiza en `.env`:
```bash
ERPNEXT_DB_HOST=tu-servidor-erpnext
ERPNEXT_DB_PORT=3306
ERPNEXT_DB_NAME=nombre_de_tu_site  # e.g. universidad_erp
ERPNEXT_DB_USER=root
ERPNEXT_DB_PASSWORD=tu_password
```

### Conectar a un Moodle real (producción)
```bash
MOODLE_DB_HOST=tu-servidor-moodle
MOODLE_DB_NAME=moodle
MOODLE_DB_USER=moodle
MOODLE_DB_PASSWORD=tu_password
```

### Acceso a Dremio para consultas ad-hoc
Una vez configurado, Dremio expone las siguientes vistas:
```sql
-- Ver alumnos
SELECT * FROM lakehouse.bronze.erpnext_students LIMIT 100;

-- KPI financiero desde silver
SELECT program_code, SUM(total_amount), SUM(paid_amount)
FROM lakehouse.silver.fees
GROUP BY program_code;

-- Rendimiento académico
SELECT * FROM analytics.grade_summary;
```

---

## Solución de Problemas

**Moodle tarda mucho en iniciar**  
Normal — la primera instalación toma 5-10 minutos. Monitorea con `make logs-service s=moodle`.

**Dremio no aparece la fuente Nessie**  
Ejecuta `make setup-dremio` después de que Dremio esté completamente iniciado (ver `make status`).

**Airflow DAGs en estado "failed"**  
Verifica que el seed de datos se ejecutó correctamente: `make seed`. Luego dispara manualmente: `make trigger-full-etl`.

**Metabase no muestra datos**  
El pipeline ETL debe haber corrido al menos una vez. Verifica en Airflow que los 4 DAGs completaron exitosamente.

**Problemas de memoria**  
Si Docker tiene menos de 12GB asignados, reduce los límites en `docker-compose.yml`:
- `dremio`: de `4g` a `2g`
- `metabase`: de `1536m` a `1g`

---

## Contribuir

1. Fork del repositorio
2. Crea una rama: `git checkout -b feature/mi-mejora`
3. Commit: `git commit -m "Agrega soporte para X"`
4. Push: `git push origin feature/mi-mejora`
5. Abre un Pull Request

### Ideas para contribuciones
- Conector para PostgreSQL como fuente adicional (Odoo, SIS propio)
- DAG para detección de anomalías académicas
- Alertas automáticas de morosidad vía email
- Dashboard de retención y deserción por cohorte
- Soporte para Polaris como catálogo alternativo a Nessie

---

## Licencia

[MIT License](LICENSE) — libre para uso, modificación y distribución.

---

*Desarrollado como proyecto open source para la comunidad de educación superior latinoamericana.*
