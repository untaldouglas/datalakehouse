# Manual de Operaciones

Guía práctica para operar el Universidad Data Lakehouse en el día a día.

---

## Índice

1. [Primera puesta en marcha](#1-primera-puesta-en-marcha)
2. [Arranque diario](#2-arranque-diario)
3. [Verificación de salud](#3-verificación-de-salud)
4. [Gestión del pipeline ETL](#4-gestión-del-pipeline-etl)
5. [Gestión del lakehouse Iceberg](#5-gestión-del-lakehouse-iceberg)
6. [Gestión de Dremio](#6-gestión-de-dremio)
7. [Gestión de Metabase](#7-gestión-de-metabase)
8. [Copias de seguridad](#8-copias-de-seguridad)
9. [Actualización de componentes](#9-actualización-de-componentes)
10. [Agregar una nueva fuente de datos](#10-agregar-una-nueva-fuente-de-datos)
11. [Proceso de desarrollo — validaciones previas](#11-proceso-de-desarrollo--validaciones-previas)

---

## 1. Primera Puesta en Marcha

### Prerequisitos
```bash
# Verificar Docker
docker --version      # debe ser ≥ 24
docker compose version # debe ser ≥ 2.0

# Verificar Python local (para setup-metabase)
python3 --version     # debe ser ≥ 3.9
pip3 install requests  # único requisito local
```

### Pasos de instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/universidad-datalakehouse.git
cd universidad-datalakehouse

# 2. Crear y configurar .env
cp .env.example .env
make gen-keys         # genera AIRFLOW_FERNET_KEY y AIRFLOW_SECRET_KEY

# Editar opciones de universidad (opcional):
nano .env
# → UNIVERSITY_NAME, NUM_STUDENTS, TIMEZONE

# 3. Verificar configuración
make check-env

# 4. Bootstrap completo (único comando — ~15 min la primera vez)
make bootstrap
```

### ¿Qué hace `make bootstrap`?

| Paso | Duración aprox. | Descripción |
|------|-----------------|-------------|
| Construcción de imágenes | 3-5 min | Descarga y construye la imagen de Airflow |
| Inicio de servicios | 1-2 min | Levanta los 10 contenedores |
| Espera de salud | 1-3 min | Verifica que todos estén `healthy` |
| Carga de datos | 30-60 s | Genera 5 000 alumnos con Python Faker |
| Pipeline ETL | 2-5 min | Ejecuta los 4 DAGs en secuencia |
| Configuración Dremio | 10 s | Crea fuente Nessie y 3 vistas SQL |
| Configuración Metabase | 30 s | Conecta BD y crea 2 dashboards |

---

## 2. Arranque Diario

### Después de reiniciar el equipo

```bash
cd universidad-datalakehouse
make up       # levanta todos los contenedores (datos preservados)
make status   # verificar que todos están corriendo
```

Los servicios tardan entre 30 segundos (MinIO) y 2 minutos (Dremio, Metabase) en estar completamente listos.

### Verificar que el ETL nocturno corrió

```bash
make etl-status    # muestra el estado de la última ejecución de cada DAG
```

Si algún DAG falló:
```bash
make reset-etl     # limpia y re-dispara todos los DAGs
```

---

## 3. Verificación de Salud

### Verificación rápida

```bash
make health
```

Muestra en un solo comando:
- Estado de los 10 contenedores
- Tablas Iceberg existentes con confirmación de que son accesibles
- Conteos de registros en la capa semántica PostgreSQL
- Estado de las últimas ejecuciones de cada DAG

### Verificación por componente

```bash
# Servicios Docker
make status

# Tablas Iceberg (cuántas filas tiene cada tabla)
make iceberg-tables

# Capa semántica PostgreSQL
make semantic-check

# Estado de DAGs Airflow
make etl-status

# Nessie (catálogo Iceberg)
curl http://localhost:19120/api/v2/config | python3 -m json.tool

# Dremio (query de prueba)
# Ir a http://localhost:9047 → SQL Runner → ejecutar:
# SELECT COUNT(*) FROM lakehouse.silver.fees AT BRANCH "main"
```

### Logs de servicios específicos

```bash
make logs-service s=airflow-scheduler   # logs del scheduler ETL
make logs-service s=nessie              # logs del catálogo
make logs-service s=dremio              # logs del query engine
make logs-service s=metabase            # logs de dashboards
```

---

## 4. Gestión del Pipeline ETL

### Disparar el ETL manualmente

```bash
make trigger-etl    # dispara los 4 DAGs y muestra su estado inicial
```

Para esperar a que completen (útil en scripts):
```bash
make trigger-etl && make wait-etl
```

### Monitorear desde Airflow UI

1. Abrir http://localhost:8090
2. Usuario: `admin` / contraseña definida en `.env` (`AIRFLOW_ADMIN_PASSWORD`)
3. Ver el estado de cada DAG en el panel principal
4. Hacer clic en un DAG → ver el grafo de tareas → hacer clic en una tarea → ver logs

### Forzar re-ejecución completa

```bash
make reset-etl    # limpia historial de ejecuciones y re-dispara
```

### Cambiar la frecuencia del ETL

En `.env`, modificar:
```bash
ETL_SCHEDULE_INTERVAL="0 0,12 * * *"   # cada 12h (defecto)
ETL_SCHEDULE_INTERVAL="0 */6 * * *"    # cada 6h
ETL_SCHEDULE_INTERVAL="0 8 * * 1-5"    # lunes-viernes a las 8am
ETL_SCHEDULE_INTERVAL="@daily"          # una vez al día a medianoche
```

Luego reiniciar el scheduler:
```bash
make restart-service s=airflow-scheduler
```

### Ejecutar solo un DAG específico

```bash
# Desde la terminal:
docker exec airflow-scheduler airflow dags trigger 01_moodle_to_bronze

# Desde Airflow UI:
# DAGs → 01_moodle_to_bronze → botón ▶ (Trigger DAG)
```

### Ver logs de una tarea ETL fallida

```bash
# Encontrar el log más reciente de una tarea:
docker exec airflow-scheduler bash -c "
  find /opt/airflow/logs -name '*.log' -path '*transform_fees*' | sort -r | head -3
"

# Ver el contenido del log:
docker exec airflow-scheduler tail -50 \
  "/opt/airflow/logs/dag_id=03_bronze_to_silver/run_id=.../task_id=transform_fees/attempt=1.log"
```

---

## 5. Gestión del Lakehouse Iceberg

### Ver tablas disponibles

```bash
make iceberg-tables    # con conteo de filas
make nessie-tables     # lista de entradas en el catálogo
```

### Consultar tablas desde Python

```bash
docker exec airflow-scheduler python3 -c "
import sys; sys.path.insert(0, '/opt/airflow/dags')
from common.lakehouse import get_catalog

catalog = get_catalog()

# Listar namespaces y tablas
for ns in catalog.list_namespaces():
    for t in catalog.list_tables(ns):
        df = catalog.load_table(t).scan().to_arrow()
        print(f'{\".\".join(t)}: {len(df)} filas')
"
```

### Eliminar una tabla específica

```bash
docker exec airflow-scheduler python3 -c "
import sys; sys.path.insert(0, '/opt/airflow/dags')
from common.lakehouse import get_catalog
catalog = get_catalog()
catalog.drop_table('silver.students')  # cambiar por la tabla a eliminar
print('Tabla eliminada')
"
# Luego re-ejecutar el ETL para recrearla:
docker exec airflow-scheduler airflow dags trigger 03_bronze_to_silver
```

### Reiniciar el catálogo Nessie (borrar todos los metadatos)

```bash
make nessie-reset    # borra metadatos Nessie (no los Parquet en MinIO)
make reset-etl       # re-ejecuta el ETL para recrear todas las tablas
```

**Cuándo es necesario**: si cambias schemas de tablas Iceberg en el código.

### Explorar los archivos Parquet en MinIO

```bash
make minio-ls    # lista el bucket principal

# O navegar en la UI: http://localhost:9001
# Usuario: minioadmin (o el configurado en .env)
```

### Historial de versiones de las tablas (Nessie branches)

```bash
make nessie-branches

# Ver commits en la rama main:
curl http://localhost:19120/api/v2/trees/main/history | python3 -m json.tool | head -50
```

---

## 6. Gestión de Dremio

### Acceso

- URL: http://localhost:9047
- Usuario: `admin` / contraseña en `.env` (`DREMIO_ADMIN_PASSWORD`)

### Re-configurar Dremio (fuente y vistas)

```bash
make setup-dremio
```

Esto crea/actualiza:
- Fuente `lakehouse` → conectada a Nessie
- Space `analytics` → con 3 vistas SQL predefinidas:
  - `analytics.students` — alumnos con programa
  - `analytics.financial_summary` — KPI financiero desde silver
  - `analytics.grade_summary` — rendimiento académico desde silver

### Ejecutar consultas vía API (para scripts/automatización)

```bash
# Ejemplo: consulta y espera resultado
docker exec airflow-scheduler python3 -c "
import requests, time

DREMIO = 'http://dremio:9047'
r = requests.post(f'{DREMIO}/apiv2/login',
    json={'userName': 'admin', 'password': 'Admin1234!'})
token = r.json()['token']
headers = {'Authorization': f'_dremio{token}', 'Content-Type': 'application/json'}

# Enviar consulta
r = requests.post(f'{DREMIO}/api/v3/sql',
    json={'sql': 'SELECT COUNT(*) AS total FROM lakehouse.silver.students AT BRANCH \"main\"'},
    headers=headers)
job_id = r.json()['id']

# Esperar resultado
while True:
    r = requests.get(f'{DREMIO}/api/v3/job/{job_id}', headers=headers)
    state = r.json()['jobState']
    if state == 'COMPLETED':
        results = requests.get(f'{DREMIO}/api/v3/job/{job_id}/results', headers=headers)
        print(results.json())
        break
    elif state in ('FAILED', 'CANCELED'):
        print('Error:', r.json().get('errorMessage'))
        break
    time.sleep(2)
"
```

---

## 7. Gestión de Metabase

### Acceso

- URL: http://localhost:3000
- Usuario: `admin@universidad.edu` / contraseña en `.env` (`AIRFLOW_ADMIN_PASSWORD`)

### Re-configurar Metabase (dashboards)

```bash
make setup-metabase
```

Si los dashboards ya existen, el script actualizará las cards.

### Acceder directamente a la BD semántica

```bash
docker exec -it metabase-db psql -U metabase -d universidad_analytics

# Consultas útiles:
SELECT COUNT(*) FROM dim_alumno;
SELECT COUNT(*) FROM kpi_financiero_mensual;
SELECT programa_codigo, SUM(ingresos_facturados) FROM kpi_financiero_mensual GROUP BY 1;
SELECT * FROM etl_run_log ORDER BY run_date DESC LIMIT 5;
```

### Sincronizar el schema de la base de datos

Cuando el ETL agrega nuevas tablas o cambia columnas:
1. Ir a Metabase → Admin → Databases → universidad_analytics
2. Hacer clic en "Sync database schema now"
3. O desde la API: `make setup-metabase` (re-hace la sincronización automáticamente)

---

## 8. Copias de Seguridad

### Datos de MinIO (archivos Parquet)

```bash
# Crear backup del bucket del lakehouse
docker exec minio mc mirror local/lakehouse /tmp/lakehouse-backup/
# O usar mc desde el host si lo tienes instalado

# Listar tamaño del bucket:
docker exec minio mc du local/lakehouse/
```

### Capa semántica (PostgreSQL)

```bash
# Backup completo de universidad_analytics:
docker exec metabase-db pg_dump -U metabase universidad_analytics \
  > backups/universidad_analytics_$(date +%Y%m%d).sql

# Restaurar:
docker exec -i metabase-db psql -U metabase universidad_analytics \
  < backups/universidad_analytics_20260612.sql
```

### Datos de fuentes (Moodle y ERPNext)

```bash
# Backup de ERPNext:
docker exec erpnext-db mariadb-dump \
  -u root -p${MARIADB_ROOT_PASSWORD} ${ERPNEXT_DB_NAME} \
  > backups/erpnext_$(date +%Y%m%d).sql

# Backup de Moodle:
docker exec moodle-db mysqldump \
  -u root -p${MYSQL_ROOT_PASSWORD} ${MOODLE_DB_NAME} \
  > backups/moodle_$(date +%Y%m%d).sql
```

> **Nota**: No es necesario hacer backup de los metadatos de Nessie (RocksDB) porque son reconstruibles ejecutando `make nessie-reset && make reset-etl`. Los archivos Parquet en MinIO son la fuente de verdad.

---

## 9. Actualización de Componentes

### Actualizar Nessie

1. En `.env`, cambiar `NESSIE_VERSION=0.108.0` por la nueva versión
2. Verificar compatibilidad con PyIceberg en https://py.iceberg.apache.org/
3. Ejecutar:
```bash
make nessie-reset           # borrar metadatos con versión anterior
docker compose pull nessie  # descargar nueva imagen
docker compose up -d nessie # iniciar con nueva versión
make reset-etl              # recrear tablas Iceberg
```

### Actualizar Metabase

1. En `.env`, cambiar `METABASE_VERSION=latest` o una versión específica
2. Ejecutar:
```bash
docker compose pull metabase
docker compose up -d metabase
# Esperar a que aplique migraciones (~2-3 min)
make setup-metabase         # puede ser necesario re-crear dashboards
```

### Actualizar la imagen de Airflow

1. En `pipelines/Dockerfile`, cambiar la versión base
2. En `pipelines/requirements.txt`, actualizar versiones de paquetes
3. Ejecutar:
```bash
docker compose build airflow-scheduler airflow-webserver
docker compose up -d airflow-scheduler airflow-webserver
```

---

## 10. Agregar una Nueva Fuente de Datos

### Ejemplo: agregar una fuente PostgreSQL (Odoo, sistema propio, etc.)

**Paso 1**: Agregar variables de entorno en `.env` y `docker-compose.yml`:
```bash
# .env
NUEVA_FUENTE_DB_HOST=nueva-fuente-db
NUEVA_FUENTE_DB_PORT=5432
NUEVA_FUENTE_DB_NAME=mi_base
NUEVA_FUENTE_DB_USER=usuario
NUEVA_FUENTE_DB_PASSWORD=password
```

**Paso 2**: Definir el schema Iceberg en `pipelines/dags/common/lakehouse.py`:
```python
SCHEMA_NUEVA_FUENTE_ENTIDAD = Schema(
    NestedField(1, "id",          LongType()),
    NestedField(2, "nombre",      StringType()),
    NestedField(3, "fecha",       StringType()),
    NestedField(4, "_etl_loaded_at", StringType()),
)
```
> ⚠️ **Importante**: No usar `required=True` en ningún `NestedField` — PyArrow siempre genera tipos opcionales.

**Paso 3**: Crear un nuevo DAG en `pipelines/dags/05_nueva_fuente_bronze_dag.py`:
```python
import os, logging, psycopg2
from datetime import datetime, timedelta
import pyarrow as pa
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.lakehouse import get_catalog, upsert_iceberg, SCHEMA_NUEVA_FUENTE_ENTIDAD

CONN = dict(
    host=os.environ.get("NUEVA_FUENTE_DB_HOST"),
    port=int(os.environ.get("NUEVA_FUENTE_DB_PORT", 5432)),
    dbname=os.environ.get("NUEVA_FUENTE_DB_NAME"),
    user=os.environ.get("NUEVA_FUENTE_DB_USER"),
    password=os.environ.get("NUEVA_FUENTE_DB_PASSWORD"),
)

def extract_entidad(**ctx):
    conn = psycopg2.connect(**CONN)
    cur = conn.cursor()
    cur.execute("SELECT id, nombre, fecha::text FROM entidad WHERE updated_at >= NOW() - INTERVAL '13 hours'")
    rows = cur.fetchall()
    cur.close(); conn.close()

    if not rows:
        return 0

    now = datetime.utcnow().isoformat()
    table = pa.table({
        "id":             pa.array([r[0] for r in rows], pa.int64()),
        "nombre":         pa.array([r[1] or "" for r in rows], pa.string()),
        "fecha":          pa.array([r[2] or "" for r in rows], pa.string()),
        "_etl_loaded_at": pa.array([now] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.nueva_fuente_entidad", table,
                          SCHEMA_NUEVA_FUENTE_ENTIDAD)

with DAG(
    dag_id="05_nueva_fuente_to_bronze",
    schedule_interval="0 0,12 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "lakehouse", "retries": 3},
    tags=["bronze", "nueva_fuente"],
) as dag:
    PythonOperator(task_id="extract_entidad", python_callable=extract_entidad)
```

**Paso 4**: El DAG aparecerá automáticamente en Airflow (el volumen `./pipelines/dags` está montado).

**Paso 5** (opcional): Agregar transformación en `03_silver_transform_dag.py` y materialización en `04_gold_materialize_dag.py` siguiendo el mismo patrón.

---

## Referencia Rápida de Comandos

```bash
# ── Estado ──────────────────────────────
make health              # resumen completo
make status              # contenedores Docker
make etl-status          # estado de los 4 DAGs
make iceberg-tables      # tablas Iceberg con filas
make semantic-check      # KPIs en PostgreSQL

# ── Iniciar/Detener ─────────────────────
make up                  # iniciar todos
make down                # detener (conserva datos)
make restart             # detener + iniciar

# ── ETL ─────────────────────────────────
make trigger-etl         # disparar los 4 DAGs
make reset-etl           # limpiar historial + disparar
make wait-etl            # esperar a que completen

# ── Configuración ────────────────────────
make setup-dremio        # configurar Dremio
make setup-metabase      # configurar Metabase
make seed                # recargar datos de prueba

# ── Recuperación ────────────────────────
make nessie-reset        # reiniciar Nessie (luego: reset-etl)
make clean               # borrar TODO (pide confirmación)
make reset               # borrar TODO + bootstrap completo

# ── Logs y diagnóstico ───────────────────
make logs                # logs de todos los servicios
make logs-service s=X    # logs de un servicio (X=nessie, dremio…)
make nessie-branches     # ramas del catálogo
make minio-ls            # objetos en MinIO
```

---

---

## 11. Proceso de Desarrollo — Validaciones Previas

Esta sección describe el proceso correcto para extender el stack con nuevas tablas, fuentes o DAGs, basado en las lecciones aprendidas durante la integración inicial. Ver también `docs/STACK.md` y `docs/RETRO_DEVOPS.md`.

### El problema que resuelve este proceso

Sin validaciones previas, los errores de integración se descubren en runtime (mientras un DAG falla a mitad de ejecución), lo que requiere resetear estado de Nessie, borrar tablas mal creadas y re-ejecutar pipelines completos. Las validaciones previas detectan estos problemas antes de escribir una sola línea de código de DAG.

### Paso 1 — Completar la Matriz de Compatibilidad (`docs/STACK.md`)

Antes de cualquier desarrollo, verificar que las versiones del stack y sus restricciones están documentadas. Abrir `docs/STACK.md` y confirmar que:

- Las versiones de imagen Docker están fijadas (sin `latest`)
- Las restricciones conocidas del stack están documentadas
- El bloque de "contexto para IA" está actualizado

Si se agrega un nuevo componente, revisar su changelog buscando "breaking changes" antes de seleccionar la versión.

### Paso 2 — Pre-flight Check

```bash
# Levantar servicios (si no están corriendo):
make up

# Ejecutar validación de conectividad e integración:
make preflight
```

`make preflight` ejecuta `pipelines/scripts/preflight_check.py` dentro del scheduler de Airflow. Valida:

| Check | Qué confirma |
|---|---|
| MinIO alcanzable | El objeto storage responde |
| MinIO bucket existe | El bucket `lakehouse` fue creado |
| Nessie API v2 | El catálogo Nessie responde en `/api/v2` |
| Nessie REST Iceberg | Expone `/iceberg/v1/config` — requerido por PyIceberg 0.7.x |
| PyIceberg round-trip | Crea tabla, escribe fila, lee fila, borra tabla — ciclo completo |
| Dremio API v3 | Login y acceso a `/api/v3/catalog` |
| Metabase login | Credenciales válidas |
| PostgreSQL accesible | BD `universidad_analytics` responde |
| analytics: permisos tablas | El usuario puede hacer SELECT/INSERT |
| analytics: permisos secuencias | El usuario puede usar SERIAL/IDENTITY — sin esto falla el Gold DAG |

**Regla**: si cualquier check falla, resolver antes de continuar. No escribir código de DAG con checks rojos.

### Paso 3 — Validar Schemas Iceberg

```bash
make validate-schemas
```

Ejecuta `pipelines/scripts/validate_schemas.py`. Para cada schema definido en `common/lakehouse.py` y en los DAGs Silver verifica:

- Ningún `NestedField` tiene `required=True` (causa el error `Mismatch in fields` en runtime)
- El schema se convierte a PyArrow sin error
- Un round-trip con datos reales (incluyendo `None`) funciona

Ejecutar este check cada vez que se modifiquen schemas.

### Paso 4 — Pipeline Vertical Mínimo

Antes de implementar el pipeline completo para una nueva fuente, implementar el camino completo para **una sola tabla**:

```
Bronze (1 tabla) → Silver (1 tabla) → Gold (1 registro en PostgreSQL)
       ↓                  ↓
  make iceberg-tables   make semantic-check
```

Solo cuando ese camino funciona end-to-end, escalar a las tablas restantes.

### Paso 5 — Acceptance Test

```bash
make acceptance-test
```

Ejecuta `pipelines/scripts/acceptance_test.py`. Valida el stack completo:

| Sección | Checks |
|---|---|
| Iceberg / Nessie | ≥ 8 tablas bronze, ≥ 4 tablas silver, datos presentes |
| PostgreSQL | dim_alumno, kpi_financiero, kpi_academico, fact_ingresos pobladas |
| Dremio | Fuente `lakehouse` configurada, espacio `analytics` con vistas |
| Metabase | BD conectada, ≥ 2 dashboards creados |

`make bootstrap` ejecuta `acceptance-test` automáticamente como último paso.

### Diseño de DAGs — Reglas de Oro

**Regla 1: Schemas Iceberg sin `required=True`**
```python
# ❌ Incorrecto — causa Mismatch en runtime:
NestedField(1, "student_code", StringType(), required=True)

# ✅ Correcto:
NestedField(1, "student_code", StringType())
```

**Regla 2: ExternalTaskSensor siempre con `execution_date_fn`**
```python
# ❌ Incorrecto — queda bloqueado con triggers manuales:
ExternalTaskSensor(external_dag_id="upstream_dag")

# ✅ Correcto — busca el último run exitoso:
def _latest(dt, **kwargs):
    from airflow.models import DagRun, settings
    session = settings.Session()
    run = session.query(DagRun).filter(
        DagRun.dag_id == "upstream_dag", DagRun.state == "success"
    ).order_by(DagRun.execution_date.desc()).first()
    return run.execution_date if run else dt

ExternalTaskSensor(external_dag_id="upstream_dag", execution_date_fn=_latest)
```

**Regla 3: Convertir PyArrow a Python antes de psycopg2**
```python
# ❌ Incorrecto — psycopg2 no puede serializar PyArrow scalars:
value = df["column"][i]

# ✅ Correcto — convertir a lista Python primero:
values = df["column"].to_pylist()
value = values[i]
```

**Regla 4: Estimar memoria antes de configurar paralelismo**
```
RAM por tarea ≈ filas × columnas × 8 bytes × 3 (overhead PyArrow)
mem_limit scheduler = max_tareas_paralelas × RAM_por_tarea × 1.3
```

---

## Puertos y URLs de Referencia

| Servicio | Puerto | URL | Protocolo |
|----------|--------|-----|-----------|
| Airflow UI | 8090 | http://localhost:8090 | HTTP |
| Metabase | 3000 | http://localhost:3000 | HTTP |
| Dremio UI | 9047 | http://localhost:9047 | HTTP |
| Dremio JDBC | 31010 | jdbc:dremio:direct=localhost:31010 | JDBC/Arrow |
| MinIO Console | 9001 | http://localhost:9001 | HTTP |
| MinIO API | 9000 | http://localhost:9000 | S3-API |
| Nessie API | 19120 | http://localhost:19120/api/v2 | HTTP/REST |
| PostgreSQL | interno | metabase-db:5432 | TCP |
