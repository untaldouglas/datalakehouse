# Guía de Resolución de Problemas

Esta guía documenta todos los problemas encontrados durante la integración y sus soluciones confirmadas.

---

## Diagnóstico General

Antes de buscar un problema específico, ejecuta:

```bash
make health        # resumen de servicios + tablas + KPIs
make etl-status    # estado de los 4 DAGs
```

---

## Problemas de Inicio

### Nessie no arranca: "No default-warehouse configured"

**Síntoma**: El contenedor `nessie` entra en bucle de reinicio. Los logs muestran:
```
No default-warehouse configured
```

**Causa**: Nessie 0.108.0 requiere configuración explícita del warehouse.

**Solución**: Verificar que en `docker-compose.yml` el servicio `nessie` tenga las variables de entorno:
```yaml
NESSIE_CATALOG_DEFAULT_WAREHOUSE: ${MINIO_BUCKET_LAKEHOUSE}
NESSIE_CATALOG_WAREHOUSES__LAKEHOUSE__LOCATION: s3://${MINIO_BUCKET_LAKEHOUSE}/
```
Y que el archivo `services/nessie/application.properties` esté montado:
```yaml
volumes:
  - ./services/nessie/application.properties:/deployments/config/application.properties:ro
```

---

### Nessie falla con "Missing access key and secret for STATIC authentication mode"

**Síntoma**: Nessie arranca pero los DAGs de Airflow fallan al crear tablas Iceberg.

**Causa**: Nessie necesita sus propias credenciales S3 para validar ubicaciones de tablas del lado del servidor. Las variables de entorno estándar AWS no funcionan con Nessie 0.108.0 debido a cómo Quarkus resuelve propiedades con guiones en nombres de mapas.

**Solución**: Las credenciales deben estar en `services/nessie/application.properties`:
```properties
nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:minio.s3creds
minio.s3creds.name=${MINIO_ROOT_USER}
minio.s3creds.secret=${MINIO_ROOT_PASSWORD}
```
El prefijo `urn:nessie-secret:quarkus:` le indica a Nessie que resuelva el secreto desde la configuración de Quarkus.

---

### Airflow init falla: "AIRFLOW_FERNET_KEY not set" o "your_fernet_key_here_replace_me"

**Síntoma**: El servicio `airflow-init` falla o el scheduler no arranca.

**Causa**: Las claves Fernet y Secret no se generaron en `.env`.

**Solución**:
```bash
make gen-keys    # genera e inserta las claves automáticamente
make restart-service s=airflow-init
make restart-service s=airflow-scheduler
```

---

## Problemas en el Pipeline ETL

### Tarea falla con "Mismatch in fields: name: required string vs name: optional string"

**Síntoma**: Cualquier tarea de ETL falla con un mensaje similar a:
```
pyiceberg.exceptions.ValidationError: Mismatch in fields:
  ❌ 1: name: required string | 1: name: optional string
```

**Causa**: Una tabla Iceberg fue creada con un campo `required=True` en el schema, pero PyArrow siempre genera tipos opcionales (nullable). Ocurre cuando se reinicializó Nessie después de que la tabla ya existía con el schema incorrecto.

**Diagnóstico**: 
```bash
# Ver qué tablas existen actualmente en Nessie
make nessie-tables
```

**Solución**:
```bash
# Opción A: Borrar solo la tabla afectada
docker exec airflow-scheduler python3 -c "
import sys; sys.path.insert(0, '/opt/airflow/dags')
from common.lakehouse import get_catalog
catalog = get_catalog()
catalog.drop_table('bronze.erpnext_programs')  # ajustar el nombre
print('Tabla eliminada')
"
# Luego re-disparar el DAG:
make reset-etl

# Opción B: Si son muchas tablas o el problema es generalizado:
make nessie-reset    # borra todos los metadatos Iceberg
make reset-etl       # re-ejecuta todo el ETL
```

---

### Tarea falla con exit code -9 (OOM — Out of Memory)

**Síntoma**: Una tarea del scheduler finaliza con `return code -9`. El log muestra que el proceso fue terminado abruptamente.

**Causa**: El proceso Airflow que ejecuta la tarea consumió más RAM de la asignada al contenedor. Ocurre especialmente cuando varias tareas pesadas corren en paralelo (ej: `transform_fees` con 200k filas + otras transformaciones simultáneas).

**Solución inmediata** (sin reiniciar nada):
```bash
# Reintentar la tarea sola (cuando las demás tareas paralelas ya terminaron):
docker exec airflow-scheduler python3 -c "
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow import settings
session = settings.Session()
ti = session.query(TaskInstance).filter(
    TaskInstance.dag_id == '03_bronze_to_silver',
    TaskInstance.task_id == 'transform_fees',
).order_by(TaskInstance.start_date.desc()).first()
if ti:
    ti.state = State.QUEUED
    session.commit()
    print(f'Tarea {ti.task_id} reseteada a queued, run_id={ti.run_id}')
"
# El scheduler la recogerá en ~30 segundos
```

**Solución permanente**: Aumentar la memoria del scheduler en `docker-compose.yml`:
```yaml
airflow-scheduler:
  mem_limit: 2g   # cambiar de 1g a 2g
```
O reducir el paralelismo de los DAGs. En `03_silver_transform_dag.py`, cambiar:
```python
# Antes (paralelo — puede causar OOM):
t_students >> [t_fees, t_payments, t_grades]

# Después (serial — más lento pero seguro con poca RAM):
t_students >> t_fees >> t_payments >> t_grades
```

---

### ExternalTaskSensor en timeout: el DAG silver/gold espera indefinidamente

**Síntoma**: Los DAGs `03_bronze_to_silver` o `04_silver_to_gold` tienen las tareas `wait_*_bronze` o `wait_silver_transform` en estado `running` por más de 10 minutos.

**Causa**: El sensor busca una ejecución del DAG upstream con el mismo `execution_date`. Al disparar DAGs manualmente en momentos distintos, los `execution_date` no coinciden.

**Diagnóstico**:
```bash
# Ver execution_dates de los bronze DAGs
docker exec airflow-scheduler airflow dags list-runs -d 01_moodle_to_bronze 2>/dev/null | tail -3
docker exec airflow-scheduler airflow dags list-runs -d 02_erpnext_to_bronze 2>/dev/null | tail -3
```

**Solución**: Los DAGs ya incluyen `execution_date_fn` que busca la última ejecución exitosa. Si el sensor aún se bloquea, marcar manualmente como exitosas las tareas sensor:

```bash
docker exec airflow-scheduler python3 -c "
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow import settings
from datetime import datetime, timezone

session = settings.Session()

# Buscar el run_id del DAG que está atascado
run = session.query(DagRun).filter(
    DagRun.dag_id == '03_bronze_to_silver',
    DagRun.state == 'running'
).order_by(DagRun.execution_date.desc()).first()

if run:
    for task_id in ['wait_moodle_bronze', 'wait_erpnext_bronze']:
        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.run_id == run.run_id,
            TaskInstance.task_id == task_id
        ).first()
        if ti:
            ti.state = State.SUCCESS
            ti.end_date = datetime.now(timezone.utc)
            print(f'Marcado como success: {task_id}')
    session.commit()
"
```

---

### DAG falla: "INSERT has more target columns than expressions"

**Síntoma**: La tarea `materialize_student_dims` del DAG `04_silver_to_gold` falla con:
```
psycopg2.errors.SyntaxError: INSERT has more target columns than expressions
LINE 4: ...fecha_ingreso, programa_codigo, anio_academico, estado, updated_at...
```

**Causa**: Bug en el SQL del INSERT — la columna `updated_at` está en la lista de columnas pero no en los VALUES.

**El archivo `04_gold_materialize_dag.py` ya está corregido.** Si el error reaparece, verificar que la línea del INSERT dice:
```python
INSERT INTO dim_alumno
  (alumno_codigo, nombre_completo, genero, fecha_nacimiento,
   fecha_ingreso, programa_codigo, anio_academico, estado)   # SIN updated_at
```

---

### DAG falla: "must be owner of sequence kpi_financiero_mensual_kpi_id_seq"

**Síntoma**: Las tareas `materialize_financial_kpis` o `materialize_academic_kpis` fallan con error de privilegios de secuencia.

**Causa**: `TRUNCATE TABLE x RESTART IDENTITY` requiere ser propietario de la secuencia. El usuario `analytics` no es dueño de las secuencias creadas por `postgres`.

**Solución inmediata**:
```bash
docker exec metabase-db psql -U metabase -d universidad_analytics -c "
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO analytics;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO analytics;
"
```

**El archivo `04_gold_materialize_dag.py` ya está corregido** (`RESTART IDENTITY` fue removido de todos los `TRUNCATE`).

---

### DAG falla: "can't adapt type 'pyarrow.lib.LargeStringScalar'"

**Síntoma**: La tarea `materialize_financial_kpis` falla durante la carga de `fact_ingresos_matricula`.

**Causa**: Se accedió a un elemento de un array PyArrow directamente (`df["column"][i]`) en lugar de convertirlo primero con `.to_pylist()`. psycopg2 no sabe serializar tipos nativos de PyArrow.

**Regla**: Siempre convertir columnas PyArrow a listas Python antes de iterar:
```python
# Incorrecto:
value = df["column"][i]          # PyArrow scalar → psycopg2 falla

# Correcto:
values = df["column"].to_pylist()
value = values[i]                # Python str/int/float → psycopg2 OK
```

**El archivo `04_gold_materialize_dag.py` ya está corregido.**

---

## Problemas de Dremio

### Dremio no encuentra la fuente Nessie

**Síntoma**: En Dremio no aparece la fuente `lakehouse` en Sources.

**Solución**:
```bash
make setup-dremio
```

Si el comando falla, ejecutarlo con logs:
```bash
docker exec -e DREMIO_HOST=http://dremio:9047 airflow-scheduler \
  python3 /opt/airflow/scripts/setup_dremio.py
```

---

### Error en Dremio: "Version context for table must be specified using AT SQL syntax"

**Síntoma**: Al ejecutar SQL en Dremio contra tablas del lakehouse:
```
Validation of view sql failed. Version context for table lakehouse.bronze.erpnext_students
must be specified using AT SQL syntax.
```

**Causa**: Dremio requiere que las consultas sobre fuentes Nessie incluyan la versión del branch.

**Solución**: Siempre incluir `AT BRANCH "main"` después del nombre de la tabla:
```sql
-- Correcto:
SELECT * FROM lakehouse.silver.fees AT BRANCH "main" LIMIT 10;

-- También correcto con alias:
FROM lakehouse.bronze.erpnext_students AT BRANCH "main" s
```

Las vistas en `analytics.*` ya incluyen esta sintaxis y no requieren el AT BRANCH.

---

### Dremio rechaza el endpoint de Nessie: "Invalid API version"

**Síntoma**: Al crear la fuente Nessie en Dremio falla con:
```
Invalid API version. Make sure that Nessie endpoint URL has a valid API version.
```

**Causa**: Dremio 25.0 requiere el endpoint de la API v2 de Nessie. La variable `NESSIE_URI` apunta a `/api/v1`.

**El archivo `setup_dremio.py` ya está corregido** — convierte automáticamente `/api/v1` a `/api/v2`.
Si se configura manualmente en la UI de Dremio, usar: `http://nessie:19120/api/v2`

---

## Problemas de Metabase

### Dashboards vacíos o cards sin datos

**Síntoma**: Los dashboards de Metabase muestran "No results" o errores.

**Diagnóstico**:
```bash
make etl-status      # verificar que los 4 DAGs completaron con éxito
make semantic-check  # verificar que hay datos en PostgreSQL
```

Si los DAGs fallaron, ejecutar:
```bash
make reset-etl
make wait-etl        # esperar a que completen
```

---

### Error al agregar cards al dashboard: "API endpoint does not exist"

**Síntoma**: Durante `make setup-metabase`, aparecen warnings:
```
POST /api/dashboard/2/cards → 404: "API endpoint does not exist."
```

**Causa**: Metabase 0.62 cambió el endpoint — ya no es `POST /api/dashboard/{id}/cards`.

**El archivo `configure_metabase.py` ya está corregido** — usa `PUT /api/dashboard/{id}/cards`.

Si los dashboards están vacíos, re-ejecutar:
```bash
make setup-metabase
```

---

### Metabase no puede conectarse a la base de datos `universidad_analytics`

**Síntoma**: En Metabase aparece error de conexión a la base de datos.

**Diagnóstico**:
```bash
docker exec metabase-db psql -U metabase -d universidad_analytics -c "SELECT COUNT(*) FROM dim_alumno;"
```

Si falla, el problema es que la base semántica no fue inicializada:
```bash
# Verificar que el init SQL corrió
docker exec metabase-db psql -U metabase -c "\l" | grep universidad_analytics
```

Si no existe, reiniciar con el init:
```bash
docker compose rm -f metabase-db
docker volume rm datalakehouse_metabase_db_data
docker compose up -d metabase-db
# esperar ~30s y luego:
make reset-etl
make setup-metabase
```

---

## Problemas de Memoria

### Docker Desktop se queda sin memoria

**Síntoma**: Los contenedores se reinician solos, Docker se vuelve lento, o aparecen errores de `OOMKilled`.

**Diagnóstico**:
```bash
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"
```

**Soluciones** (aplicar en orden):

1. **Aumentar RAM de Docker Desktop**: Settings → Resources → Memory → 14-16 GB

2. **Reducir límites en `docker-compose.yml`**:
```yaml
dremio:
  mem_limit: 3g              # default: 5g
  environment:
    DREMIO_MAX_MEMORY_SIZE_MB: "2048"  # default: 4096

metabase:
  mem_limit: 768m            # default: 1g
  environment:
    JAVA_OPTS: "-Xmx640m -Xms256m"  # default: -Xmx896m
```

3. **No correr más de 2 tareas Airflow en paralelo**: En Airflow UI → Admin → Pools, reducir `default_pool` a 2 slots.

---

## Operaciones de Recuperación

### Reinicio completo del lakehouse (sin borrar datos de origen)

```bash
# Solo borra metadatos Iceberg (Nessie), NO los Parquet en MinIO
make nessie-reset
# Luego re-ejecutar el ETL para recrear las tablas Iceberg:
make reset-etl
make wait-etl
```

### Reinicio total desde cero

```bash
# ADVERTENCIA: borra absolutamente todo — base de datos, lakehouse, configuración
make clean           # pide confirmación
make bootstrap       # levanta y configura todo desde cero
```

### Re-configurar solo Dremio o Metabase (sin borrar datos)

```bash
make setup-dremio    # re-crea fuente Nessie y vistas en Dremio
make setup-metabase  # re-crea dashboards en Metabase
```
