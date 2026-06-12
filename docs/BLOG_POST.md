# Construyendo un Data Lakehouse universitario con tecnología open source: Iceberg, Nessie, Dremio y Airflow

*Por el equipo de ingeniería de datos — Universidad Musa*

---

Hay una tensión muy conocida en las instituciones de educación superior: tienen datos valiosos dispersos en múltiples sistemas — el LMS, el ERP, las plataformas académicas — pero ninguna forma práctica de cruzarlos, analizarlos juntos o exponerlos a quienes toman decisiones. El resultado es el de siempre: reportes manuales en Excel, dashboards desconectados entre sí, y equipos que pierden horas conciliando cifras que deberían existir en un solo lugar.

Este post documenta cómo construimos un prototipo funcional de Data Lakehouse para una universidad — completamente con herramientas open source — que integra datos de Moodle y ERPNext en una arquitectura medallón (Bronze/Silver/Gold) con consultas ad-hoc vía Dremio y dashboards operativos en Metabase. Todo orquestado por Airflow, persistido en Apache Iceberg sobre MinIO, y versionado con Nessie.

No es un post de "mira qué lindo quedó el stack". Es un recuento honesto de qué decisiones tomamos, por qué, y qué nos costó llegar ahí.

---

## El problema de dominio

Una universidad maneja dos flujos de datos estructuralmente distintos:

- **Datos académicos** (Moodle): inscripciones, calificaciones, actividad en cursos, usuarios. Origen: MySQL. Actualizaciones frecuentes, estructura razonablemente estable.
- **Datos financieros** (ERPNext): matrícula, pagos, cobranza, programas académicos. Origen: MariaDB. Tablas normalizadas al estilo ERP, con campos que pueden ser nulos de maneras inesperadas.

Ninguno de los dos sistemas habla con el otro. La pregunta que nadie puede responder rápido es: *¿cuántos alumnos de Medicina están en riesgo académico Y tienen cuotas vencidas?* Esa query requiere cruzar cuatro tablas de dos bases de datos distintas, con esquemas incompatibles.

La solución clásica sería un data warehouse. Pero los data warehouses tradicionales tienen un problema de rigidez: cada vez que cambia el esquema fuente, el ETL se rompe. Y los esquemas de ERPNext y Moodle cambian con cada actualización de versión.

---

## Por qué un Lakehouse y no un Warehouse

El patrón Lakehouse combina la flexibilidad del almacenamiento en objeto (S3/MinIO + Parquet) con las garantías de un formato de tabla transaccional (Apache Iceberg: ACID, time travel, evolución de esquema). El resultado: podés cambiar el esquema de una tabla Iceberg sin romper las queries que ya existen sobre ella.

La arquitectura medallón que elegimos tiene tres capas:

```
FUENTES                       LAKEHOUSE (Iceberg sobre MinIO)           SEMÁNTICA
────────                      ──────────────────────────────           ──────────
Moodle (MySQL)  ──── DAG 1 ──▶  Bronze  ──── DAG 3 ──▶  Silver  ──┐
ERPNext (MariaDB) ── DAG 2 ──▶  (raw)               (cleaned)    │
                                                                   ├─ DAG 4 ──▶ PostgreSQL
                                                                   │            (dims/KPIs)
                                                                   │
                                                    ──────────────▶ Dremio OSS (SQL ad-hoc)
                                                                     Metabase (dashboards)
```

**Bronze**: datos crudos, sin transformar, con el campo `_etl_loaded_at` para auditoría. Nunca se modifican; solo se hace append.

**Silver**: datos limpios, tipados, con campos derivados. Por ejemplo, la capa Silver de fees agrega `is_overdue` (boolean), `days_overdue` (int) y `pending_amount` (calculado). Esta transformación ocurre en Python puro con PyArrow — sin SQL intermedio.

**Gold**: KPIs precalculados materializados en PostgreSQL para que Metabase pueda servirlos en milisegundos. `kpi_financiero_mensual`, `kpi_academico_periodo`, `fact_ingresos_matricula`, `dim_alumno`.

---

## El stack y por qué cada pieza

### Apache Iceberg + PyIceberg 0.7.1

Iceberg es el formato de tabla. No es una base de datos; es una especificación sobre cómo organizar archivos Parquet en un objeto store con capacidades que antes solo tenías en un data warehouse: transacciones, time travel, evolución de esquema, predicado pushdown.

PyIceberg es la librería Python para leer y escribir tablas Iceberg. Una advertencia importante para quienes vienen de versiones anteriores: **PyIceberg 0.7.x eliminó el tipo de catálogo `nessie` nativo**. La conexión ahora es vía REST catalog — lo que en realidad es la forma correcta porque desacopla el cliente del catálogo.

### Nessie 0.108.0 — Git para tus tablas

Nessie es el catálogo de tablas con semántica de control de versiones. Pensalo como Git, pero para tablas Iceberg: branches, commits, merges, tags. Desde Nessie 0.76+, expone un endpoint REST Iceberg compatible que PyIceberg puede consumir directamente en `/iceberg/v1/config`.

```python
def get_catalog(branch: str = "main"):
    nessie_base = NESSIE_URI.replace("/api/v1", "")
    return load_catalog("nessie", **{
        "type":                "rest",
        "uri":                 f"{nessie_base}/iceberg/",    # trailing slash obligatorio
        "warehouse":           f"s3://{LAKEHOUSE_BUCKET}",
        "s3.endpoint":         MINIO_ENDPOINT,
        "s3.access-key-id":    MINIO_ACCESS_KEY,
        "s3.secret-access-key":MINIO_SECRET_KEY,
        "s3.path-style-access":"true",
    })
```

El versionado de datos es especialmente útil en un contexto universitario: podés hacer una transformación experimental en un branch separado sin tocar los datos que ya están consumiendo los dashboards en `main`.

### MinIO — S3 que corre en tu laptop

MinIO es el object store. Implementa la API S3 de forma compatible al 100%, lo que significa que el mismo código que escribe a MinIO en desarrollo escribe a S3 real en producción. Para Iceberg, esto importa porque los archivos Parquet viven ahí, y el catálogo (Nessie) solo guarda los metadatos.

### Airflow 2.9.1 — Orquestación con LocalExecutor

Cuatro DAGs, cada uno en su namespace:

| DAG | Rol | Fuente → Destino |
|-----|-----|-----------------|
| `01_moodle_to_bronze` | Ingesta | MySQL → Iceberg bronze |
| `02_erpnext_to_bronze` | Ingesta | MariaDB → Iceberg bronze |
| `03_bronze_to_silver` | Transformación | Bronze → Silver Iceberg |
| `04_silver_to_gold` | Materialización | Silver → PostgreSQL |

Los DAGs 03 y 04 usan `ExternalTaskSensor` para esperar a que sus upstream completen. Hay un gotcha importante aquí que documento más abajo.

La función `upsert_iceberg` es el corazón del pipeline — crea la tabla si no existe y escribe datos:

```python
def upsert_iceberg(catalog, table_identifier, df, schema,
                   partition_spec=None, mode="append"):
    namespace, table_name = table_identifier.rsplit(".", 1)
    ensure_namespace(catalog, namespace)

    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec or PartitionSpec(),
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy",
            },
        )

    table = catalog.load_table(table_identifier)
    table.overwrite(df) if mode == "overwrite" else table.append(df)
```

### Dremio OSS 25.0 — SQL sobre todo

Dremio actúa como query engine federado. Conecta directo al catálogo Nessie y permite hacer SQL sobre cualquier tabla Iceberg con pushdown de predicados — sin mover datos. Desde Dremio, podés hacer un JOIN entre una tabla Bronze de Moodle y una Silver de ERPNext en una sola query.

Un detalle que no está en ningún tutorial: cuando consultás tablas Nessie desde Dremio, **tenés que especificar el branch explícitamente** con sintaxis `AT`:

```sql
SELECT s.student_name, p.program_name, f.pending_amount
FROM lakehouse.silver.students AT BRANCH "main" s
JOIN lakehouse.silver.fees AT BRANCH "main" f ON s.student_code = f.student_code
JOIN lakehouse.bronze.erpnext_programs AT BRANCH "main" p ON s.program_code = p.name
WHERE f.is_overdue = 1 AND s.active = 1
ORDER BY f.days_overdue DESC;
```

Las vistas virtuales en el espacio `analytics` ya incluyen esta sintaxis, así que los usuarios finales no necesitan conocerla.

### Metabase 0.62 — Dashboards sin SQL

Metabase se conecta a la capa PostgreSQL (Gold) — no a Dremio — para los dashboards operativos. La razón es práctica: las queries de dashboard son predecibles y repetitivas; materializar los KPIs en PostgreSQL cada 12 horas da tiempos de respuesta de milisegundos vs. los segundos que tarda Dremio en arrancar una query Iceberg en frío.

---

## Las tres lecciones más duras

### 1. PyArrow siempre genera tipos nullable — siempre

Esta es la trampa más frecuente al diseñar schemas Iceberg con PyIceberg. Si definís un campo con `required=True`:

```python
# MAL — esto va a explotar en runtime:
NestedField(1, "student_code", StringType(), required=True)
```

Y luego intentás escribir un `pa.Table` generado desde una query SQL, vas a ver esto:

```
pyiceberg.exceptions.ValidationError: Mismatch in fields:
  ❌ 1: student_code: required string | 1: student_code: optional string
```

PyArrow siempre infiere columnas como nullable, independientemente de si la columna fuente es `NOT NULL` en MySQL. La solución es simple: **nunca usar `required=True` en `NestedField` cuando la fuente es una base de datos relacional**. Los `NestedField` sin `required` son opcionales por defecto.

```python
# Bien — siempre omitir required=True para datos de orígenes relacionales:
NestedField(1, "student_code", StringType()),
```

### 2. ExternalTaskSensor necesita `execution_date_fn` para triggers manuales

`ExternalTaskSensor` de Airflow compara `execution_date` de forma exacta entre el DAG upstream y el downstream. En producción, donde los DAGs corren en schedule fijo, los timestamps coinciden perfectamente. En desarrollo, donde los DAGs se disparan manualmente, los timestamps son siempre distintos — y el sensor queda esperando para siempre.

La solución es `execution_date_fn`, una función que le dice al sensor cómo encontrar el run correcto:

```python
def _latest_success(upstream_dag_id):
    def fn(dt, **kwargs):
        from airflow.models import DagRun
        from airflow import settings
        session = settings.Session()
        run = session.query(DagRun).filter(
            DagRun.dag_id == upstream_dag_id,
            DagRun.state == "success",
        ).order_by(DagRun.execution_date.desc()).first()
        return run.execution_date if run else dt
    return fn

wait_bronze = ExternalTaskSensor(
    task_id="wait_moodle_bronze",
    external_dag_id="01_moodle_to_bronze",
    execution_date_fn=_latest_success("01_moodle_to_bronze"),
    mode="reschedule",   # no bloquea un worker slot mientras espera
    poke_interval=30,
    timeout=3600,
)
```

### 3. Los tipos PyArrow no son serializables por psycopg2

Cuando leés una tabla Iceberg con PyIceberg, obtenés un `pa.Table`. Si accedés directamente a elementos individuales (`df["column"][i]`), obtenés un `pyarrow.lib.LargeStringScalar` — un tipo nativo de PyArrow que psycopg2 no sabe insertar en PostgreSQL:

```
TypeError: can't adapt type 'pyarrow.lib.LargeStringScalar'
```

La corrección es siempre convertir columnas completas a listas Python antes de iterar:

```python
# MAL:
value = df["fee_id"][i]          # → LargeStringScalar → psycopg2 explota

# BIEN:
fee_ids = df["fee_id"].to_pylist()
value = fee_ids[i]               # → str → psycopg2 OK
```

---

## Arquitectura de validación

Una de las cosas que aprendimos en este proyecto es que la validación preventiva vale más que el debugging reactivo. Por eso el stack incluye tres scripts que se ejecutan antes de desarrollar y después de deployar:

**`make preflight`** — valida conectividad e integración de cada servicio antes de escribir un solo DAG. Crea una tabla Iceberg de prueba, escribe una fila, la lee, la borra. Si eso no funciona, hay un problema de red o de versiones que resolver primero.

**`make validate-schemas`** — verifica que ningún schema tiene `required=True` y que cada uno puede hacer un round-trip completo con PyArrow, incluyendo valores `None`.

**`make acceptance-test`** — post-bootstrap, confirma que el stack completo está funcionando: tablas Iceberg con datos, KPIs en PostgreSQL, fuente Nessie configurada en Dremio, dashboards en Metabase.

---

## Números del prototipo

Corriendo en un MacBook Pro con Docker Desktop (16 GB asignados a Docker):

| Métrica | Valor |
|---------|-------|
| Alumnos generados | 5.000 |
| Registros de fees | ~210.000 |
| Registros de pagos | ~185.000 |
| Calificaciones | ~10.000 |
| Tablas Iceberg | 12 (8 bronze + 4 silver) |
| Tiempo de pipeline completo | ~5 min |
| Tiempo de query Dremio (sin caché) | 1–3 s |
| Tiempo de dashboard Metabase | < 200 ms |
| Memoria total del stack | ~10–12 GB |

---

## Qué hay en el repositorio

```
github.com/untaldouglas/datalakehouse
├── docker-compose.yml          # 10 servicios, todo declarado
├── Makefile                    # make bootstrap arranca todo
├── pipelines/
│   ├── dags/
│   │   ├── common/lakehouse.py # catálogo, schemas, upsert_iceberg()
│   │   ├── 01_moodle_bronze_dag.py
│   │   ├── 02_erpnext_bronze_dag.py
│   │   ├── 03_silver_transform_dag.py
│   │   └── 04_gold_materialize_dag.py
│   └── scripts/
│       ├── seed_data.py        # genera datos de prueba con Faker
│       ├── preflight_check.py  # smoke tests pre-desarrollo
│       ├── validate_schemas.py # validación schemas Iceberg ↔ PyArrow
│       └── acceptance_test.py  # validación end-to-end post-deploy
├── metabase/setup/configure_metabase.py
├── services/
│   ├── nessie/application.properties  # credenciales S3 para Nessie
│   └── metabase/init/                 # schema SQL de la capa semántica
└── docs/
    ├── STACK.md                # matriz de compatibilidad de versiones
    ├── TROUBLESHOOTING.md      # 12 bugs documentados con causa y solución
    ├── OPERACIONES.md          # runbook operativo
    └── RETRO_DEVOPS.md         # auditoría del proceso de desarrollo
```

Un solo comando levanta el stack completo desde cero:

```bash
cp .env.example .env
make gen-keys    # genera claves Fernet y Secret para Airflow
make bootstrap   # ~15 min la primera vez
```

`make bootstrap` hace exactamente lo que su nombre dice: levanta los 10 servicios, espera a que estén healthy, ejecuta un pre-flight check, carga 5.000 alumnos de prueba, corre los 4 DAGs, configura Dremio y Metabase, y finaliza con un acceptance test automático.

---

## Lo que sigue

Este prototipo demuestra que el patrón es sólido. Las extensiones naturales son:

**Streaming con Apache Kafka + Flink** — para datos que cambian en tiempo real (presencia en aula, alertas académicas tempranas). Iceberg tiene soporte nativo para writes desde Flink.

**dbt sobre la capa Silver** — para teams que prefieren definir las transformaciones en SQL versionado. dbt tiene un adaptador para Iceberg/DuckDB que puede leer directamente del catálogo Nessie.

**Data quality con Great Expectations o Soda** — validación de datos entre capas, directamente integrable como operadores de Airflow.

**Multi-tenant** — un branch Nessie por departamento académico. Cada uno ve sus propios datos, con posibilidad de hacer merge a `main` para reportes consolidados.

---

## Reflexión final

La narrativa común sobre los data lakehouses dice que son para empresas grandes con equipos de ingeniería dedicados. Este prototipo demuestra que no es así. Con Docker Compose, Python, y las herramientas correctas de la comunidad open source, una institución mediana puede tener una plataforma de datos que hubiera costado millones hace diez años.

Lo más valioso del stack no es ninguna herramienta individual — es la combinación: Iceberg da durabilidad y evolución de esquema, Nessie da versionado y la posibilidad de experimentar sin riesgo, Dremio da SQL federado sin mover datos, y Airflow da orquestación declarativa con observabilidad. Cada pieza hace una cosa bien y se integra limpiamente con las demás.

El código, la documentación, los scripts de validación y el runbook operativo completo están en:

**[github.com/untaldouglas/datalakehouse](https://github.com/untaldouglas/datalakehouse)**

Si lo usás, lo adaptás o lo rompés de alguna manera interesante — abrí un issue.

---

*Stack: Python 3.11 · Apache Iceberg 1.x · PyIceberg 0.7.1 · Nessie 0.108.0 · Airflow 2.9.1 · Dremio OSS 25.0 · Metabase 0.62 · MinIO · PostgreSQL 15 · Docker Compose*
