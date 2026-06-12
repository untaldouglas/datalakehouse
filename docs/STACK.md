# Matriz de Compatibilidad del Stack

> Este documento debe existir ANTES de escribir cualquier DAG.  
> Es el primer artefacto a completar en cualquier proyecto nuevo con este stack.  
> Incluirlo como contexto al inicio de cada sesión de trabajo con IA.

---

## Versiones Confirmadas y Funcionales

| Componente | Versión | Imagen Docker | Notas |
|---|---|---|---|
| **Nessie** | 0.108.0 | `ghcr.io/projectnessie/nessie:0.108.0` | Mínimo para REST Iceberg catalog |
| **PyIceberg** | 0.7.1 | (en imagen Airflow) | Ver restricciones abajo |
| **PyArrow** | 15.0.2 | (en imagen Airflow) | Siempre genera tipos nullable |
| **Airflow** | 2.9.1 | `apache/airflow:2.9.1` | LocalExecutor |
| **Dremio OSS** | 25.0 | `dremio/dremio-oss:25.0` | API migrada a v3 |
| **Metabase** | 0.62 | `metabase/metabase:v0.62.x` | API de cards cambió a PUT |
| **MinIO** | FIJAR versión | NO usar `latest` | `latest` puede romper compatibilidad |
| **MySQL** | 8.0 | `mysql:8.0` | Fuente Moodle |
| **MariaDB** | 10.6 | `mariadb:10.6` | Fuente ERPNext |
| **PostgreSQL** | 15 | `postgres:15-alpine` | Capa semántica |

---

## Restricciones Conocidas — Incluir como contexto al usar IA

El siguiente bloque debe pegarse al inicio de cualquier sesión de trabajo con un LLM cuando se trabaja con este stack:

```
RESTRICCIONES DEL STACK (Universidad Data Lakehouse):

PyIceberg 0.7.x:
- catalog type=rest (NO nessie nativo — fue eliminado en 0.7.x)
- URI: http://nessie:19120/iceberg/  (con trailing slash, sin /api/v1)
- NUNCA usar required=True en NestedField — PyArrow siempre genera nullable
- La conexión Nessie requiere credenciales S3 en application.properties (no env vars)

Dremio 25.0:
- API de catálogo en /api/v3/catalog (NO /apiv2/source/)
- Nessie endpoint debe ser /api/v2 (NO /api/v1)
- Todo SQL sobre tablas Nessie requiere: AT BRANCH "main"
- Ejemplo: SELECT * FROM lakehouse.bronze.erpnext_students AT BRANCH "main" LIMIT 10

Metabase 0.62:
- Dashboard cards: PUT /api/dashboard/{id}/cards con lista completa
  (NO POST /api/dashboard/{id}/cards — este endpoint ya no existe)

Airflow 2.9.1:
- ExternalTaskSensor + triggers manuales: siempre usar execution_date_fn
  que busca el último run exitoso (NO confiar en execution_date exacto)
- OOM en LocalExecutor: cada tarea PyArrow consume ~300MB por 100k filas
  mem_limit = tareas_paralelas × 300MB × filas/100k × 1.3 (margen)

PostgreSQL:
- NUNCA usar TRUNCATE ... RESTART IDENTITY si el user no es owner de la secuencia
- Siempre incluir en SQL de inicialización:
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO analytics;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO analytics;

Networking Docker (scripts que corren DENTRO de contenedores):
- Usar nombre de servicio: dremio:9047, nessie:19120, minio:9000, metabase-db:5432
- NUNCA usar localhost desde dentro de un contenedor Docker
```

---

## Integraciones y sus Endpoints

### PyIceberg → Nessie

```python
load_catalog("nessie", **{
    "type":                "rest",
    "uri":                 "http://nessie:19120/iceberg/",   # trailing slash
    "warehouse":           "s3://lakehouse",
    "s3.endpoint":         "http://minio:9000",
    "s3.access-key-id":    "minioadmin",
    "s3.secret-access-key":"minioadmin123",
    "s3.path-style-access":"true",
})
```

### Nessie → Credenciales S3 (application.properties)

Nessie requiere credenciales S3 en `/deployments/config/application.properties`.
Las variables de entorno `AWS_*` NO funcionan con Quarkus cuando los nombres
de propiedades tienen guiones en llaves de mapas.

```properties
nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:minio.s3creds
minio.s3creds.name=${MINIO_ROOT_USER}
minio.s3creds.secret=${MINIO_ROOT_PASSWORD}
```

### Dremio → Nessie (source config)

```json
{
  "entityType": "source",
  "name": "lakehouse",
  "type": "NESSIE",
  "config": {
    "nessieEndpoint": "http://nessie:19120/api/v2",
    "nessieAuthType": "NONE",
    "awsRootPath": "lakehouse",
    "credentialType": "ACCESS_KEY",
    "accessKey": "minioadmin",
    "accessSecret": "minioadmin123",
    "secure": false,
    "propertyList": [
      {"name": "fs.s3a.path.style.access", "value": "true"},
      {"name": "fs.s3a.endpoint",          "value": "minio:9000"}
    ]
  }
}
```

---

## Reglas de Diseño de Schemas Iceberg

### Regla 1: Nunca `required=True` con fuentes relacionales

```python
# INCORRECTO — causa ValidationError en runtime:
Schema(
    NestedField(1, "student_code", StringType(), required=True),  # ❌
)

# CORRECTO — PyArrow puede generar nullable arrays:
Schema(
    NestedField(1, "student_code", StringType()),  # ✅ required=False por defecto
)
```

**Por qué**: PyArrow genera arrays con tipo nullable independientemente de si el campo
es NOT NULL en la base de datos fuente. Iceberg detecta el mismatch y lanza:
```
ValidationError: Mismatch in fields: student_code: required string | student_code: optional string
```

### Regla 2: Siempre convertir a Python antes de psycopg2

```python
# INCORRECTO — psycopg2 no sabe serializar tipos PyArrow:
value = df["column"][i]  # pyarrow.lib.LargeStringScalar → ERROR

# CORRECTO — convertir a lista Python primero:
values = df["column"].to_pylist()
value = values[i]  # str/int/float → OK
```

### Regla 3: ExternalTaskSensor para triggers manuales

```python
# INCORRECTO — execution_date exacto nunca coincide en triggers manuales:
ExternalTaskSensor(external_dag_id="upstream")  # queda bloqueado para siempre

# CORRECTO — buscar el último run exitoso:
def _latest(dt, **kwargs):
    from airflow.models import DagRun
    from airflow import settings
    session = settings.Session()
    run = session.query(DagRun).filter(
        DagRun.dag_id == "upstream_dag_id",
        DagRun.state == "success",
    ).order_by(DagRun.execution_date.desc()).first()
    return run.execution_date if run else dt

ExternalTaskSensor(external_dag_id="upstream", execution_date_fn=_latest)  # ✅
```

---

## Checklist de Inicio de Proyecto

Completar en orden. No avanzar si algún step falla.

- [ ] **1. Completar este documento** (`docs/STACK.md`) con versiones exactas (30 min)
- [ ] **2. Revisar changelogs** de cada versión seleccionada buscando "breaking changes" (20 min)
- [ ] **3. Levantar infra base** (`make up`) y ejecutar pre-flight: `make preflight` (20 min)
- [ ] **4. Validar schemas** antes de escribir DAGs: `make validate-schemas` (10 min)
- [ ] **5. Pipeline vertical mínimo**: UNA tabla Bronze→Silver→Gold→Dremio→Metabase (30 min)
- [ ] **6. Aceptación**: `make acceptance-test` — todos los checks en verde (10 min)
- [ ] **7. Escalar** a todas las tablas y fuentes solo tras completar el paso 6

**Tiempo total estimado para el stack completo**: 1 sesión de trabajo (4–6 horas).

---

## Referencias de Changelogs

| Componente | URL | Secciones a revisar |
|---|---|---|
| PyIceberg | https://github.com/apache/iceberg-python/blob/main/CHANGES.md | "Breaking changes", "Removed" |
| Nessie | https://github.com/projectnessie/nessie/releases | "Breaking changes" |
| Dremio OSS | https://docs.dremio.com/current/release-notes/ | "API changes" |
| Metabase | https://github.com/metabase/metabase/releases | "API changes" |
| Airflow | https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html | "Breaking changes" |
