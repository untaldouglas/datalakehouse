# Retrospectiva DevOps — Universidad Data Lakehouse

> **Perspectiva**: Auditoría de proceso escrita desde el rol de DevOps Senior con experiencia en proyectos de Data Engineering sobre stacks open-source.
> **Fecha de referencia**: Junio 2026
> **Objetivo**: Convertir la experiencia de integración de este proyecto en un proceso replicable, con menos errores, mayor calidad y menor tiempo de ejecución.

---

## 1. Resumen Ejecutivo

El proyecto logró integrar exitosamente 10 servicios Docker en un pipeline ETL completo (Bronze → Silver → Gold), con Dremio como capa analítica y Metabase como capa de presentación. El stack funciona. Sin embargo, el camino hasta ese resultado estuvo marcado por **12 errores de integración evitables**, todos con una causa raíz común: **la ausencia de una fase de validación pre-implementación**.

El tiempo perdido en debugging reactivo (resetear Nessie, corregir schemas, reparar APIs) representó estimativamente el 60–70% del esfuerzo total del proyecto. Con el proceso propuesto en este documento, ese mismo stack debería levantarse en una primera sesión de trabajo, sin retrocesos.

---

## 2. Inventario de Errores — Clasificación y Causa Raíz

Cada error encontrado se clasifica por **fase de origen** (cuándo debió haberse detectado) y **tipo de causa raíz**.

| # | Error | Fase donde se detectó | Fase donde debió detectarse | Tipo |
|---|-------|-----------------------|-----------------------------|------|
| 1 | `required=True` en Iceberg → PyArrow siempre nullable | ETL Runtime | Pre-implementación | Incompatibilidad de tipos |
| 2 | ExternalTaskSensor nunca pasa en triggers manuales | ETL Runtime | Diseño de DAGs | Patrón Airflow erróneo |
| 3 | OOM exit code -9 en tareas paralelas | ETL Runtime | Pre-implementación | Planificación de recursos |
| 4 | `TRUNCATE RESTART IDENTITY` → falta ownership de secuencia PG | ETL Runtime | Pre-implementación | Permisos DB mal diseñados |
| 5 | `can't adapt type 'pyarrow.lib.LargeStringScalar'` en psycopg2 | ETL Runtime | Pre-implementación | Falta de contrato de tipos |
| 6 | Dremio usa `/api/v3/catalog`, no `/apiv2/source/` | Setup | Pre-implementación | API changelog no revisado |
| 7 | Nessie requiere endpoint `/api/v2` para Dremio | Setup | Pre-implementación | API changelog no revisado |
| 8 | Dremio exige `AT BRANCH "main"` en SQL sobre Nessie | Setup | Pre-implementación | Documentación no leída |
| 9 | Metabase 0.62 cambió `POST cards` → `PUT cards` | Setup | Pre-implementación | API changelog no revisado |
| 10 | `setup_dremio.py` usaba `localhost` dentro del contenedor | Setup | Code review | Error de networking básico |
| 11 | Nessie requiere `application.properties` para credenciales S3 | Arranque | Pre-implementación | Limitación de Quarkus no documentada |
| 12 | Tabla Iceberg con schema incorrecto requiere drop manual | ETL Runtime | Consecuencia del error #1 | Efecto en cadena |

**Patrón dominante**: 9 de 12 errores ocurrieron porque una pieza crítica de documentación (changelog de API, comportamiento de una librería, limitación de un framework) no fue consultada antes de escribir código.

---

## 3. Análisis de Causa Raíz Profunda

### 3.1 No existía una Matriz de Compatibilidad pre-definida

El stack combina 6 tecnologías con APIs propias que evolucionan de manera independiente:

```
PyIceberg 0.7.1 ↔ Nessie 0.108.0 ↔ Dremio 25.0 ↔ Metabase 0.62
     ↕                    ↕
PyArrow 15.0.2        MinIO (latest — riesgo)
```

Ninguna de las incompatibilidades era obvia: PyIceberg 0.7.x eliminó el tipo de catálogo `nessie` nativo en silencio; Dremio 25.0 migró su API a v3; Metabase 0.62 cambió el comportamiento de un endpoint de dashboard. Estos cambios aparecen en changelogs y en issues de GitHub, pero no en la documentación principal.

**Impacto**: cada incompatibilidad se descubrió en runtime, requiriendo resetear estado ya creado (volúmenes Nessie, tablas Iceberg) y re-ejecutar pipelines completos.

### 3.2 No hubo una fase de Proof of Concept (PoC) por integración

El proyecto saltó directamente al desarrollo del pipeline sin validar cada conexión de manera aislada. El orden correcto hubiera sido:

```
Airflow → Nessie (write 1 row) → Nessie → Dremio (query 1 row) → Dremio → Metabase (1 card)
```

Sin ese PoC incremental, un error en cualquier punto contaminaba toda la cadena.

### 3.3 Los schemas Iceberg se diseñaron con una suposición incorrecta

El uso de `required=True` en `NestedField` es válido en la especificación Iceberg pero incompatible con PyArrow, que siempre genera columnas nullable. Esta es una restricción conocida de PyIceberg documentada en su changelog desde 0.6.x. No se consultó antes de definir los schemas.

### 3.4 El patrón `ExternalTaskSensor` con triggers manuales no fue diseñado desde el principio

Airflow's `ExternalTaskSensor` compara `execution_date` de manera exacta. Cuando los DAGs se disparan manualmente en momentos diferentes, los timestamps no coinciden nunca. La solución (`execution_date_fn` que busca el último run exitoso) es estándar, pero requiere conocer esta limitación antes de implementar los sensores.

### 3.5 La planificación de recursos fue optimista sin evidencia

El scheduler de Airflow se configuró con `mem_limit: 1g`. Con 4 tareas paralelas (cada una cargando ~200k filas vía PyArrow), el pico real de memoria era ~1.8–2.4 GB. No hubo estimación previa.

---

## 4. Recomendaciones — El Proceso Mejorado

Se propone un proceso de 5 fases para proyectos similares. El tiempo total estimado para este stack sería de **1 sesión de trabajo** (4–6 horas) vs. las múltiples sesiones reactivas del proceso actual.

---

### FASE 0 — Definición del Stack y Matriz de Compatibilidad (30 min)

**Antes de escribir una sola línea de código**, construir la matriz de compatibilidad consultando changelogs y release notes de cada versión seleccionada.

**Artefacto de salida**: `docs/STACK.md`

```markdown
## Stack seleccionado y referencias de compatibilidad

| Componente | Versión | Notas críticas de compatibilidad |
|---|---|---|
| Nessie | 0.108.0 | Expone REST Iceberg en `/iceberg/v1/config` desde 0.76+ |
| PyIceberg | 0.7.1 | `CatalogType.NESSIE` eliminado en 0.7.x — usar `type=rest` |
| PyArrow | 15.0.2 | Siempre genera tipos nullable — NUNCA usar `required=True` en NestedField |
| Dremio OSS | 25.0 | API migrada a `/api/v3/catalog`; Nessie requiere endpoint `/api/v2` |
| Metabase | 0.62 | Dashboard cards: `PUT /api/dashboard/{id}/cards` (full list) |
| MinIO | FIJAR versión, no usar `latest` | `latest` puede romper compatibilidad |
| Airflow | 2.9.1 | ExternalTaskSensor: usar `execution_date_fn` para triggers manuales |
```

**Checklist de la fase**:
- [ ] Revisar GitHub releases de cada componente (buscar "breaking changes")
- [ ] Verificar issues abiertos de integración entre pares clave (ej. "pyiceberg nessie 0.7")
- [ ] Fijar TODAS las versiones de imágenes Docker (sin `latest`)
- [ ] Documentar la URL de referencia de cada "gotcha" encontrado

---

### FASE 1 — Validación de Infraestructura (45 min)

Levantar solo la infraestructura base y validar cada conexión con un script de smoke test antes de escribir cualquier DAG.

**Script de pre-flight** a crear como `scripts/preflight_check.py`:

```python
#!/usr/bin/env python3
"""
Ejecutar ANTES de desarrollar cualquier DAG.
Valida que cada servicio es alcanzable y que las integraciones clave funcionan.
"""

def check_minio():
    """Crear bucket de prueba, subir objeto, leerlo."""

def check_nessie_rest():
    """GET /iceberg/v1/config — confirmar que expone REST catalog."""

def check_pyiceberg_write():
    """Crear tabla de 1 fila en Nessie vía PyIceberg con schema completamente nullable."""

def check_dremio_api():
    """GET /api/v3/catalog — confirmar versión de API accesible."""

def check_nessie_in_dremio():
    """Crear fuente Nessie de prueba vía Dremio API y eliminarla."""

def check_metabase_api():
    """POST /api/session — confirmar login y versión de API."""

def check_postgres_permissions():
    """Verificar que el usuario analytics tiene permisos sobre tablas Y secuencias."""

if __name__ == "__main__":
    checks = [
        check_minio, check_nessie_rest, check_pyiceberg_write,
        check_dremio_api, check_nessie_in_dremio,
        check_metabase_api, check_postgres_permissions,
    ]
    for check in checks:
        try:
            check()
            print(f"  ✅ {check.__name__}")
        except Exception as e:
            print(f"  ❌ {check.__name__}: {e}")
```

**Regla clave**: si cualquier check falla, no se avanza a la siguiente fase. Se resuelve primero.

---

### FASE 2 — Schema Design con Validación Round-Trip (20 min)

Antes de escribir los DAGs, definir todos los schemas Iceberg y validarlos en un notebook o script:

```python
# scripts/validate_schemas.py
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType
import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow

schema = Schema(
    NestedField(1, "id", LongType()),      # SIN required=True
    NestedField(2, "name", StringType()),   # SIN required=True
)

# Validar que PyArrow puede serializar y deserializar el schema
arrow_schema = schema_to_pyarrow(schema)
table = pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=arrow_schema)
assert table.schema == arrow_schema, "Schema mismatch!"
print("✅ Schema válido para PyArrow")

# Regla de oro: si un campo es nullable en la fuente (MySQL, MariaDB),
# DEBE ser nullable en Iceberg. required=True solo para PKs conocidas y
# solo si la fuente garantiza NOT NULL.
```

**Regla de oro**: todo campo que venga de una base de datos relacional debe ser `nullable=True` en Iceberg (omitir `required=True` en `NestedField`). PyArrow no puede crear arrays no-nullable desde resultados de queries.

---

### FASE 3 — Desarrollo Incremental con Tests de Integración (el grueso del trabajo)

#### 3.1 Orden de desarrollo obligatorio

```
1. DAG Bronze (una sola tabla) → validar en MinIO + Nessie
2. DAG Silver (misma tabla) → validar schema round-trip
3. DAG Gold (mismo subset) → validar en PostgreSQL
4. Dremio setup (solo source) → query manual
5. Metabase setup (1 card) → verificar visual
6. Extender a todas las tablas una vez que la cadena funciona end-to-end
```

Nunca escalar horizontalmente (más tablas) hasta que el camino vertical (una tabla por cada capa) esté probado.

#### 3.2 Checklist de diseño de DAGs Airflow

Antes de escribir cualquier `ExternalTaskSensor`:

```python
# SIEMPRE usar execution_date_fn para tolerancia a triggers manuales:
def _latest_run(upstream_dag_id):
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

# USO:
wait_upstream = ExternalTaskSensor(
    task_id="wait_upstream",
    external_dag_id="upstream_dag",
    execution_date_fn=_latest_run("upstream_dag"),  # NO execution_date=None
    mode="reschedule",
    poke_interval=30,
    timeout=3600,
)
```

#### 3.3 Estimación de memoria antes de configurar `mem_limit`

Para pipelines con PyArrow sobre datasets de más de 50k filas:

```
Memoria estimada por tarea = (filas × columnas × 8 bytes) × 3
                           = overhead de PyArrow (lectura + escritura + buffer)

Ejemplo: 210k filas × 15 columnas × 8 bytes × 3 ≈ 756 MB por tarea

mem_limit del scheduler = tareas_paralelas_máximas × memoria_por_tarea × 1.3 (margen)
                        = 1 × 756 MB × 1.3 ≈ 1 GB mínimo para tarea única
```

Si múltiples tareas corren en paralelo, multiplicar en consecuencia. Preferir pipelines seriales sobre paralelos cuando la memoria es limitada.

#### 3.4 Permisos PostgreSQL — diseñar desde el schema

En `services/metabase/init/00_create_databases.sql`, incluir siempre:

```sql
-- Al crear el usuario analytics:
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO analytics;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO analytics;  -- CRÍTICO para SERIAL/IDENTITY
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO analytics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO analytics;
```

Y evitar `TRUNCATE TABLE x RESTART IDENTITY` a menos que el usuario sea propietario de la secuencia. Usar `DELETE FROM x` o gestionar IDs manualmente.

---

### FASE 4 — Validación End-to-End antes de declarar éxito

Un script de aceptación que se ejecuta después de todo el setup:

```bash
# Makefile target: make acceptance-test
acceptance-test:
    @echo "=== Validación end-to-end ==="
    @echo "1. Tablas Iceberg..."
    @docker exec airflow-scheduler python3 -c "
    from common.lakehouse import get_catalog
    cat = get_catalog()
    tables = cat.list_tables('bronze') + cat.list_tables('silver')
    assert len(tables) >= 12, f'Faltan tablas: {len(tables)}'
    print(f'  ✅ {len(tables)} tablas Iceberg')
    "
    @echo "2. KPIs en PostgreSQL..."
    @docker exec metabase-db psql -U metabase -d universidad_analytics -t -c \
        "SELECT COUNT(*) FROM kpi_financiero_mensual" | grep -v "^0$$" && \
        echo "  ✅ KPIs financieros presentes" || echo "  ❌ Sin KPIs financieros"
    @echo "3. Dremio source Nessie..."
    @curl -sf http://localhost:9047/api/v3/catalog | python3 -c \
        "import sys,json; data=json.load(sys.stdin); \
        sources=[e for e in data.get('data',[]) if e.get('name')=='lakehouse']; \
        assert sources, 'Fuente lakehouse no encontrada en Dremio'; \
        print('  ✅ Dremio fuente lakehouse activa')"
    @echo "4. Metabase dashboards..."
    @# (login y verificar cards count)
    @echo "=== Validación completada ==="
```

---

## 5. Recomendaciones de Ingeniería de Prompting con IA

El proceso de trabajo con un LLM como herramienta de desarrollo tiene sus propios patrones de mejora.

### 5.1 El Prompt de Inicio — Contexto que ahorra 70% del debugging

Un prompt de inicio de proyecto debe incluir:

```
STACK (con versiones exactas):
- Nessie 0.108.0, PyIceberg 0.7.1, PyArrow 15.0.2
- Dremio OSS 25.0, Metabase 0.62, Airflow 2.9.1

RESTRICCIONES CONOCIDAS (para este stack):
- PyIceberg 0.7.x: usar catalog type=rest, NO nessie nativo
- PyArrow: NUNCA required=True en NestedField (siempre nullable)
- Dremio 25.0: API en /api/v3/catalog; Nessie endpoint /api/v2
- Dremio + Nessie: toda query SQL requiere AT BRANCH "main"
- Metabase 0.62: dashboard cards via PUT (no POST)
- ExternalTaskSensor: siempre usar execution_date_fn para triggers manuales
- PostgreSQL: TRUNCATE sin RESTART IDENTITY si el user no es owner de la secuencia

NETWORKING (dentro de contenedores Docker):
- Los scripts que corren dentro de contenedores usan nombres de servicio, no localhost
- ej: DREMIO_HOST=http://dremio:9047, NESSIE_URI=http://nessie:19120/api/v1
```

Con este contexto inicial, la IA no genera código que viola ninguna de estas restricciones, eliminando de raíz todos los errores tipo 1, 2, 4, 5, 6, 7, 8, 9, 10 del inventario.

### 5.2 Solicitar validación antes de implementación

En lugar de pedir "implementa el DAG Silver", pedir:

```
"Antes de escribir el código, lista todas las asunciones sobre tipos de datos,
comportamiento de PyArrow, y permisos de base de datos que el código va a hacer.
Luego implementa."
```

Esto fuerza a la IA a externalizar suposiciones que de otro modo quedan implícitas en el código.

### 5.3 Solicitar el script de smoke test antes del código de producción

```
"Antes de escribir el DAG, escribe un script de validación que confirme que
la conexión a Nessie funciona, que el schema round-trip PyArrow↔Iceberg es
correcto, y que el usuario de PostgreSQL tiene los permisos necesarios."
```

### 5.4 Dividir el trabajo en fases verificables

```
Sesión 1: "Solo infraestructura + preflight_check.py — nada de DAGs todavía."
Sesión 2: "Bronze DAG para UNA tabla + validación end-to-end."
Sesión 3: "Extender Bronze a todas las tablas."
(...)
```

Este ritmo permite detectar errores de integración en el momento en que se introducen, sin acumular deuda de debugging.

### 5.5 Preservar el contexto entre sesiones

El archivo `docs/STACK.md` (restricciones) y `docs/TROUBLESHOOTING.md` (errores resueltos) deben existir desde el principio del proyecto y ser incluidos como contexto en cada nueva sesión de trabajo. Son la "memoria de proyecto" que evita redescubrir los mismos problemas.

---

## 6. El Skill Reutilizable — Plantilla de Proyecto Data Lakehouse

El conjunto de artefactos que se propone mantener y reutilizar en proyectos similares:

```
project-template/
├── docs/
│   ├── STACK.md                    # Matriz de compatibilidad (llenar primero)
│   └── KNOWN_ISSUES.md             # Problemas conocidos del stack (alimentar continuamente)
│
├── scripts/
│   ├── preflight_check.py          # Smoke tests de integración (ejecutar antes de DAGs)
│   ├── validate_schemas.py         # Round-trip test de schemas Iceberg+PyArrow
│   └── acceptance_test.sh          # Validación end-to-end post-setup
│
├── patterns/
│   ├── airflow_external_sensor.py  # ExternalTaskSensor con execution_date_fn
│   ├── iceberg_schema_safe.py      # Template de schema sin required=True
│   ├── pyarrow_to_psycopg2.py      # Conversión segura PyArrow → Python antes de psycopg2
│   └── dremio_api_v3.py            # Cliente Dremio API v3 reutilizable
│
└── Makefile                        # Targets: preflight, acceptance-test, bootstrap
```

### Checklist de inicio de proyecto (5 pasos, 90 minutos)

- [ ] **1. Llenar `STACK.md`** — versiones exactas + notas de compatibilidad (30 min)
- [ ] **2. Ejecutar `preflight_check.py`** — todos los checks en verde antes de continuar (20 min)
- [ ] **3. Ejecutar `validate_schemas.py`** — confirmar round-trip de schemas (10 min)
- [ ] **4. Implementar pipeline para UNA tabla end-to-end** — Bronze→Silver→Gold→Dremio→Metabase (30 min)
- [ ] **5. Ejecutar `acceptance_test.sh`** — validar que el pipeline mínimo funciona (10 min)

Solo después de completar estos 5 pasos se escala al pipeline completo.

---

## 7. Métricas del Proceso — Antes y Después

| Métrica | Proceso actual (este proyecto) | Proceso propuesto |
|---------|--------------------------------|-------------------|
| Errores de integración detectados | 12 | 1–2 (residuales) |
| Resets de Nessie necesarios | 3+ | 0 |
| Re-ejecuciones de pipelines por schema incorrecto | 5+ | 0 |
| Tiempo estimado hasta stack funcional | 3–5 sesiones | 1 sesión (4–6h) |
| Porcentaje de tiempo en debugging reactivo | ~65% | ~10% |
| Documentación generada al final | Sí (tardía) | Sí (temprana, como input) |

---

## 8. Conclusión

El error sistémico de este proyecto no fue técnico — fue de proceso. Todos los problemas encontrados estaban documentados en changelogs, issues de GitHub y documentación oficial de los componentes. La brecha fue no consultar esa información antes de implementar.

El cambio más impactante para futuros proyectos similares es simple: **invertir los primeros 90 minutos en validación antes que en implementación**. `preflight_check.py` ejecutado exitosamente antes de escribir el primer DAG hubiera eliminado 10 de los 12 errores del inventario.

El segundo cambio más impactante es proporcionar a la IA las restricciones conocidas del stack como contexto inicial, no como correcciones después del hecho. La IA no adivina que PyIceberg 0.7.x rompió compatibilidad con Nessie nativo — pero si se le dice, tampoco genera el código incorrecto.

---

*Documento generado post-mortem — Proyecto Universidad Data Lakehouse, Junio 2026.*
*Aplicar como checklist en el inicio de cualquier proyecto con stack Apache Iceberg + Nessie + Airflow.*
