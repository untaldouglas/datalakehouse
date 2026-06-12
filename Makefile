# ============================================================
# Universidad Data Lakehouse — Makefile
# Uso: make <target>         make help  para ver todos los comandos
# ============================================================

.PHONY: help check-env up down restart restart-service logs logs-service status \
        health seed setup-dremio setup-metabase bootstrap \
        trigger-etl reset-etl etl-status wait-etl \
        iceberg-tables semantic-check \
        nessie-reset nessie-branches nessie-tables \
        open-airflow open-minio open-dremio open-metabase open-nessie open-all \
        minio-ls clean reset build lint ps \
        preflight validate-schemas acceptance-test \
        session-end session-status

# ── Configuración ────────────────────────────────────────────
-include .env
export

COMPOSE      = docker compose
AIRFLOW_EXEC = $(COMPOSE) exec airflow-scheduler airflow
PYTHON_RUN   = $(COMPOSE) run --rm -e DREMIO_HOST=http://dremio:9047 data-seeder python

# ── Ayuda ────────────────────────────────────────────────────

## help: Muestra esta ayuda
help:
	@echo ""
	@echo "  Universidad Data Lakehouse"
	@echo "  =========================="
	@echo ""
	@echo "  INICIO"
	@grep -E '^## (up|down|bootstrap|check-env|build|status|health|ps):' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""
	@echo "  CALIDAD — ejecutar en orden antes de desarrollar DAGs"
	@grep -E '^## (preflight|validate-schemas|acceptance-test):' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""
	@echo "  DATOS"
	@grep -E '^## (seed|setup-dremio|setup-metabase|trigger-etl|reset-etl|etl-status|wait-etl|iceberg-tables|semantic-check):' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""
	@echo "  DIAGNÓSTICO"
	@grep -E '^## (logs|logs-service|nessie-branches|nessie-tables|nessie-reset|minio-ls):' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""
	@echo "  NAVEGADOR"
	@grep -E '^## open' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""
	@echo "  SESIÓN"
	@grep -E '^## (session-end|session-status):' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""
	@echo "  MANTENIMIENTO"
	@grep -E '^## (restart|restart-service|clean|reset|lint):' $(MAKEFILE_LIST) | sed 's/## /    /'
	@echo ""


# ── Prerequisitos ────────────────────────────────────────────

## check-env: Verifica que .env existe y tiene las claves requeridas
check-env:
	@test -f .env || (echo "" && echo "ERROR: falta el archivo .env" && echo "  Solución: cp .env.example .env" && echo "  Luego edita .env con tus valores." && echo "" && exit 1)
	@for key in MINIO_ROOT_USER MINIO_ROOT_PASSWORD AIRFLOW_FERNET_KEY AIRFLOW_SECRET_KEY; do \
	  val=$$(grep "^$$key=" .env | cut -d= -f2); \
	  if [ -z "$$val" ] || echo "$$val" | grep -q "replace_me"; then \
	    echo "ERROR: $$key no está configurado en .env"; \
	    echo "  Genera las claves con: make gen-keys"; \
	    exit 1; \
	  fi; \
	done
	@echo "OK .env verificado"

## gen-keys: Genera AIRFLOW_FERNET_KEY y AIRFLOW_SECRET_KEY aleatorios e inserta en .env
gen-keys:
	@which python3 >/dev/null 2>&1 || (echo "ERROR: python3 no encontrado" && exit 1)
	@FERNET=$$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"); \
	 SECRET=$$(python3 -c "import secrets; print(secrets.token_hex(32))"); \
	 sed -i.bak "s|AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$FERNET|" .env; \
	 sed -i.bak "s|AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=$$SECRET|" .env; \
	 rm -f .env.bak; \
	 echo "OK Claves generadas e insertadas en .env"


# ── Ciclo de vida de los servicios ───────────────────────────

## up: Inicia todos los servicios (primera vez: ~5 min para construir imágenes)
up: check-env
	@echo "→ Iniciando Universidad Data Lakehouse..."
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Servicios iniciando. Usa 'make status' para verificar salud."
	@echo ""
	@echo "  Airflow:  http://localhost:$(AIRFLOW_PORT)       usuario: admin"
	@echo "  MinIO:    http://localhost:$(MINIO_CONSOLE_PORT)  usuario: $(MINIO_ROOT_USER)"
	@echo "  Dremio:   http://localhost:$(DREMIO_UI_PORT)      usuario: admin"
	@echo "  Metabase: http://localhost:$(METABASE_PORT)"
	@echo "  Nessie:   http://localhost:$(NESSIE_PORT)/api/v2/config"
	@echo ""
	@echo "Tip: 'make health' cuando todos los servicios estén verdes"

## down: Detiene todos los servicios (conserva volúmenes de datos)
down:
	$(COMPOSE) down

## restart: Reinicia todos los servicios
restart: down up

## restart-service: Reinicia un servicio específico (uso: make restart-service s=nessie)
restart-service:
	@test -n "$(s)" || (echo "ERROR: especifica el servicio con s=<nombre>" && exit 1)
	$(COMPOSE) restart $(s)

## build: Construye/reconstruye imágenes Docker sin iniciar
build:
	$(COMPOSE) build


# ── Monitoreo ────────────────────────────────────────────────

## status: Estado resumido de todos los contenedores
status:
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

## ps: Alias de status
ps: status

## health: Verificación completa: servicios + tablas Iceberg + datos en PostgreSQL
health:
	@echo ""
	@echo "=== SERVICIOS ==="
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null | grep -v "^NAME"
	@echo ""
	@echo "=== TABLAS ICEBERG ==="
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
import sys; sys.path.insert(0, '/opt/airflow/dags'); \
from common.lakehouse import get_catalog; \
catalog = get_catalog(); \
[print(f'  {\".\".join(t)}: OK') for ns in catalog.list_namespaces() for t in catalog.list_tables(ns)] \
" 2>/dev/null || echo "  (Nessie no disponible o sin tablas)"
	@echo ""
	@echo "=== CAPA SEMÁNTICA (PostgreSQL) ==="
	@$(COMPOSE) exec -T metabase-db psql -U ${METABASE_DB_USER} -d ${SEMANTIC_DB_NAME} -t -c "\
SELECT '  ' || rpad(tablename, 30) || TO_CHAR((SELECT COUNT(*) FROM information_schema.tables WHERE table_name=t.tablename), '999,999') || ' registros' \
FROM pg_tables t WHERE schemaname='public' AND tablename NOT LIKE 'dim_tiempo%' ORDER BY tablename;" \
2>/dev/null || echo "  (Base semántica no disponible)"
	@echo ""
	@echo "=== ESTADO ETL (Airflow) ==="
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
from airflow.models import DagRun; \
from airflow import settings; \
session = settings.Session(); \
dags = ['01_moodle_to_bronze','02_erpnext_to_bronze','03_bronze_to_silver','04_silver_to_gold']; \
[print(f'  {d}: ' + (lambda r: r.state if r else 'sin ejecucion')(session.query(DagRun).filter(DagRun.dag_id==d).order_by(DagRun.execution_date.desc()).first())) for d in dags] \
" 2>/dev/null || echo "  (Airflow no disponible)"
	@echo ""

## logs: Sigue los logs de todos los servicios
logs:
	$(COMPOSE) logs -f --tail=100

## logs-service: Sigue los logs de un servicio específico (uso: make logs-service s=dremio)
logs-service:
	@test -n "$(s)" || (echo "ERROR: especifica el servicio con s=<nombre>" && exit 1)
	$(COMPOSE) logs -f --tail=200 $(s)


# ── Calidad y Validación ─────────────────────────────────────

## preflight: Valida conectividad e integración de todos los servicios antes de desarrollar
# Verifica: MinIO, Nessie REST catalog, PyIceberg round-trip, Dremio API v3, Metabase, PostgreSQL permisos
# Ejecutar siempre después de 'make up' y antes de escribir cualquier DAG
preflight:
	@echo ""
	@echo "→ Ejecutando pre-flight checks..."
	@$(COMPOSE) exec -T airflow-scheduler python3 /opt/airflow/scripts/preflight_check.py
	@echo ""

## validate-schemas: Verifica que todos los schemas Iceberg son compatibles con PyArrow
# Confirma: sin required=True, conversión a PyArrow exitosa, round-trip con datos reales
# Ejecutar después de modificar schemas en lakehouse.py o DAGs
validate-schemas:
	@echo ""
	@echo "→ Validando schemas Iceberg ↔ PyArrow..."
	@$(COMPOSE) exec -T airflow-scheduler python3 /opt/airflow/scripts/validate_schemas.py
	@echo ""

## acceptance-test: Validación end-to-end del stack completo post-bootstrap
# Verifica: tablas Iceberg, KPIs en PostgreSQL, Dremio source, Metabase dashboards
# Ejecutar después de 'make bootstrap' para confirmar que todo funciona
acceptance-test:
	@echo ""
	@echo "→ Ejecutando acceptance tests (validación end-to-end)..."
	@$(COMPOSE) exec -T -e DREMIO_HOST=http://dremio:9047 \
	             -e METABASE_HOST=http://metabase:3000 \
	             airflow-scheduler \
	             python3 /opt/airflow/scripts/acceptance_test.py
	@echo ""


# ── Datos y ETL ──────────────────────────────────────────────

## seed: Carga datos de prueba (5000 alumnos por defecto, configurable en .env con NUM_STUDENTS)
seed:
	@echo "→ Cargando datos de prueba (NUM_STUDENTS=$(NUM_STUDENTS))..."
	$(COMPOSE) run --rm data-seeder python /opt/airflow/scripts/seed_data.py
	@echo "OK Datos cargados"

## setup-dremio: Configura Dremio: crea fuente Nessie y vistas virtuales analíticas
setup-dremio:
	@echo "→ Configurando Dremio (fuente Nessie + vistas analíticas)..."
	$(PYTHON_RUN) /opt/airflow/scripts/setup_dremio.py
	@echo "OK Dremio configurado"

## setup-metabase: Configura Metabase: conecta a la BD semántica y crea los 2 dashboards
setup-metabase:
	@echo "→ Configurando Metabase (dashboards Gerencial y Académico)..."
	python3 metabase/setup/configure_metabase.py
	@echo ""
	@echo "  Dashboard Gerencial: http://localhost:$(METABASE_PORT)/dashboard/2"
	@echo "  Dashboard Académico: http://localhost:$(METABASE_PORT)/dashboard/3"
	@echo ""

## trigger-etl: Dispara los 4 DAGs de ETL en secuencia (espera entre pasos)
trigger-etl:
	@echo "→ Disparando pipeline ETL completo..."
	@echo "  [1/4] Moodle → Bronze..."
	@$(AIRFLOW_EXEC) dags trigger 01_moodle_to_bronze 2>/dev/null | grep -E "queued|running" | head -1 || true
	@echo "  [2/4] ERPNext → Bronze..."
	@$(AIRFLOW_EXEC) dags trigger 02_erpnext_to_bronze 2>/dev/null | grep -E "queued|running" | head -1 || true
	@echo "  [3/4] Bronze → Silver..."
	@$(AIRFLOW_EXEC) dags trigger 03_bronze_to_silver 2>/dev/null | grep -E "queued|running" | head -1 || true
	@echo "  [4/4] Silver → Gold..."
	@$(AIRFLOW_EXEC) dags trigger 04_silver_to_gold 2>/dev/null | grep -E "queued|running" | head -1 || true
	@echo ""
	@echo "OK DAGs disparados. Usa 'make etl-status' para monitorear."

## etl-status: Muestra el estado actual de las últimas ejecuciones de los 4 DAGs
etl-status:
	@echo ""
	@echo "Estado ETL (última ejecución por DAG):"
	@echo "───────────────────────────────────────────────────────────────────────"
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
from airflow.models import DagRun, TaskInstance; \
from airflow import settings; \
session = settings.Session(); \
dags = ['01_moodle_to_bronze','02_erpnext_to_bronze','03_bronze_to_silver','04_silver_to_gold']; \
for d in dags: \
    run = session.query(DagRun).filter(DagRun.dag_id==d).order_by(DagRun.execution_date.desc()).first(); \
    if run: \
        tis = session.query(TaskInstance).filter(TaskInstance.dag_id==d, TaskInstance.run_id==run.run_id).all(); \
        states = {ti.task_id: ti.state for ti in tis}; \
        ok = sum(1 for s in states.values() if s=='success'); \
        total = len(states); \
        badge = 'OK' if run.state=='success' else ('RUNNING' if run.state=='running' else 'FAIL'); \
        print(f'  [{badge:7s}] {d}: {ok}/{total} tareas — {run.state}'); \
    else: \
        print(f'  [------] {d}: sin ejecuciones') \
" 2>/dev/null || echo "  Airflow no disponible"
	@echo ""

## wait-etl: Espera a que todos los DAGs completen (útil en scripts de CI)
wait-etl:
	@echo "→ Esperando a que el ETL complete (máx. 20 min)..."
	@for i in $$(seq 1 120); do \
	  STATES=$$($(COMPOSE) exec -T airflow-scheduler python3 -c "\
from airflow.models import DagRun; from airflow import settings; session=settings.Session(); \
dags=['01_moodle_to_bronze','02_erpnext_to_bronze','03_bronze_to_silver','04_silver_to_gold']; \
print(' '.join([(lambda r: r.state if r else 'none')(session.query(DagRun).filter(DagRun.dag_id==d).order_by(DagRun.execution_date.desc()).first()) for d in dags]))" 2>/dev/null); \
	  echo "  $$i/120: $$STATES"; \
	  echo "$$STATES" | grep -qv "running\|queued" && echo "OK Todos los DAGs completaron." && break; \
	  sleep 10; \
	done

## reset-etl: Limpia el historial de ejecuciones de todos los DAGs y re-dispara el ETL completo
reset-etl:
	@echo "→ Limpiando historial ETL y re-disparando pipeline..."
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
from airflow.models import DagRun, TaskInstance; from airflow import settings; \
session=settings.Session(); \
dags=['01_moodle_to_bronze','02_erpnext_to_bronze','03_bronze_to_silver','04_silver_to_gold']; \
[session.query(TaskInstance).filter(TaskInstance.dag_id==d).delete() for d in dags]; \
[session.query(DagRun).filter(DagRun.dag_id==d).delete() for d in dags]; \
session.commit(); print('Historial limpiado.')" 2>/dev/null
	@$(MAKE) trigger-etl


# ── Diagnóstico del Lakehouse ─────────────────────────────────

## iceberg-tables: Lista todas las tablas Iceberg con número de filas
iceberg-tables:
	@echo ""
	@echo "Tablas Iceberg en Nessie (rama main):"
	@echo "─────────────────────────────────────"
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
import sys; sys.path.insert(0, '/opt/airflow/dags'); \
from common.lakehouse import get_catalog; \
catalog = get_catalog(); \
for ns in sorted(catalog.list_namespaces()): \
    for t in sorted(catalog.list_tables(ns)): \
        try: \
            n = len(catalog.load_table(t).scan().to_arrow()); \
            print(f'  {\".\" .join(t):40s} {n:>10,} filas') \
        except Exception as e: print(f'  {\".\" .join(t):40s} ERROR: {e}') \
" 2>/dev/null || echo "  Nessie no disponible"
	@echo ""

## semantic-check: Verifica los KPIs clave en la capa semántica de PostgreSQL
semantic-check:
	@echo ""
	@echo "Capa Semántica — universidad_analytics:"
	@echo "────────────────────────────────────────"
	@$(COMPOSE) exec -T metabase-db psql -U ${METABASE_DB_USER} -d ${SEMANTIC_DB_NAME} -c "\
SELECT \
    (SELECT COUNT(*) FROM dim_alumno)              AS estudiantes, \
    (SELECT COUNT(DISTINCT programa_codigo) FROM dim_alumno) AS programas, \
    (SELECT ROUND(SUM(ingresos_facturados)::numeric/1000000,2) FROM kpi_financiero_mensual) AS facturado_M, \
    (SELECT ROUND(AVG(promedio_notas)::numeric,1) FROM kpi_academico_periodo WHERE promedio_notas>0) AS nota_prom, \
    (SELECT COUNT(*) FROM fact_calificaciones)     AS calificaciones, \
    (SELECT COUNT(*) FROM etl_run_log)             AS etl_runs;" \
	2>/dev/null || echo "  Base semántica no disponible"
	@echo ""

## nessie-branches: Lista las ramas del catálogo Nessie
nessie-branches:
	@curl -s http://localhost:$(NESSIE_PORT)/api/v2/trees | python3 -m json.tool 2>/dev/null || echo "Nessie no disponible"

## nessie-tables: Lista las entradas de la rama main en Nessie
nessie-tables:
	@curl -s "http://localhost:$(NESSIE_PORT)/api/v2/trees/main/entries?maxRecords=100" | \
	  python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'  {e[\"name\"][\"elements\"]}') for e in d.get('entries',[])]" 2>/dev/null || echo "Nessie no disponible"

## minio-ls: Lista objetos en el bucket principal del lakehouse
minio-ls:
	@$(COMPOSE) exec -T minio mc ls local/$(MINIO_BUCKET_LAKEHOUSE)/ 2>/dev/null || \
	  $(COMPOSE) exec -T minio sh -c "mc alias set local http://localhost:9000 $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) >/dev/null 2>&1 && mc ls local/$(MINIO_BUCKET_LAKEHOUSE)/"


# ── Recuperación Nessie ───────────────────────────────────────

## nessie-reset: Reinicia Nessie con volumen limpio (borra metadatos Iceberg, NO los datos Parquet)
# ADVERTENCIA: requiere re-ejecutar todo el ETL después (make reset-etl)
nessie-reset:
	@echo "ADVERTENCIA: Esto borrará los metadatos Iceberg en Nessie."
	@echo "Los archivos Parquet en MinIO NO se borran, pero las tablas deberán recrearse."
	@read -p "¿Confirmas? (yes/no): " c && [ "$$c" = "yes" ]
	@echo "→ Deteniendo Nessie..."
	$(COMPOSE) stop nessie
	$(COMPOSE) rm -f nessie
	@echo "→ Borrando volumen de datos Nessie..."
	docker volume rm $$(docker volume ls -q | grep nessie_data) 2>/dev/null || true
	@echo "→ Reiniciando Nessie con volumen limpio..."
	$(COMPOSE) up -d nessie
	@echo "OK Nessie reiniciado. Espera ~30s y ejecuta: make reset-etl"


# ── Inicio rápido completo ────────────────────────────────────

## bootstrap: Primera puesta en marcha completa (up + preflight + seed + ETL + Dremio + Metabase + acceptance-test)
# Ejecutar una sola vez después de 'cp .env.example .env && make gen-keys'
bootstrap: check-env
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║   Universidad Data Lakehouse — Bootstrap Completo   ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""
	@echo "[1/8] Iniciando servicios..."
	$(COMPOSE) up -d --build
	@echo ""
	@echo "[2/8] Esperando a que todos los servicios estén saludables (máx. 5 min)..."
	@for svc in airflow-webserver airflow-scheduler nessie dremio metabase; do \
	  echo "  Esperando $$svc..."; \
	  for i in $$(seq 1 30); do \
	    STATUS=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null); \
	    [ "$$STATUS" = "healthy" ] && echo "  OK $$svc" && break; \
	    [ $$i -eq 30 ] && echo "  TIMEOUT $$svc (continúa de todas formas)" && break; \
	    sleep 10; \
	  done; \
	done
	@echo ""
	@echo "[3/8] Pre-flight check (validación de integraciones)..."
	@$(COMPOSE) exec -T airflow-scheduler python3 /opt/airflow/scripts/preflight_check.py || \
	  (echo "" && echo "ERROR: Pre-flight fallido — revisar servicios antes de continuar." && exit 1)
	@echo ""
	@echo "[4/8] Cargando datos de prueba ($(NUM_STUDENTS) alumnos)..."
	$(COMPOSE) run --rm data-seeder python /opt/airflow/scripts/seed_data.py
	@echo ""
	@echo "[5/8] Ejecutando pipeline ETL completo..."
	@$(MAKE) trigger-etl
	@echo "  (Esperando 60 segundos para que los DAGs inicien...)"
	@sleep 60
	@$(MAKE) wait-etl
	@echo ""
	@echo "[6/8] Configurando Dremio..."
	$(PYTHON_RUN) /opt/airflow/scripts/setup_dremio.py
	@echo ""
	@echo "[7/8] Configurando Metabase..."
	python3 metabase/setup/configure_metabase.py
	@echo ""
	@echo "[8/8] Acceptance test (validación end-to-end)..."
	@$(COMPOSE) exec -T -e DREMIO_HOST=http://dremio:9047 \
	             -e METABASE_HOST=http://metabase:3000 \
	             airflow-scheduler \
	             python3 /opt/airflow/scripts/acceptance_test.py || \
	  echo "  ADVERTENCIA: Algunos checks fallaron — ver 'make acceptance-test' para detalle."
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║              Bootstrap completado                    ║"
	@echo "╠══════════════════════════════════════════════════════╣"
	@echo "║  Airflow:  http://localhost:$(AIRFLOW_PORT)              ║"
	@echo "║  Dremio:   http://localhost:$(DREMIO_UI_PORT)            ║"
	@echo "║  Metabase: http://localhost:$(METABASE_PORT)             ║"
	@echo "║  MinIO:    http://localhost:$(MINIO_CONSOLE_PORT)        ║"
	@echo "║                                                      ║"
	@echo "║  Credenciales: admin / Admin1234!                    ║"
	@echo "║  Metabase:     admin@universidad.edu / Admin1234!    ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""


# ── Abrir en navegador ────────────────────────────────────────

## open-airflow: Abre Airflow en el navegador
open-airflow:
	@open http://localhost:$(AIRFLOW_PORT) 2>/dev/null || xdg-open http://localhost:$(AIRFLOW_PORT)

## open-minio: Abre la consola de MinIO en el navegador
open-minio:
	@open http://localhost:$(MINIO_CONSOLE_PORT) 2>/dev/null || xdg-open http://localhost:$(MINIO_CONSOLE_PORT)

## open-dremio: Abre Dremio en el navegador
open-dremio:
	@open http://localhost:$(DREMIO_UI_PORT) 2>/dev/null || xdg-open http://localhost:$(DREMIO_UI_PORT)

## open-metabase: Abre Metabase en el navegador
open-metabase:
	@open http://localhost:$(METABASE_PORT) 2>/dev/null || xdg-open http://localhost:$(METABASE_PORT)

## open-nessie: Abre la API de Nessie en el navegador (útil para depuración)
open-nessie:
	@open http://localhost:$(NESSIE_PORT)/api/v2/trees 2>/dev/null || xdg-open http://localhost:$(NESSIE_PORT)/api/v2/trees

## open-all: Abre todos los servicios en el navegador
open-all: open-airflow open-minio open-dremio open-metabase


# ── Gestión de sesión ────────────────────────────────────────
#
# session-end  : Cierre limpio de sesión de trabajo.
#   1. Muestra resumen del estado actual (git, servicios, ETL, datos).
#   2. Escribe un snapshot en SESSION_SNAPSHOT.md (sobrescribe cada vez).
#   3. Detiene los servicios preservando todos los datos (docker compose stop).
#   4. Imprime las instrucciones exactas para reanudar.
#
# session-status : Solo muestra el resumen sin detener nada.
#                  Útil para consultar el estado en cualquier momento.
#
# Para reanudar después de session-end:
#   cd <directorio> && make up && make health
# ─────────────────────────────────────────────────────────────

SESSION_FILE = SESSION_SNAPSHOT.md

## session-status: Muestra el estado actual del proyecto sin detener servicios
session-status:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║        Estado del Proyecto — Data Lakehouse          ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  Fecha:  $$(date '+%Y-%m-%d %H:%M:%S %Z')"
	@echo "  Repo:   $$(git remote get-url origin 2>/dev/null || echo 'sin remote')"
	@echo "  Commit: $$(git log --oneline -1 2>/dev/null)"
	@echo "  Branch: $$(git branch --show-current 2>/dev/null)"
	@echo ""
	@PENDING=$$(git status --porcelain 2>/dev/null | wc -l | tr -d ' '); \
	  if [ "$$PENDING" -gt 0 ]; then \
	    echo "  ⚠ Cambios sin commitear: $$PENDING archivo(s)"; \
	    git status --short 2>/dev/null | sed 's/^/    /'; \
	  else \
	    echo "  ✓ Working tree limpio — sin cambios pendientes"; \
	  fi
	@echo ""
	@echo "  --- Servicios Docker ---"
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null | grep -v "^NAME" | sed 's/^/  /' || \
	  echo "  (Docker no disponible)"
	@echo ""
	@echo "  --- ETL (última ejecución por DAG) ---"
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
from airflow.models import DagRun; from airflow import settings; \
session = settings.Session(); \
dags = ['01_moodle_to_bronze','02_erpnext_to_bronze','03_bronze_to_silver','04_silver_to_gold']; \
[print('  ' + ('✓' if (lambda r: r.state if r else 'none')(session.query(DagRun).filter(DagRun.dag_id==d).order_by(DagRun.execution_date.desc()).first()) == 'success' else '✗') + ' ' + d + ': ' + (lambda r: r.state + ' (' + str(r.execution_date)[:16] + ')' if r else 'sin ejecuciones')(session.query(DagRun).filter(DagRun.dag_id==d).order_by(DagRun.execution_date.desc()).first())) for d in dags]" \
	2>/dev/null || echo "  (Airflow no disponible)"
	@echo ""
	@echo "  --- Datos en PostgreSQL ---"
	@$(COMPOSE) exec -T metabase-db psql -U $${METABASE_DB_USER} -d $${SEMANTIC_DB_NAME} -t -c "\
SELECT '  alumnos: ' || COUNT(*) FROM dim_alumno \
UNION ALL SELECT '  kpi_financiero: ' || COUNT(*) || ' períodos' FROM kpi_financiero_mensual \
UNION ALL SELECT '  kpi_académico:  ' || COUNT(*) || ' períodos' FROM kpi_academico_periodo;" \
	2>/dev/null || echo "  (PostgreSQL no disponible)"
	@echo ""

## session-end: Cierre limpio de sesión — guarda snapshot, detiene servicios y muestra cómo reanudar
session-end:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║         Cierre de Sesión — Data Lakehouse            ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""
	@echo "[1/4] Verificando cambios sin commitear..."
	@PENDING=$$(git status --porcelain 2>/dev/null | wc -l | tr -d ' '); \
	  if [ "$$PENDING" -gt 0 ]; then \
	    echo ""; \
	    echo "  ⚠  Hay $$PENDING archivo(s) con cambios sin commitear:"; \
	    git status --short 2>/dev/null | sed 's/^/     /'; \
	    echo ""; \
	    echo "  Commiteá los cambios antes de cerrar:"; \
	    echo "    git add <archivos> && git commit -m 'mensaje'"; \
	    echo "    git push"; \
	    echo ""; \
	    read -p "  ¿Continuar de todas formas? (yes/no): " c && [ "$$c" = "yes" ]; \
	  else \
	    echo "  ✓ Working tree limpio."; \
	  fi
	@echo ""
	@echo "[2/4] Generando snapshot de sesión..."
	@echo "# SESSION SNAPSHOT" > $(SESSION_FILE)
	@echo "" >> $(SESSION_FILE)
	@echo "Generado: $$(date '+%Y-%m-%d %H:%M:%S %Z')" >> $(SESSION_FILE)
	@echo "" >> $(SESSION_FILE)
	@echo "## Git" >> $(SESSION_FILE)
	@echo "- Repo:   $$(git remote get-url origin 2>/dev/null)" >> $(SESSION_FILE)
	@echo "- Branch: $$(git branch --show-current 2>/dev/null)" >> $(SESSION_FILE)
	@echo "- Commit: $$(git log --oneline -1 2>/dev/null)" >> $(SESSION_FILE)
	@echo "" >> $(SESSION_FILE)
	@echo "## Commits recientes" >> $(SESSION_FILE)
	@git log --oneline -5 2>/dev/null | sed 's/^/- /' >> $(SESSION_FILE)
	@echo "" >> $(SESSION_FILE)
	@echo "## Estado ETL" >> $(SESSION_FILE)
	@$(COMPOSE) exec -T airflow-scheduler python3 -c "\
from airflow.models import DagRun; from airflow import settings; \
session = settings.Session(); \
dags = ['01_moodle_to_bronze','02_erpnext_to_bronze','03_bronze_to_silver','04_silver_to_gold']; \
[print('- ' + d + ': ' + (lambda r: r.state + ' (' + str(r.execution_date)[:16] + ')' if r else 'sin ejecuciones')(session.query(DagRun).filter(DagRun.dag_id==d).order_by(DagRun.execution_date.desc()).first())) for d in dags]" \
	2>/dev/null >> $(SESSION_FILE) || echo "- Airflow no disponible al cerrar" >> $(SESSION_FILE)
	@echo "" >> $(SESSION_FILE)
	@echo "## Datos (PostgreSQL)" >> $(SESSION_FILE)
	@$(COMPOSE) exec -T metabase-db psql -U $${METABASE_DB_USER} -d $${SEMANTIC_DB_NAME} -t -c "\
SELECT 'alumnos: ' || COUNT(*) FROM dim_alumno \
UNION ALL SELECT 'kpi_financiero: ' || COUNT(*) || ' períodos' FROM kpi_financiero_mensual \
UNION ALL SELECT 'kpi_académico: ' || COUNT(*) || ' períodos' FROM kpi_academico_periodo \
UNION ALL SELECT 'calificaciones: ' || COUNT(*) FROM fact_calificaciones;" \
	2>/dev/null | grep -v '^$$' | sed 's/^/- /' >> $(SESSION_FILE) || \
	  echo "- PostgreSQL no disponible al cerrar" >> $(SESSION_FILE)
	@echo "" >> $(SESSION_FILE)
	@echo "## Para reanudar" >> $(SESSION_FILE)
	@echo "\`\`\`bash" >> $(SESSION_FILE)
	@echo "cd $$(pwd)" >> $(SESSION_FILE)
	@echo "make up       # levanta todos los servicios" >> $(SESSION_FILE)
	@echo "make health   # verifica que todo está correcto" >> $(SESSION_FILE)
	@echo "\`\`\`" >> $(SESSION_FILE)
	@echo "  ✓ Snapshot guardado en $(SESSION_FILE)"
	@echo ""
	@echo "[3/4] Deteniendo servicios (los datos se conservan)..."
	@$(COMPOSE) stop 2>/dev/null && echo "  ✓ Servicios detenidos." || \
	  echo "  (Servicios ya detenidos o Docker no disponible)"
	@echo ""
	@echo "[4/4] Cierre completado."
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║  Sesión cerrada. Los datos están preservados.        ║"
	@echo "╠══════════════════════════════════════════════════════╣"
	@echo "║  Para reanudar:                                      ║"
	@echo "║    make up      → levanta los servicios              ║"
	@echo "║    make health  → verifica el estado                 ║"
	@echo "║  Snapshot:                                           ║"
	@echo "║    cat $(SESSION_FILE)                       ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""


# ── Mantenimiento ────────────────────────────────────────────

## clean: Detiene y elimina TODOS los contenedores y volúmenes (DESTRUYE TODOS LOS DATOS)
clean:
	@echo ""
	@echo "ADVERTENCIA: Esto eliminará todos los datos, incluyendo bases de datos y el lakehouse."
	@read -p "¿Confirmas borrado completo? (yes/no): " confirm && [ "$$confirm" = "yes" ]
	$(COMPOSE) down -v --remove-orphans
	@echo "OK Todo eliminado"

## reset: Limpieza completa + reconstrucción + inicio (equivale a bootstrap desde cero)
reset: clean bootstrap

## lint: Verifica el código Python de DAGs y scripts con ruff
lint:
	@which ruff >/dev/null 2>&1 || pip install ruff -q
	ruff check pipelines/dags/ pipelines/scripts/ metabase/setup/ --select E,W,F --ignore E501
	@echo "OK Lint pasado"
