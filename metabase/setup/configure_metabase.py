#!/usr/bin/env python3
"""
Metabase API setup script:
  1. Creates admin user (first-time setup)
  2. Connects to universidad_analytics PostgreSQL
  3. Creates Dashboard Gerencial (financial KPIs)
  4. Creates Dashboard Académico (academic KPIs)

Run after 'make up' and after first ETL cycle completes:
  docker compose exec airflow-scheduler \
    python /opt/airflow/scripts/configure_metabase.py
Or locally:
  python metabase/setup/configure_metabase.py
"""

import os
import time
import logging
import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MB_HOST      = os.environ.get("METABASE_HOST",     "http://localhost:3000")
MB_USER      = os.environ.get("METABASE_ADMIN_EMAIL", "admin@universidad.edu")
MB_PASS      = os.environ.get("METABASE_ADMIN_PASSWORD", "Admin1234!")
MB_FIRST     = os.environ.get("METABASE_FIRST_NAME", "Admin")
MB_LAST      = os.environ.get("METABASE_LAST_NAME",  "Universidad")
SITE_NAME    = os.environ.get("UNIVERSITY_NAME",    "Universidad Nacional Ficticia")

SEM_HOST     = os.environ.get("SEMANTIC_DB_HOST",     "metabase-db")
SEM_PORT     = int(os.environ.get("SEMANTIC_DB_PORT", 5432))
SEM_DB       = os.environ.get("SEMANTIC_DB_NAME",     "universidad_analytics")
SEM_USER     = os.environ.get("SEMANTIC_DB_USER",     "analytics")
SEM_PASS     = os.environ.get("SEMANTIC_DB_PASSWORD", "analytics_secret_2024")

session = requests.Session()


def wait_for_metabase(max_retries=30, delay=10):
    for i in range(max_retries):
        try:
            r = requests.get(f"{MB_HOST}/api/health", timeout=10)
            if r.status_code == 200 and r.json().get("status") == "ok":
                log.info("Metabase is up!")
                return
        except Exception as e:
            log.info(f"Waiting for Metabase ({i+1}/{max_retries}): {e}")
        time.sleep(delay)
    raise Exception("Metabase never became available")


def api(method, path, **kwargs):
    r = session.request(method, f"{MB_HOST}{path}", timeout=30, **kwargs)
    if r.status_code not in (200, 201, 202):
        log.warning(f"{method} {path} → {r.status_code}: {r.text[:300]}")
    return r


def setup_admin():
    """Create the admin user if Metabase is in setup mode."""
    r = requests.get(f"{MB_HOST}/api/session/properties", timeout=10)
    props = r.json()
    if props.get("has-user-setup"):
        log.info("Metabase already set up — logging in")
        return

    log.info("First-time Metabase setup...")
    token = props.get("setup-token", "")
    r = requests.post(f"{MB_HOST}/api/setup", json={
        "token": token,
        "user": {
            "email": MB_USER,
            "first_name": MB_FIRST,
            "last_name": MB_LAST,
            "password": MB_PASS,
            "password_confirm": MB_PASS,
            "site_name": SITE_NAME,
        },
        "prefs": {
            "site_name": SITE_NAME,
            "allow_tracking": False,
        },
    }, timeout=30)
    if r.status_code == 200:
        log.info("Admin user created")
    else:
        log.error(f"Setup failed: {r.text}")


def login():
    r = requests.post(f"{MB_HOST}/api/session", json={
        "username": MB_USER,
        "password": MB_PASS,
    }, timeout=30)
    r.raise_for_status()
    token = r.json()["id"]
    session.headers.update({"X-Metabase-Session": token})
    log.info("Logged in to Metabase")
    return token


def add_database():
    """Add the universidad_analytics PostgreSQL semantic database."""
    r = api("GET", "/api/database")
    existing = {db["name"] for db in r.json().get("data", [])}
    if "Universidad Analytics" in existing:
        log.info("Database already connected")
        dbs = r.json().get("data", [])
        return next(d["id"] for d in dbs if d["name"] == "Universidad Analytics")

    r = api("POST", "/api/database", json={
        "engine": "postgres",
        "name": "Universidad Analytics",
        "details": {
            "host": SEM_HOST,
            "port": SEM_PORT,
            "dbname": SEM_DB,
            "user": SEM_USER,
            "password": SEM_PASS,
            "ssl": False,
            "tunnel_enabled": False,
        },
        "auto_run_queries": True,
        "is_full_sync": True,
    })
    if r.status_code in (200, 201):
        db_id = r.json()["id"]
        log.info(f"Database connected: id={db_id}")
        return db_id
    raise Exception(f"Failed to add database: {r.text}")


def wait_for_sync(db_id: int, max_wait=120):
    """Wait until Metabase syncs table metadata."""
    log.info("Waiting for table sync...")
    for _ in range(max_wait // 5):
        r = api("GET", f"/api/database/{db_id}/metadata")
        tables = r.json().get("tables", [])
        if len(tables) >= 5:
            log.info(f"Sync complete: {len(tables)} tables found")
            return tables
        time.sleep(5)
    log.warning("Sync timeout — proceeding anyway")
    return []


def get_table_id(tables, name: str) -> int:
    for t in tables:
        if t["name"] == name:
            return t["id"]
    return None


def get_field_id(table_id: int, field_name: str) -> int:
    r = api("GET", f"/api/table/{table_id}/query_metadata")
    for f in r.json().get("fields", []):
        if f["name"] == field_name:
            return f["id"]
    return None


def create_collection(name: str, description: str = "") -> int:
    r = api("POST", "/api/collection", json={
        "name": name,
        "description": description,
        "color": "#509EE3",
    })
    if r.status_code in (200, 201):
        return r.json()["id"]
    r2 = api("GET", "/api/collection")
    for c in r2.json().get("data", []):
        if c["name"] == name:
            return c["id"]
    return None


def create_dashboard(name: str, description: str, collection_id: int) -> int:
    r = api("POST", "/api/dashboard", json={
        "name": name,
        "description": description,
        "collection_id": collection_id,
    })
    return r.json()["id"]


_dashboard_cards: dict = {}  # dashboard_id → list of card dicts


def add_card_to_dashboard(dashboard_id: int, card_id: int,
                          row: int, col: int, size_x: int, size_y: int):
    if dashboard_id not in _dashboard_cards:
        _dashboard_cards[dashboard_id] = []
    _dashboard_cards[dashboard_id].append({
        "id": -(len(_dashboard_cards[dashboard_id]) + 1),
        "card_id": card_id,
        "row": row,
        "col": col,
        "size_x": size_x,
        "size_y": size_y,
    })


def flush_dashboard_cards(dashboard_id: int):
    cards = _dashboard_cards.get(dashboard_id, [])
    if not cards:
        return
    r = api("PUT", f"/api/dashboard/{dashboard_id}/cards", json={"cards": cards})
    if r.status_code == 200:
        log.info(f"  Added {len(cards)} cards to dashboard {dashboard_id}")
    else:
        log.warning(f"  Dashboard cards flush: {r.status_code} {r.text[:200]}")


def create_question(name: str, db_id: int, sql: str, collection_id: int,
                    display: str = "bar", viz_settings: dict = None) -> int:
    payload = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": db_id,
        },
        "display": display,
        "visualization_settings": viz_settings or {},
        "collection_id": collection_id,
    }
    r = api("POST", "/api/card", json=payload)
    if r.status_code in (200, 201):
        card_id = r.json()["id"]
        log.info(f"  Card created: '{name}' (id={card_id})")
        return card_id
    log.error(f"  Card failed '{name}': {r.text[:200]}")
    return None


# ============================================================
# Dashboard Gerencial — Financial KPIs
# ============================================================

FINANCIAL_QUERIES = [
    {
        "name": "Ingresos Totales por Programa (Año Actual)",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
                ELSE programa_codigo
              END AS programa,
              SUM(ingresos_facturados) AS facturado,
              SUM(ingresos_cobrados)   AS cobrado,
              SUM(ingresos_pendientes) AS pendiente
            FROM kpi_financiero_mensual
            WHERE anio = EXTRACT(YEAR FROM CURRENT_DATE)::INT
            GROUP BY programa_codigo
            ORDER BY cobrado DESC
        """,
        "display": "bar",
        "viz": {
            "graph.x_axis.title_text": "Programa",
            "graph.y_axis.title_text": "USD",
            "series_settings": {
                "cobrado":   {"color": "#509EE3"},
                "pendiente": {"color": "#EF8C8C"},
            },
        },
    },
    {
        "name": "Ingresos Mensuales — Evolución Anual",
        "sql": """
            SELECT
              TO_CHAR(TO_DATE(mes::TEXT, 'MM'), 'Mon') AS mes,
              SUM(ingresos_cobrados) AS cobrado
            FROM kpi_financiero_mensual
            WHERE anio = EXTRACT(YEAR FROM CURRENT_DATE)::INT
            GROUP BY mes
            ORDER BY mes
        """,
        "display": "line",
        "viz": {"graph.x_axis.title_text": "Mes", "graph.y_axis.title_text": "USD Cobrado"},
    },
    {
        "name": "Tasa de Cobranza por Programa y Ciclo",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              ciclo_academico,
              ROUND(AVG(tasa_cobranza), 1) AS tasa_cobranza_pct,
              ROUND(AVG(tasa_morosidad), 1) AS tasa_morosidad_pct
            FROM kpi_financiero_mensual
            GROUP BY programa_codigo, ciclo_academico
            ORDER BY programa, ciclo_academico
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Alumnos Morosos Actuales por Programa",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              SUM(alumnos_morosos) AS morosos,
              SUM(alumnos_activos) AS activos,
              ROUND(SUM(alumnos_morosos)::NUMERIC / NULLIF(SUM(alumnos_activos), 0) * 100, 1) AS pct_morosos
            FROM kpi_financiero_mensual
            WHERE anio = EXTRACT(YEAR FROM CURRENT_DATE)::INT
            GROUP BY programa_codigo
            ORDER BY morosos DESC
        """,
        "display": "row",
        "viz": {},
    },
    {
        "name": "Cartera Vencida Total",
        "sql": """
            SELECT
              TO_CHAR(CURRENT_TIMESTAMP, 'DD/MM/YYYY HH24:MI') AS actualizado_al,
              ROUND(SUM(monto_pendiente), 2)                   AS cartera_vencida_usd,
              COUNT(DISTINCT alumno_codigo)                    AS alumnos_con_deuda
            FROM fact_ingresos_matricula
            WHERE estado_cobro NOT IN ('Paid', 'Pagado')
        """,
        "display": "scalar",
        "viz": {},
    },
    {
        "name": "Distribución Modo de Pago",
        "sql": """
            SELECT modo_pago, COUNT(*) AS cantidad, SUM(monto_pagado) AS monto_total
            FROM fact_ingresos_matricula
            WHERE monto_pagado > 0
            GROUP BY modo_pago
            ORDER BY monto_total DESC
        """,
        "display": "pie",
        "viz": {"pie.metric": "monto_total", "pie.dimension": "modo_pago"},
    },
    {
        "name": "Ingreso Promedio por Alumno (por programa)",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              ROUND(AVG(ingreso_promedio_alumno), 2) AS ingreso_promedio_usd
            FROM kpi_financiero_mensual
            WHERE anio = EXTRACT(YEAR FROM CURRENT_DATE)::INT
              AND alumnos_activos > 0
            GROUP BY programa_codigo
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Ingresos Acumulados vs Meta Anual",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              SUM(ingresos_cobrados)   AS cobrado_acumulado,
              SUM(ingresos_facturados) AS meta_facturada,
              ROUND(SUM(ingresos_cobrados) / NULLIF(SUM(ingresos_facturados), 0) * 100, 1) AS avance_pct
            FROM kpi_financiero_mensual
            WHERE anio = EXTRACT(YEAR FROM CURRENT_DATE)::INT
            GROUP BY programa_codigo
        """,
        "display": "progress",
        "viz": {},
    },
]

ACADEMIC_QUERIES = [
    {
        "name": "Tasa de Aprobación por Programa",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              ROUND(AVG(tasa_aprobacion), 1)  AS tasa_aprobacion_pct,
              ROUND(AVG(tasa_reprobacion), 1) AS tasa_reprobacion_pct
            FROM kpi_academico_periodo
            GROUP BY programa_codigo
            ORDER BY tasa_aprobacion_pct DESC
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Promedio de Notas por Programa y Ciclo",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              ciclo_academico,
              ROUND(AVG(promedio_notas), 2) AS promedio_nota
            FROM kpi_academico_periodo
            GROUP BY programa_codigo, ciclo_academico
            ORDER BY programa, ciclo_academico
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Top 10 Cursos con Mayor Reprobación",
        "sql": """
            SELECT
              curso_codigo,
              CASE LEFT(curso_codigo, 3)
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              ROUND(AVG(tasa_reprobacion), 1) AS reprobacion_pct,
              SUM(alumnos_matriculados) AS total_alumnos
            FROM kpi_academico_periodo
            GROUP BY curso_codigo
            ORDER BY reprobacion_pct DESC
            LIMIT 10
        """,
        "display": "table",
        "viz": {},
    },
    {
        "name": "Distribución de Calificaciones",
        "sql": """
            SELECT
              CASE
                WHEN nota_final >= 9  THEN 'Excelente (9-10)'
                WHEN nota_final >= 7  THEN 'Bueno (7-8.9)'
                WHEN nota_final >= 6  THEN 'Aprobado (6-6.9)'
                ELSE                       'Reprobado (<6)'
              END AS rango,
              COUNT(*) AS cantidad,
              ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS porcentaje
            FROM fact_calificaciones
            GROUP BY rango
            ORDER BY rango DESC
        """,
        "display": "pie",
        "viz": {"pie.metric": "cantidad", "pie.dimension": "rango"},
    },
    {
        "name": "Total de Alumnos por Programa",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              COUNT(*) AS total_alumnos,
              SUM(CASE WHEN estado = 'Activo' THEN 1 ELSE 0 END) AS activos
            FROM dim_alumno
            GROUP BY programa_codigo
            ORDER BY total_alumnos DESC
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Alumnos Inscritos por Año de Ingreso (Cohorte)",
        "sql": """
            SELECT
              EXTRACT(YEAR FROM fecha_ingreso)::INT AS anio_ingreso,
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              COUNT(*) AS alumnos
            FROM dim_alumno
            WHERE fecha_ingreso IS NOT NULL
            GROUP BY anio_ingreso, programa_codigo
            ORDER BY anio_ingreso, programa
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Genero por Programa",
        "sql": """
            SELECT
              CASE programa_codigo
                WHEN 'MED' THEN 'Medicina'
                WHEN 'INF' THEN 'Informática'
                WHEN 'GN'  THEN 'Gestión de Negocios'
              END AS programa,
              genero,
              COUNT(*) AS cantidad
            FROM dim_alumno
            WHERE genero IS NOT NULL AND genero != ''
            GROUP BY programa_codigo, genero
            ORDER BY programa, cantidad DESC
        """,
        "display": "bar",
        "viz": {},
    },
    {
        "name": "Cursos con Mayor Rendimiento Académico",
        "sql": """
            SELECT
              curso_codigo,
              ROUND(AVG(promedio_notas), 2) AS promedio,
              ROUND(AVG(tasa_aprobacion), 1) AS aprobacion_pct,
              SUM(alumnos_matriculados) AS alumnos
            FROM kpi_academico_periodo
            GROUP BY curso_codigo
            ORDER BY promedio DESC
            LIMIT 10
        """,
        "display": "table",
        "viz": {},
    },
]


def build_dashboard(db_id, collection_id, dashboard_name, description, queries):
    dash_id = create_dashboard(dashboard_name, description, collection_id)
    log.info(f"Dashboard '{dashboard_name}' created (id={dash_id})")

    POSITIONS = [(0, 0), (0, 4), (0, 8), (4, 0), (4, 4), (4, 8), (8, 0), (8, 4)]
    for i, q in enumerate(queries):
        card_id = create_question(
            q["name"], db_id, q["sql"].strip(), collection_id,
            display=q.get("display", "bar"),
            viz_settings=q.get("viz", {}),
        )
        if card_id and i < len(POSITIONS):
            row, col = POSITIONS[i]
            add_card_to_dashboard(dash_id, card_id, row, col, 4, 4)

    flush_dashboard_cards(dash_id)
    log.info(f"Dashboard '{dashboard_name}' populated with {len(queries)} cards")
    return dash_id


def main():
    wait_for_metabase()
    setup_admin()
    time.sleep(3)
    login()

    # Connect semantic DB
    db_id = add_database()
    tables = wait_for_sync(db_id)

    # Create collection
    col_id = create_collection(
        "Universidad Analytics",
        "Dashboards gerenciales y académicos de la Universidad",
    )

    # Build dashboards
    dash_gerencial = build_dashboard(
        db_id, col_id,
        "Dashboard Gerencial — Ventas y Cobranza",
        "KPIs financieros: ingresos por matrícula, cobranza, morosidad y rentabilidad por carrera",
        FINANCIAL_QUERIES,
    )
    dash_academico = build_dashboard(
        db_id, col_id,
        "Dashboard Académico — Indicadores Moodle",
        "KPIs académicos: calificaciones, aprobación, retención y actividad en Moodle",
        ACADEMIC_QUERIES,
    )

    log.info("=" * 60)
    log.info("Metabase setup complete!")
    log.info(f"  Dashboard Gerencial : {MB_HOST}/dashboard/{dash_gerencial}")
    log.info(f"  Dashboard Académico : {MB_HOST}/dashboard/{dash_academico}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
