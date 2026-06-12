#!/usr/bin/env python3
"""
Acceptance test — ejecutar después de 'make bootstrap' para confirmar
que el stack completo está funcionando end-to-end.

Verifica: Iceberg tables → PostgreSQL KPIs → Dremio source → Metabase dashboards

Uso:
    make acceptance-test
    # o directamente:
    docker compose exec airflow-scheduler python3 /opt/airflow/scripts/acceptance_test.py
"""

import os
import sys
import requests
import psycopg2

sys.path.insert(0, "/opt/airflow/dags")

NESSIE_BASE      = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1").replace("/api/v1", "")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
LAKEHOUSE_BUCKET = os.environ.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")
DREMIO_HOST      = os.environ.get("DREMIO_HOST", "http://dremio:9047")
DREMIO_USER      = os.environ.get("DREMIO_ADMIN_USER", "admin")
DREMIO_PASS      = os.environ.get("DREMIO_ADMIN_PASSWORD", "Admin1234!")
METABASE_HOST    = os.environ.get("METABASE_HOST", "http://metabase:3000")
METABASE_USER    = os.environ.get("METABASE_ADMIN_EMAIL", "admin@universidad.edu")
METABASE_PASS    = os.environ.get("METABASE_ADMIN_PASSWORD", "Admin1234!")
PG_HOST          = os.environ.get("METABASE_DB_HOST", "metabase-db")
PG_USER          = os.environ.get("METABASE_DB_USER", "metabase")
PG_PASS          = os.environ.get("METABASE_DB_PASSWORD", "metabase_secret_2024")
PG_DB            = os.environ.get("SEMANTIC_DB_NAME", "universidad_analytics")
NUM_STUDENTS     = int(os.environ.get("NUM_STUDENTS", "5000"))

results = []
warnings = []


def check(name, category=""):
    def decorator(fn):
        def wrapper():
            label = f"[{category}] {name}" if category else name
            try:
                detail = fn()
                msg = f": {detail}" if detail else ""
                results.append(("OK", label, ""))
                print(f"  \033[32m✅ {label}{msg}\033[0m")
            except AssertionError as e:
                results.append(("FAIL", label, str(e)))
                print(f"  \033[31m❌ {label}\033[0m")
                print(f"     → {e}")
            except Exception as e:
                results.append(("FAIL", label, str(e)[:120]))
                print(f"  \033[31m❌ {label}\033[0m")
                print(f"     → {str(e)[:120]}")
        return wrapper
    return decorator


# ── 1. Iceberg / Nessie ───────────────────────────────────────────────────────

@check("Tablas Bronze (≥ 8)", "Iceberg")
def check_bronze_tables():
    from common.lakehouse import get_catalog
    catalog = get_catalog()
    bronze = list(catalog.list_tables("bronze"))
    assert len(bronze) >= 8, f"Solo {len(bronze)} tablas bronze, se esperan ≥ 8"
    return f"{len(bronze)} tablas"


@check("Tablas Silver (≥ 4)", "Iceberg")
def check_silver_tables():
    from common.lakehouse import get_catalog
    catalog = get_catalog()
    silver = list(catalog.list_tables("silver"))
    assert len(silver) >= 4, f"Solo {len(silver)} tablas silver, se esperan ≥ 4"
    return f"{len(silver)} tablas"


@check("Datos en bronze.erpnext_students", "Iceberg")
def check_bronze_students_data():
    from common.lakehouse import get_catalog
    catalog = get_catalog()
    t = catalog.load_table(("bronze", "erpnext_students"))
    n = len(t.scan().to_arrow())
    assert n > 0, "Tabla vacía"
    expected_min = NUM_STUDENTS // 2
    if n < expected_min:
        warnings.append(f"bronze.erpnext_students tiene {n} filas, se esperan ≥ {NUM_STUDENTS}")
    return f"{n:,} filas"


@check("Datos en silver.students", "Iceberg")
def check_silver_students_data():
    from common.lakehouse import get_catalog
    catalog = get_catalog()
    t = catalog.load_table(("silver", "students"))
    n = len(t.scan().to_arrow())
    assert n > 0, "Tabla vacía"
    return f"{n:,} filas"


# ── 2. PostgreSQL — Capa Semántica ────────────────────────────────────────────

def pg_count(table):
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS,
                            connect_timeout=10)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    conn.close()
    return count


@check("dim_alumno poblada", "PostgreSQL")
def check_dim_alumno():
    n = pg_count("dim_alumno")
    assert n > 0, "dim_alumno está vacía — el DAG Gold no completó"
    return f"{n:,} alumnos"


@check("kpi_financiero_mensual poblada", "PostgreSQL")
def check_kpi_financiero():
    n = pg_count("kpi_financiero_mensual")
    assert n > 0, "Sin KPIs financieros — DAG Gold no completó"
    return f"{n} períodos"


@check("kpi_academico_periodo poblada", "PostgreSQL")
def check_kpi_academico():
    n = pg_count("kpi_academico_periodo")
    assert n > 0, "Sin KPIs académicos — DAG Gold no completó"
    return f"{n} períodos"


@check("fact_ingresos_matricula poblada", "PostgreSQL")
def check_fact_ingresos():
    n = pg_count("fact_ingresos_matricula")
    assert n > 0, "fact_ingresos_matricula vacía"
    return f"{n:,} registros"


# ── 3. Dremio ─────────────────────────────────────────────────────────────────

def dremio_token():
    r = requests.post(f"{DREMIO_HOST}/apiv2/login",
                      json={"userName": DREMIO_USER, "password": DREMIO_PASS},
                      timeout=15)
    assert r.status_code == 200, f"Login Dremio falló: HTTP {r.status_code}"
    return r.json()["token"]


@check("Fuente 'lakehouse' (Nessie) configurada", "Dremio")
def check_dremio_nessie_source():
    token = dremio_token()
    headers = {"Authorization": f"_dremio{token}"}
    r = requests.get(f"{DREMIO_HOST}/api/v3/catalog", headers=headers, timeout=10)
    assert r.status_code == 200
    sources = [e for e in r.json().get("data", []) if e.get("name") == "lakehouse"]
    assert sources, "Fuente 'lakehouse' no encontrada en Dremio — ejecutar 'make setup-dremio'"
    return f"type={sources[0].get('type', '?')}"


@check("Espacio 'analytics' con vistas existe", "Dremio")
def check_dremio_analytics_space():
    token = dremio_token()
    headers = {"Authorization": f"_dremio{token}"}
    r = requests.get(f"{DREMIO_HOST}/api/v3/catalog", headers=headers, timeout=10)
    assert r.status_code == 200
    spaces = [e for e in r.json().get("data", []) if e.get("name") == "analytics"]
    assert spaces, "Espacio 'analytics' no encontrado — ejecutar 'make setup-dremio'"


# ── 4. Metabase ───────────────────────────────────────────────────────────────

def metabase_token():
    r = requests.post(f"{METABASE_HOST}/api/session",
                      json={"username": METABASE_USER, "password": METABASE_PASS},
                      timeout=15)
    assert r.status_code == 200, f"Login Metabase falló: HTTP {r.status_code}"
    return r.json()["id"]


@check("Base de datos 'universidad_analytics' conectada en Metabase", "Metabase")
def check_metabase_db():
    token = metabase_token()
    headers = {"X-Metabase-Session": token}
    r = requests.get(f"{METABASE_HOST}/api/database", headers=headers, timeout=10)
    assert r.status_code == 200
    dbs = r.json().get("data", r.json()) if isinstance(r.json(), dict) else r.json()
    if isinstance(dbs, dict):
        dbs = dbs.get("data", [])
    names = [d.get("name", "") for d in dbs]
    assert any("universidad" in n.lower() or "analytics" in n.lower() for n in names), \
        f"BD universidad_analytics no encontrada. BDs: {names}"


@check("Dashboards creados (≥ 2)", "Metabase")
def check_metabase_dashboards():
    token = metabase_token()
    headers = {"X-Metabase-Session": token}
    r = requests.get(f"{METABASE_HOST}/api/dashboard", headers=headers, timeout=10)
    assert r.status_code == 200
    dashboards = r.json()
    assert len(dashboards) >= 2, \
        f"Solo {len(dashboards)} dashboards — ejecutar 'make setup-metabase'"
    return f"{len(dashboards)} dashboards"


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║       Acceptance Test — Universidad Lakehouse        ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    checks_by_group = [
        ("Iceberg / Nessie", [
            check_bronze_tables, check_silver_tables,
            check_bronze_students_data, check_silver_students_data,
        ]),
        ("PostgreSQL — Capa Semántica", [
            check_dim_alumno, check_kpi_financiero,
            check_kpi_academico, check_fact_ingresos,
        ]),
        ("Dremio", [
            check_dremio_nessie_source, check_dremio_analytics_space,
        ]),
        ("Metabase", [
            check_metabase_db, check_metabase_dashboards,
        ]),
    ]

    for group, fns in checks_by_group:
        print(f"  --- {group} ---")
        for fn in fns:
            fn()
        print()

    passed = sum(1 for r in results if r[0] == "OK")
    failed = sum(1 for r in results if r[0] == "FAIL")

    if warnings:
        print("  \033[33m⚠ Advertencias:\033[0m")
        for w in warnings:
            print(f"    • {w}")
        print()

    if failed == 0:
        print(f"  \033[32m✅ Stack validado ({passed}/{passed} checks). Listo para producción.\033[0m")
        sys.exit(0)
    else:
        print(f"  \033[31m❌ {failed} check(s) fallaron — el stack NO está completo.\033[0m")
        print()
        print("  Acciones sugeridas:")
        for _, name, msg in results:
            if name and msg:
                print(f"    • {name}: {msg}")
        print()
        print("  Ver: make health, make etl-status, docs/TROUBLESHOOTING.md")
        sys.exit(1)
