#!/usr/bin/env python3
"""
Pre-flight validation — ejecutar antes de desarrollar cualquier DAG.

Valida que cada servicio es alcanzable y que las integraciones clave
funcionan correctamente. Si cualquier check falla, NO se avanza al desarrollo.

Uso:
    make preflight
    # o directamente:
    docker compose exec airflow-scheduler python3 /opt/airflow/scripts/preflight_check.py
"""

import os
import sys
import time
import json
import traceback
import requests
import psycopg2
import pyarrow as pa

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
ANALYTICS_PASS   = os.environ.get("ANALYTICS_DB_PASSWORD", "analytics_secret_2024")

results = []


def check(name):
    def decorator(fn):
        def wrapper():
            try:
                fn()
                results.append(("OK", name, ""))
                print(f"  \033[32m✅ {name}\033[0m")
            except Exception as e:
                msg = str(e).split("\n")[0][:120]
                results.append(("FAIL", name, msg))
                print(f"  \033[31m❌ {name}\033[0m")
                print(f"     → {msg}")
        return wrapper
    return decorator


# ── 1. MinIO ──────────────────────────────────────────────────────────────────

@check("MinIO alcanzable")
def check_minio_health():
    r = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=10)
    assert r.status_code == 200, f"HTTP {r.status_code}"


@check("MinIO bucket lakehouse existe")
def check_minio_bucket():
    import boto3
    from botocore.client import Config
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert LAKEHOUSE_BUCKET in buckets, f"bucket '{LAKEHOUSE_BUCKET}' no encontrado. Buckets: {buckets}"


# ── 2. Nessie ─────────────────────────────────────────────────────────────────

@check("Nessie API v2 alcanzable")
def check_nessie_api():
    r = requests.get(f"{NESSIE_BASE}/api/v2/config", timeout=10)
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}"


@check("Nessie expone REST Iceberg catalog (/iceberg/v1/config)")
def check_nessie_iceberg_rest():
    r = requests.get(f"{NESSIE_BASE}/iceberg/v1/config", timeout=10)
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}"
    data = r.json()
    assert "defaults" in data or "overrides" in data or "endpoints" in data, \
        f"Respuesta inesperada: {list(data.keys())}"


@check("PyIceberg escribe y lee en Nessie (round-trip)")
def check_pyiceberg_roundtrip():
    sys.path.insert(0, "/opt/airflow/dags")
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, LongType

    catalog = load_catalog("preflight", **{
        "type": "rest",
        "uri": f"{NESSIE_BASE}/iceberg/",
        "warehouse": f"s3://{LAKEHOUSE_BUCKET}",
        "s3.endpoint": MINIO_ENDPOINT,
        "s3.access-key-id": MINIO_ACCESS_KEY,
        "s3.secret-access-key": MINIO_SECRET_KEY,
        "s3.path-style-access": "true",
    })

    ns = "preflight_test"
    table_id = (ns, "smoke_test")

    if not catalog.namespace_exists(ns):
        catalog.create_namespace(ns)

    schema = Schema(
        NestedField(1, "id", LongType()),       # SIN required=True — PyArrow siempre nullable
        NestedField(2, "label", StringType()),
    )

    if catalog.table_exists(table_id):
        catalog.drop_table(table_id)

    table = catalog.create_table(table_id, schema=schema)

    arrow_table = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    table.append(arrow_table)

    result = table.scan().to_arrow()
    assert len(result) == 3, f"Esperaba 3 filas, leí {len(result)}"

    catalog.drop_table(table_id)
    catalog.drop_namespace(ns)


# ── 3. Dremio ─────────────────────────────────────────────────────────────────

@check("Dremio API v3 alcanzable")
def check_dremio_api():
    # login
    r = requests.post(f"{DREMIO_HOST}/apiv2/login",
                      json={"userName": DREMIO_USER, "password": DREMIO_PASS},
                      timeout=15)
    assert r.status_code == 200, f"Login falló HTTP {r.status_code}: {r.text[:200]}"
    token = r.json().get("token", "")
    assert token, "Token vacío tras login"

    headers = {"Authorization": f"_dremio{token}"}
    r2 = requests.get(f"{DREMIO_HOST}/api/v3/catalog", headers=headers, timeout=10)
    assert r2.status_code == 200, f"GET /api/v3/catalog HTTP {r2.status_code}"


# ── 4. Metabase ───────────────────────────────────────────────────────────────

@check("Metabase API alcanzable y login correcto")
def check_metabase_api():
    r = requests.post(f"{METABASE_HOST}/api/session",
                      json={"username": METABASE_USER, "password": METABASE_PASS},
                      timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}"
    token = r.json().get("id", "")
    assert token, "Token de sesión vacío"


# ── 5. PostgreSQL ─────────────────────────────────────────────────────────────

@check("PostgreSQL universidad_analytics accesible")
def check_postgres_db():
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS, connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    conn.close()


@check("Usuario 'analytics' tiene permisos sobre tablas")
def check_analytics_table_perms():
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user="analytics", password=ANALYTICS_PASS,
        connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'")
    count = cur.fetchone()[0]
    conn.close()
    # Si hay tablas, verificar que puede hacer SELECT
    if count > 0:
        conn2 = psycopg2.connect(
            host=PG_HOST, dbname=PG_DB, user="analytics", password=ANALYTICS_PASS,
            connect_timeout=10
        )
        cur2 = conn2.cursor()
        cur2.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' LIMIT 1")
        tbl = cur2.fetchone()
        if tbl:
            cur2.execute(f"SELECT 1 FROM {tbl[0]} LIMIT 1")
        conn2.close()


@check("Usuario 'analytics' tiene permisos sobre secuencias (crítico para INSERT con SERIAL)")
def check_analytics_sequence_perms():
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user="analytics", password=ANALYTICS_PASS,
        connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
        LIMIT 1
    """)
    seq = cur.fetchone()
    if seq:
        cur.execute(f"SELECT nextval('{seq[0]}')")
        cur.execute(f"SELECT setval('{seq[0]}', (SELECT last_value FROM {seq[0]}) - 1)")
    conn.close()


# ── Ejecutar todos los checks ─────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║         Pre-flight Check — Data Lakehouse           ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    checks = [
        check_minio_health,
        check_minio_bucket,
        check_nessie_api,
        check_nessie_iceberg_rest,
        check_pyiceberg_roundtrip,
        check_dremio_api,
        check_metabase_api,
        check_postgres_db,
        check_analytics_table_perms,
        check_analytics_sequence_perms,
    ]

    for fn in checks:
        fn()

    print()
    passed = sum(1 for r in results if r[0] == "OK")
    failed = sum(1 for r in results if r[0] == "FAIL")

    if failed == 0:
        print(f"  \033[32m✅ Todos los checks pasaron ({passed}/{passed}). Listo para desarrollar.\033[0m")
        sys.exit(0)
    else:
        print(f"  \033[31m❌ {failed} check(s) fallaron — resolver antes de continuar.\033[0m")
        print()
        print("  Checks fallidos:")
        for status, name, msg in results:
            if status == "FAIL":
                print(f"    • {name}: {msg}")
        print()
        print("  Consultar: docs/TROUBLESHOOTING.md y docs/STACK.md")
        sys.exit(1)
