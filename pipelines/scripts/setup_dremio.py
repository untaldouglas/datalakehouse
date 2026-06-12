#!/usr/bin/env python3
"""
Configures Dremio OSS via REST API:
  1. Creates admin user on first run
  2. Adds Nessie catalog source
  3. Creates initial virtual datasets (views)
"""

import os
import time
import logging
import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DREMIO_HOST = os.environ.get("DREMIO_HOST", "http://localhost:9047")
DREMIO_USER = os.environ.get("DREMIO_ADMIN_USER", "admin")
DREMIO_PASS = os.environ.get("DREMIO_ADMIN_PASSWORD", "Admin1234!")
DREMIO_EMAIL = os.environ.get("DREMIO_ADMIN_EMAIL", "admin@universidad.edu")
NESSIE_URI  = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")


def wait_for_dremio(max_retries=30, delay=10):
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{DREMIO_HOST}/apiv2/server_status", timeout=10)
            if r.status_code == 200:
                log.info("Dremio is up!")
                return True
        except Exception as e:
            log.info(f"Waiting for Dremio ({attempt+1}/{max_retries}): {e}")
        time.sleep(delay)
    raise Exception("Dremio never became available")


def first_run_setup():
    """Bootstrap Dremio admin user (only needed on first launch)."""
    r = requests.put(f"{DREMIO_HOST}/apiv2/bootstrap/firstuser", json={
        "userName": DREMIO_USER,
        "firstName": "Admin",
        "lastName": "Universidad",
        "email": DREMIO_EMAIL,
        "createdAt": int(time.time() * 1000),
        "password": DREMIO_PASS,
    }, timeout=30)
    if r.status_code in (200, 204):
        log.info("Admin user created")
        return True
    log.info(f"First-run setup response: {r.status_code} — may already exist")
    return False


def get_token():
    r = requests.post(f"{DREMIO_HOST}/apiv2/login", json={
        "userName": DREMIO_USER,
        "password": DREMIO_PASS,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["token"]


def api(method, path, token, **kwargs):
    headers = {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}
    r = requests.request(method, f"{DREMIO_HOST}{path}", headers=headers, timeout=30, **kwargs)
    return r


def add_nessie_source(token):
    payload = {
        "entityType": "source",
        "name": "lakehouse",
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": f"{NESSIE_URI}",
            "nessieAuthType": "NONE",
            "awsAccessKey": MINIO_ACCESS_KEY,
            "awsAccessSecret": MINIO_SECRET_KEY,
            "awsRootPath": f"/{LAKEHOUSE_BUCKET}",
            "propertyList": [
                {"name": "fs.s3a.path.style.access",   "value": "true"},
                {"name": "fs.s3a.endpoint",            "value": MINIO_ENDPOINT.replace("http://", "")},
                {"name": "dremio.s3.compat",           "value": "true"},
            ],
            "secure": False,
            "credentialType": "ACCESS_KEY",
        },
    }
    r = api("POST", "/apiv2/source/", token, json=payload)
    if r.status_code in (200, 201):
        log.info("Nessie source 'lakehouse' created in Dremio")
    elif r.status_code == 409:
        log.info("Nessie source 'lakehouse' already exists")
    else:
        log.warning(f"Nessie source creation: {r.status_code} {r.text}")


def create_space(token, space_name: str):
    r = api("POST", "/apiv2/space/", token, json={"entityType": "space", "name": space_name})
    if r.status_code in (200, 201):
        log.info(f"Space '{space_name}' created")
    elif r.status_code == 409:
        log.info(f"Space '{space_name}' already exists")


def create_vds(token, space: str, name: str, sql: str):
    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": [space, name],
        "sql": sql,
    }
    r = api("POST", "/apiv2/dataset/", token, json=payload)
    if r.status_code in (200, 201):
        log.info(f"VDS '{space}.{name}' created")
    else:
        log.warning(f"VDS '{space}.{name}': {r.status_code} {r.text[:200]}")


def main():
    wait_for_dremio()
    first_run_setup()
    time.sleep(5)
    token = get_token()

    add_nessie_source(token)
    create_space(token, "analytics")

    create_vds(token, "analytics", "students", """
        SELECT
            s.name AS student_code,
            s.student_name AS full_name,
            s.gender,
            s.joining_date,
            s.program AS program_code,
            p.program_name,
            s.academic_year,
            s.enabled AS active
        FROM lakehouse.bronze.erpnext_students s
        LEFT JOIN lakehouse.bronze.erpnext_programs p ON s.program = p.name
    """)

    create_vds(token, "analytics", "financial_summary", """
        SELECT
            f.program AS program_code,
            f.academic_year,
            f.academic_term,
            f.status,
            COUNT(*) AS num_fees,
            SUM(f.grand_total) AS total_facturado,
            SUM(f.paid_amount) AS total_cobrado,
            SUM(f.outstanding_amount) AS total_pendiente,
            AVG(f.outstanding_amount) AS avg_pendiente,
            SUM(CASE WHEN f.outstanding_amount > 0 THEN 1 ELSE 0 END) AS morosos
        FROM lakehouse.bronze.erpnext_fees f
        GROUP BY f.program, f.academic_year, f.academic_term, f.status
    """)

    create_vds(token, "analytics", "grade_summary", """
        SELECT
            g.course_code,
            SPLIT_PART(g.course_code, '-', 1) AS program_code,
            COUNT(*) AS num_grades,
            AVG(g.grade) AS avg_grade,
            AVG(g.grade_pct) AS avg_grade_pct,
            SUM(g.passed) AS num_passed,
            COUNT(*) - SUM(g.passed) AS num_failed,
            ROUND(SUM(g.passed) * 100.0 / COUNT(*), 1) AS pass_rate_pct
        FROM lakehouse.silver.grades g
        GROUP BY g.course_code
    """)

    log.info("Dremio setup complete")


if __name__ == "__main__":
    main()
