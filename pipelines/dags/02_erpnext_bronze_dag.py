"""
DAG: ERPNext → Bronze Layer (Iceberg/Nessie/MinIO)
Schedule: Every 12 hours
Extracts: students, programs, fees, payments, courses
"""

import os
import logging
from datetime import datetime, timedelta
import pyarrow as pa
import pymysql

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.lakehouse import (
    get_catalog, upsert_iceberg,
    SCHEMA_ERPNEXT_STUDENTS, SCHEMA_ERPNEXT_FEES, SCHEMA_ERPNEXT_PAYMENTS,
)

log = logging.getLogger(__name__)

SCHEDULE = os.environ.get("ETL_SCHEDULE_INTERVAL", "0 0,12 * * *")
LOOKBACK_HOURS = int(os.environ.get("ETL_LOOKBACK_HOURS", 13))

ERPNEXT_CONN = dict(
    host=os.environ.get("ERPNEXT_DB_HOST", "erpnext-db"),
    port=int(os.environ.get("ERPNEXT_DB_PORT", 3306)),
    user=os.environ.get("ERPNEXT_DB_USER", "erpnext"),
    password=os.environ.get("ERPNEXT_DB_PASSWORD", ""),
    database=os.environ.get("ERPNEXT_DB_NAME", "erpnext_universidad"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _get_erp_conn():
    return pymysql.connect(**ERPNEXT_CONN)


def _etl_ts():
    return datetime.utcnow().isoformat()


def _since_dt():
    return (datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%d %H:%M:%S")


def extract_students(**ctx):
    since = _since_dt()
    conn = _get_erp_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT name, student_name, first_name, last_name, gender,
               date_of_birth, joining_date, program, academic_year, enabled, modified
        FROM `tabStudent`
        WHERE modified >= %s
        LIMIT 10000
    """, (since,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    def s(r, k): return str(r[k]) if r[k] is not None else ""

    table = pa.table({
        "name":          pa.array([r["name"] for r in rows], pa.string()),
        "student_name":  pa.array([s(r, "student_name") for r in rows], pa.string()),
        "first_name":    pa.array([s(r, "first_name") for r in rows], pa.string()),
        "last_name":     pa.array([s(r, "last_name") for r in rows], pa.string()),
        "gender":        pa.array([s(r, "gender") for r in rows], pa.string()),
        "date_of_birth": pa.array([s(r, "date_of_birth") for r in rows], pa.string()),
        "joining_date":  pa.array([s(r, "joining_date") for r in rows], pa.string()),
        "program":       pa.array([s(r, "program") for r in rows], pa.string()),
        "academic_year": pa.array([s(r, "academic_year") for r in rows], pa.string()),
        "enabled":       pa.array([int(r["enabled"] or 0) for r in rows], pa.int64()),
        "modified":      pa.array([s(r, "modified") for r in rows], pa.string()),
        "_etl_loaded_at": pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.erpnext_students", table, SCHEMA_ERPNEXT_STUDENTS)


def extract_programs(**ctx):
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, LongType

    conn = _get_erp_conn()
    cur = conn.cursor()
    cur.execute("SELECT name, program_name, program_abbreviation, department, duration FROM `tabProgram`")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    schema = Schema(
        NestedField(1, "name",         StringType()),
        NestedField(2, "program_name", StringType()),
        NestedField(3, "abbreviation", StringType()),
        NestedField(4, "department",   StringType()),
        NestedField(5, "duration",     LongType()),
        NestedField(6, "_etl_loaded_at", StringType()),
    )
    table = pa.table({
        "name":          pa.array([r["name"] for r in rows], pa.string()),
        "program_name":  pa.array([r["program_name"] or "" for r in rows], pa.string()),
        "abbreviation":  pa.array([r["program_abbreviation"] or "" for r in rows], pa.string()),
        "department":    pa.array([r["department"] or "" for r in rows], pa.string()),
        "duration":      pa.array([int(r["duration"] or 4) for r in rows], pa.int64()),
        "_etl_loaded_at": pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.erpnext_programs", table, schema, mode="overwrite")


def extract_fees(**ctx):
    since = _since_dt()
    conn = _get_erp_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT name, student, student_name, program, academic_year, academic_term,
               due_date, posting_date, grand_total, paid_amount, outstanding_amount, status, modified
        FROM `tabFees`
        WHERE modified >= %s AND docstatus = 1
        LIMIT 200000
    """, (since,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    def s(r, k): return str(r[k]) if r[k] is not None else ""
    def f(r, k): return float(r[k]) if r[k] is not None else 0.0

    table = pa.table({
        "name":              pa.array([r["name"] for r in rows], pa.string()),
        "student":           pa.array([s(r, "student") for r in rows], pa.string()),
        "student_name":      pa.array([s(r, "student_name") for r in rows], pa.string()),
        "program":           pa.array([s(r, "program") for r in rows], pa.string()),
        "academic_year":     pa.array([s(r, "academic_year") for r in rows], pa.string()),
        "academic_term":     pa.array([s(r, "academic_term") for r in rows], pa.string()),
        "due_date":          pa.array([s(r, "due_date") for r in rows], pa.string()),
        "posting_date":      pa.array([s(r, "posting_date") for r in rows], pa.string()),
        "grand_total":       pa.array([f(r, "grand_total") for r in rows], pa.float64()),
        "paid_amount":       pa.array([f(r, "paid_amount") for r in rows], pa.float64()),
        "outstanding_amount": pa.array([f(r, "outstanding_amount") for r in rows], pa.float64()),
        "status":            pa.array([s(r, "status") for r in rows], pa.string()),
        "modified":          pa.array([s(r, "modified") for r in rows], pa.string()),
        "_etl_loaded_at":    pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.erpnext_fees", table, SCHEMA_ERPNEXT_FEES)


def extract_payments(**ctx):
    since = _since_dt()
    conn = _get_erp_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT name, party, party_name, posting_date, paid_amount,
               received_amount, reference_no, mode_of_payment, modified
        FROM `tabPayment Entry`
        WHERE modified >= %s AND docstatus = 1 AND party_type = 'Student'
        LIMIT 200000
    """, (since,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    def s(r, k): return str(r[k]) if r[k] is not None else ""
    def f(r, k): return float(r[k]) if r[k] is not None else 0.0

    table = pa.table({
        "name":            pa.array([r["name"] for r in rows], pa.string()),
        "party":           pa.array([s(r, "party") for r in rows], pa.string()),
        "party_name":      pa.array([s(r, "party_name") for r in rows], pa.string()),
        "posting_date":    pa.array([s(r, "posting_date") for r in rows], pa.string()),
        "paid_amount":     pa.array([f(r, "paid_amount") for r in rows], pa.float64()),
        "received_amount": pa.array([f(r, "received_amount") for r in rows], pa.float64()),
        "reference_no":    pa.array([s(r, "reference_no") for r in rows], pa.string()),
        "mode_of_payment": pa.array([s(r, "mode_of_payment") for r in rows], pa.string()),
        "modified":        pa.array([s(r, "modified") for r in rows], pa.string()),
        "_etl_loaded_at":  pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.erpnext_payments", table, SCHEMA_ERPNEXT_PAYMENTS)


with DAG(
    dag_id="02_erpnext_to_bronze",
    description="Extract ERPNext transactional data to Bronze Iceberg layer",
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "erpnext", "etl"],
    max_active_runs=1,
) as dag:

    t_programs = PythonOperator(task_id="extract_programs", python_callable=extract_programs)
    t_students = PythonOperator(task_id="extract_students", python_callable=extract_students)
    t_fees     = PythonOperator(task_id="extract_fees",     python_callable=extract_fees)
    t_payments = PythonOperator(task_id="extract_payments", python_callable=extract_payments)

    t_programs >> t_students >> [t_fees, t_payments]
