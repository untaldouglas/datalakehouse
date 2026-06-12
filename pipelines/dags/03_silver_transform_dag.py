"""
DAG: Bronze → Silver Layer
Reads from Iceberg (Nessie), cleans/normalizes, writes Silver Iceberg tables.
Runs after both bronze DAGs complete.
"""

import os
import logging
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.compute as pc

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from common.lakehouse import get_catalog, ensure_namespace
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, BooleanType, IntegerType,
)

log = logging.getLogger(__name__)

SCHEDULE = os.environ.get("ETL_SCHEDULE_INTERVAL", "0 0,12 * * *")

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

SCHEMA_SILVER_STUDENTS = Schema(
    NestedField(1,  "student_code",   StringType()),
    NestedField(2,  "full_name",      StringType()),
    NestedField(3,  "gender",         StringType()),
    NestedField(4,  "date_of_birth",  StringType()),
    NestedField(5,  "joining_date",   StringType()),
    NestedField(6,  "program_code",   StringType()),
    NestedField(7,  "academic_year",  StringType()),
    NestedField(8,  "active",         LongType()),
    NestedField(9,  "_updated_at",    StringType()),
)

SCHEMA_SILVER_FEES = Schema(
    NestedField(1,  "fee_id",          StringType()),
    NestedField(2,  "student_code",    StringType()),
    NestedField(3,  "program_code",    StringType()),
    NestedField(4,  "academic_year",   StringType()),
    NestedField(5,  "academic_term",   StringType()),
    NestedField(6,  "due_date",        StringType()),
    NestedField(7,  "posting_date",    StringType()),
    NestedField(8,  "total_amount",    DoubleType()),
    NestedField(9,  "paid_amount",     DoubleType()),
    NestedField(10, "pending_amount",  DoubleType()),
    NestedField(11, "status",          StringType()),
    NestedField(12, "is_overdue",      LongType()),
    NestedField(13, "days_overdue",    LongType()),
    NestedField(14, "_updated_at",     StringType()),
)

SCHEMA_SILVER_PAYMENTS = Schema(
    NestedField(1,  "payment_id",      StringType()),
    NestedField(2,  "student_code",    StringType()),
    NestedField(3,  "payment_date",    StringType()),
    NestedField(4,  "amount",          DoubleType()),
    NestedField(5,  "payment_mode",    StringType()),
    NestedField(6,  "reference",       StringType()),
    NestedField(7,  "year",            LongType()),
    NestedField(8,  "month",           LongType()),
    NestedField(9,  "_updated_at",     StringType()),
)

SCHEMA_SILVER_GRADES = Schema(
    NestedField(1,  "grade_id",        StringType()),
    NestedField(2,  "student_code",    StringType()),
    NestedField(3,  "course_code",     StringType()),
    NestedField(4,  "grade",           DoubleType()),
    NestedField(5,  "max_grade",       DoubleType()),
    NestedField(6,  "grade_pct",       DoubleType()),
    NestedField(7,  "passed",          LongType()),
    NestedField(8,  "grade_date",      StringType()),
    NestedField(9,  "_updated_at",     StringType()),
)


def transform_silver_students(**ctx):
    catalog = get_catalog()
    ensure_namespace(catalog, "silver")

    bronze = catalog.load_table("bronze.erpnext_students")
    df = bronze.scan().to_arrow()
    if len(df) == 0:
        log.info("No student records to transform")
        return 0

    now = datetime.utcnow().isoformat()
    silver_df = pa.table({
        "student_code":  df["name"],
        "full_name":     df["student_name"],
        "gender":        df["gender"],
        "date_of_birth": df["date_of_birth"],
        "joining_date":  df["joining_date"],
        "program_code":  df["program"],
        "academic_year": df["academic_year"],
        "active":        df["enabled"],
        "_updated_at":   pa.array([now] * len(df), pa.string()),
    })

    from common.lakehouse import upsert_iceberg
    return upsert_iceberg(catalog, "silver.students", silver_df, SCHEMA_SILVER_STUDENTS, mode="overwrite")


def transform_silver_fees(**ctx):
    from datetime import date
    catalog = get_catalog()
    ensure_namespace(catalog, "silver")

    bronze = catalog.load_table("bronze.erpnext_fees")
    df = bronze.scan().to_arrow()
    if len(df) == 0:
        return 0

    today_str = date.today().isoformat()

    def calc_days_overdue(due_date_str: str, status: str) -> int:
        if status in ("Paid",):
            return 0
        try:
            due = date.fromisoformat(str(due_date_str))
            diff = (date.today() - due).days
            return max(0, diff)
        except Exception:
            return 0

    due_dates = df["due_date"].to_pylist()
    statuses  = df["status"].to_pylist()
    days_overdue = [calc_days_overdue(d, s) for d, s in zip(due_dates, statuses)]
    is_overdue   = [1 if d > 0 else 0 for d in days_overdue]

    now = datetime.utcnow().isoformat()
    silver_df = pa.table({
        "fee_id":         df["name"],
        "student_code":   df["student"],
        "program_code":   df["program"],
        "academic_year":  df["academic_year"],
        "academic_term":  df["academic_term"],
        "due_date":       df["due_date"],
        "posting_date":   df["posting_date"],
        "total_amount":   df["grand_total"],
        "paid_amount":    df["paid_amount"],
        "pending_amount": df["outstanding_amount"],
        "status":         df["status"],
        "is_overdue":     pa.array(is_overdue, pa.int64()),
        "days_overdue":   pa.array(days_overdue, pa.int64()),
        "_updated_at":    pa.array([now] * len(df), pa.string()),
    })

    from common.lakehouse import upsert_iceberg
    return upsert_iceberg(catalog, "silver.fees", silver_df, SCHEMA_SILVER_FEES, mode="overwrite")


def transform_silver_payments(**ctx):
    catalog = get_catalog()
    ensure_namespace(catalog, "silver")

    bronze = catalog.load_table("bronze.erpnext_payments")
    df = bronze.scan().to_arrow()
    if len(df) == 0:
        return 0

    def parse_year(d):
        try: return int(str(d)[:4])
        except: return 0

    def parse_month(d):
        try: return int(str(d)[5:7])
        except: return 0

    dates = df["posting_date"].to_pylist()
    years  = [parse_year(d) for d in dates]
    months = [parse_month(d) for d in dates]

    now = datetime.utcnow().isoformat()
    silver_df = pa.table({
        "payment_id":   df["name"],
        "student_code": df["party"],
        "payment_date": df["posting_date"],
        "amount":       df["paid_amount"],
        "payment_mode": df["mode_of_payment"],
        "reference":    df["reference_no"],
        "year":         pa.array(years, pa.int64()),
        "month":        pa.array(months, pa.int64()),
        "_updated_at":  pa.array([now] * len(df), pa.string()),
    })

    from common.lakehouse import upsert_iceberg
    return upsert_iceberg(catalog, "silver.payments", silver_df, SCHEMA_SILVER_PAYMENTS, mode="overwrite")


def transform_silver_grades(**ctx):
    catalog = get_catalog()
    ensure_namespace(catalog, "silver")

    bronze_grades = catalog.load_table("bronze.moodle_grades")
    bronze_courses = catalog.load_table("bronze.moodle_courses")
    bronze_users   = catalog.load_table("bronze.moodle_users")

    df_grades  = bronze_grades.scan().to_arrow()
    df_courses = bronze_courses.scan().to_arrow()
    df_users   = bronze_users.scan().to_arrow()

    if len(df_grades) == 0:
        return 0

    # Build lookup maps
    course_map = dict(zip(df_courses["course_id"].to_pylist(), df_courses["shortname"].to_pylist()))
    user_map   = dict(zip(df_users["user_id"].to_pylist(), df_users["username"].to_pylist()))

    item_ids    = df_grades["itemid"].to_pylist()
    user_ids    = df_grades["userid"].to_pylist()
    raw_grades  = df_grades["rawgrade"].to_pylist()
    raw_maxs    = df_grades["rawgrademax"].to_pylist()

    student_codes = [user_map.get(uid, f"uid-{uid}") for uid in user_ids]
    course_codes  = [course_map.get(iid, f"item-{iid}") for iid in item_ids]
    grade_pcts    = [round((g / m * 100) if m and m > 0 else 0, 2) for g, m in zip(raw_grades, raw_maxs)]
    passed        = [1 if (g / m >= 0.6 if m and m > 0 else False) else 0 for g, m in zip(raw_grades, raw_maxs)]

    now = datetime.utcnow().isoformat()
    grade_ids = [f"GR-{i:08d}" for i in df_grades["grade_id"].to_pylist()]

    silver_df = pa.table({
        "grade_id":     pa.array(grade_ids, pa.string()),
        "student_code": pa.array(student_codes, pa.string()),
        "course_code":  pa.array(course_codes, pa.string()),
        "grade":        df_grades["rawgrade"],
        "max_grade":    df_grades["rawgrademax"],
        "grade_pct":    pa.array(grade_pcts, pa.float64()),
        "passed":       pa.array(passed, pa.int64()),
        "grade_date":   pa.array([now[:10]] * len(df_grades), pa.string()),
        "_updated_at":  pa.array([now] * len(df_grades), pa.string()),
    })

    from common.lakehouse import upsert_iceberg
    return upsert_iceberg(catalog, "silver.grades", silver_df, SCHEMA_SILVER_GRADES, mode="overwrite")


with DAG(
    dag_id="03_bronze_to_silver",
    description="Transform Bronze Iceberg data to cleaned Silver layer",
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["silver", "transform"],
    max_active_runs=1,
) as dag:

    def _latest_success(dag_id):
        from airflow.models import DagRun
        from airflow import settings
        session = settings.Session()
        run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == "success",
        ).order_by(DagRun.execution_date.desc()).first()
        return run.execution_date if run else None

    def _moodle_date(dt, **kwargs):
        return _latest_success("01_moodle_to_bronze") or dt

    def _erpnext_date(dt, **kwargs):
        return _latest_success("02_erpnext_to_bronze") or dt

    wait_moodle = ExternalTaskSensor(
        task_id="wait_moodle_bronze",
        external_dag_id="01_moodle_to_bronze",
        execution_date_fn=_moodle_date,
        mode="reschedule",
        timeout=3600,
        poke_interval=30,
    )
    wait_erpnext = ExternalTaskSensor(
        task_id="wait_erpnext_bronze",
        external_dag_id="02_erpnext_to_bronze",
        execution_date_fn=_erpnext_date,
        mode="reschedule",
        timeout=3600,
        poke_interval=30,
    )

    t_students = PythonOperator(task_id="transform_students", python_callable=transform_silver_students)
    t_fees     = PythonOperator(task_id="transform_fees",     python_callable=transform_silver_fees)
    t_payments = PythonOperator(task_id="transform_payments", python_callable=transform_silver_payments)
    t_grades   = PythonOperator(task_id="transform_grades",   python_callable=transform_silver_grades)

    [wait_moodle, wait_erpnext] >> t_students >> [t_fees, t_payments, t_grades]
