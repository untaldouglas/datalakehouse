"""
DAG: Moodle → Bronze Layer (Iceberg/Nessie/MinIO)
Schedule: Every 12 hours
Extracts: users, courses, enrollments, grades, quiz attempts, forum posts
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
    SCHEMA_MOODLE_USERS, SCHEMA_MOODLE_COURSES,
    SCHEMA_MOODLE_GRADES, SCHEMA_MOODLE_ENROLMENTS,
)

log = logging.getLogger(__name__)

SCHEDULE = os.environ.get("ETL_SCHEDULE_INTERVAL", "0 0,12 * * *")
LOOKBACK_HOURS = int(os.environ.get("ETL_LOOKBACK_HOURS", 13))

MOODLE_CONN = dict(
    host=os.environ.get("MOODLE_DB_HOST", "moodle-db"),
    port=int(os.environ.get("MOODLE_DB_PORT", 3306)),
    user=os.environ.get("MOODLE_DB_USER", "moodle"),
    password=os.environ.get("MOODLE_DB_PASSWORD", ""),
    database=os.environ.get("MOODLE_DB_NAME", "moodle"),
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


def _get_moodle_conn():
    return pymysql.connect(**MOODLE_CONN)


def _etl_ts():
    return datetime.utcnow().isoformat()


def extract_moodle_users(**ctx):
    since_ts = int((datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)).timestamp())
    log.info(f"Extracting Moodle users modified since {since_ts}")

    conn = _get_moodle_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, username, email, firstname, lastname, confirmed,
               lang, country, timecreated, timemodified, lastlogin
        FROM mdl_user
        WHERE timemodified >= %s AND deleted = 0
        LIMIT 50000
    """, (since_ts,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        log.info("No new/modified users")
        return 0

    etl_ts = _etl_ts()
    table = pa.table({
        "user_id":       pa.array([r["id"] for r in rows], pa.int64()),
        "username":      pa.array([r["username"] for r in rows], pa.string()),
        "email":         pa.array([r["email"] for r in rows], pa.string()),
        "firstname":     pa.array([r["firstname"] for r in rows], pa.string()),
        "lastname":      pa.array([r["lastname"] for r in rows], pa.string()),
        "confirmed":     pa.array([r["confirmed"] for r in rows], pa.int64()),
        "lang":          pa.array([r["lang"] or "es" for r in rows], pa.string()),
        "country":       pa.array([r["country"] or "" for r in rows], pa.string()),
        "timecreated":   pa.array([r["timecreated"] or 0 for r in rows], pa.int64()),
        "timemodified":  pa.array([r["timemodified"] or 0 for r in rows], pa.int64()),
        "lastlogin":     pa.array([r["lastlogin"] or 0 for r in rows], pa.int64()),
        "_etl_loaded_at": pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.moodle_users", table, SCHEMA_MOODLE_USERS)


def extract_moodle_courses(**ctx):
    conn = _get_moodle_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, fullname, shortname, category, visible, startdate, enddate, timecreated, timemodified
        FROM mdl_course
        WHERE id > 1
        LIMIT 10000
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    table = pa.table({
        "course_id":    pa.array([r["id"] for r in rows], pa.int64()),
        "fullname":     pa.array([r["fullname"] or "" for r in rows], pa.string()),
        "shortname":    pa.array([r["shortname"] or "" for r in rows], pa.string()),
        "category":     pa.array([r["category"] or 0 for r in rows], pa.int64()),
        "visible":      pa.array([r["visible"] or 0 for r in rows], pa.int64()),
        "startdate":    pa.array([r["startdate"] or 0 for r in rows], pa.int64()),
        "enddate":      pa.array([r["enddate"] or 0 for r in rows], pa.int64()),
        "timecreated":  pa.array([r["timecreated"] or 0 for r in rows], pa.int64()),
        "timemodified": pa.array([r["timemodified"] or 0 for r in rows], pa.int64()),
        "_etl_loaded_at": pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.moodle_courses", table, SCHEMA_MOODLE_COURSES, mode="overwrite")


def extract_moodle_grades(**ctx):
    since_ts = int((datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)).timestamp())
    conn = _get_moodle_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT g.id, g.itemid, g.userid, g.rawgrade, g.rawgrademax, g.timecreated, g.timemodified
        FROM mdl_grade_grades g
        WHERE g.timemodified >= %s
        LIMIT 100000
    """, (since_ts,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    table = pa.table({
        "grade_id":     pa.array([r["id"] for r in rows], pa.int64()),
        "itemid":       pa.array([r["itemid"] or 0 for r in rows], pa.int64()),
        "userid":       pa.array([r["userid"] for r in rows], pa.int64()),
        "rawgrade":     pa.array([float(r["rawgrade"]) if r["rawgrade"] else 0.0 for r in rows], pa.float64()),
        "rawgrademax":  pa.array([float(r["rawgrademax"]) if r["rawgrademax"] else 10.0 for r in rows], pa.float64()),
        "timecreated":  pa.array([r["timecreated"] or 0 for r in rows], pa.int64()),
        "timemodified": pa.array([r["timemodified"] or 0 for r in rows], pa.int64()),
        "_etl_loaded_at": pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.moodle_grades", table, SCHEMA_MOODLE_GRADES)


def extract_moodle_enrolments(**ctx):
    since_ts = int((datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)).timestamp())
    conn = _get_moodle_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ue.id, ue.enrolid, ue.userid, ue.status,
               ue.timestart, ue.timeend, ue.timecreated, ue.timemodified
        FROM mdl_user_enrolments ue
        WHERE ue.timemodified >= %s
        LIMIT 200000
    """, (since_ts,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0

    etl_ts = _etl_ts()
    table = pa.table({
        "enrol_id":     pa.array([r["id"] for r in rows], pa.int64()),
        "enrolid":      pa.array([r["enrolid"] or 0 for r in rows], pa.int64()),
        "userid":       pa.array([r["userid"] for r in rows], pa.int64()),
        "status":       pa.array([r["status"] or 0 for r in rows], pa.int64()),
        "timestart":    pa.array([r["timestart"] or 0 for r in rows], pa.int64()),
        "timeend":      pa.array([r["timeend"] or 0 for r in rows], pa.int64()),
        "timecreated":  pa.array([r["timecreated"] or 0 for r in rows], pa.int64()),
        "timemodified": pa.array([r["timemodified"] or 0 for r in rows], pa.int64()),
        "_etl_loaded_at": pa.array([etl_ts] * len(rows), pa.string()),
    })

    catalog = get_catalog()
    return upsert_iceberg(catalog, "bronze.moodle_enrolments", table, SCHEMA_MOODLE_ENROLMENTS)


with DAG(
    dag_id="01_moodle_to_bronze",
    description="Extract Moodle transactional data to Bronze Iceberg layer",
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "moodle", "etl"],
    max_active_runs=1,
) as dag:

    t_users = PythonOperator(
        task_id="extract_users",
        python_callable=extract_moodle_users,
    )
    t_courses = PythonOperator(
        task_id="extract_courses",
        python_callable=extract_moodle_courses,
    )
    t_grades = PythonOperator(
        task_id="extract_grades",
        python_callable=extract_moodle_grades,
    )
    t_enrolments = PythonOperator(
        task_id="extract_enrolments",
        python_callable=extract_moodle_enrolments,
    )

    [t_users, t_courses] >> t_grades >> t_enrolments
