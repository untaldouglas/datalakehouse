"""
DAG: Silver → Gold Layer + Semantic PostgreSQL
Reads Silver Iceberg tables, computes KPIs, writes:
  1. Gold Iceberg tables (for Dremio ad-hoc queries)
  2. PostgreSQL semantic layer (for Metabase dashboards)
Schedule: Every 12 hours, after Silver transform completes
"""

import os
import logging
from datetime import datetime, timedelta, date
from collections import defaultdict
import pyarrow as pa
import psycopg2
import psycopg2.extras

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from common.lakehouse import get_catalog, ensure_namespace

log = logging.getLogger(__name__)

SCHEDULE = os.environ.get("ETL_SCHEDULE_INTERVAL", "0 0,12 * * *")

SEMANTIC_CONN = dict(
    host=os.environ.get("SEMANTIC_DB_HOST", "metabase-db"),
    port=int(os.environ.get("SEMANTIC_DB_PORT", 5432)),
    dbname=os.environ.get("SEMANTIC_DB_NAME", "universidad_analytics"),
    user=os.environ.get("SEMANTIC_DB_USER", "analytics"),
    password=os.environ.get("SEMANTIC_DB_PASSWORD", ""),
)

PROGRAMS = {
    "MED": "Medicina",
    "INF": "Informática",
    "GN":  "Gestión de Negocios",
}

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _get_pg():
    return psycopg2.connect(**SEMANTIC_CONN)


def _ensure_semantic_db():
    """Make sure the semantic database and schema exist."""
    try:
        conn = _get_pg()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
    except Exception as e:
        log.error(f"Semantic DB not available: {e}")
        raise


# ============================================================
# GOLD: Financial KPIs
# ============================================================

def materialize_financial_kpis(**ctx):
    _ensure_semantic_db()
    catalog = get_catalog()
    ensure_namespace(catalog, "gold")

    silver_fees = catalog.load_table("silver.fees").scan().to_arrow()
    silver_payments = catalog.load_table("silver.payments").scan().to_arrow()

    if len(silver_fees) == 0:
        log.info("No fee data to aggregate")
        return

    # Convert to Python lists for aggregation
    fees_data = {
        "student":    silver_fees["student_code"].to_pylist(),
        "program":    silver_fees["program_code"].to_pylist(),
        "year":       [int(d[:4]) if d and len(d) >= 4 else 0 for d in silver_fees["posting_date"].to_pylist()],
        "month":      [int(d[5:7]) if d and len(d) >= 7 else 0 for d in silver_fees["posting_date"].to_pylist()],
        "acad_year":  silver_fees["academic_year"].to_pylist(),
        "acad_term":  silver_fees["academic_term"].to_pylist(),
        "total":      silver_fees["total_amount"].to_pylist(),
        "paid":       silver_fees["paid_amount"].to_pylist(),
        "pending":    silver_fees["pending_amount"].to_pylist(),
        "status":     silver_fees["status"].to_pylist(),
        "is_overdue": silver_fees["is_overdue"].to_pylist(),
        "fee_id":     silver_fees["fee_id"].to_pylist(),
        "due_date":   silver_fees["due_date"].to_pylist(),
    }

    # Aggregate by program, year, month, academic period
    kpis = defaultdict(lambda: {
        "facturado": 0.0, "cobrado": 0.0, "pendiente": 0.0,
        "alumnos": set(), "morosos": set(),
    })

    n = len(fees_data["student"])
    for i in range(n):
        key = (
            fees_data["program"][i] or "UNK",
            fees_data["year"][i],
            fees_data["month"][i],
            fees_data["acad_year"][i] or "",
            fees_data["acad_term"][i] or "",
        )
        kpis[key]["facturado"] += fees_data["total"][i] or 0
        kpis[key]["cobrado"]   += fees_data["paid"][i] or 0
        kpis[key]["pendiente"] += fees_data["pending"][i] or 0
        kpis[key]["alumnos"].add(fees_data["student"][i])
        if fees_data["is_overdue"][i]:
            kpis[key]["morosos"].add(fees_data["student"][i])

    # Write to PostgreSQL semantic layer
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE kpi_financiero_mensual")

    rows = []
    now = datetime.utcnow().isoformat()
    for (prog, yr, mo, acad_yr, acad_term), v in kpis.items():
        facturado  = round(v["facturado"], 2)
        cobrado    = round(v["cobrado"], 2)
        pendiente  = round(v["pendiente"], 2)
        n_alumnos  = len(v["alumnos"])
        n_morosos  = len(v["morosos"])
        tasa_cobr  = round(cobrado / facturado * 100, 2) if facturado > 0 else 0
        tasa_mora  = round(n_morosos / n_alumnos * 100, 2) if n_alumnos > 0 else 0
        ingr_prom  = round(cobrado / n_alumnos, 2) if n_alumnos > 0 else 0

        rows.append((
            yr, mo, prog, acad_yr, acad_term,
            facturado, cobrado, pendiente,
            n_alumnos, n_morosos, 0,  # nuevas_matriculas set separately
            tasa_cobr, tasa_mora, ingr_prom, now,
        ))

    psycopg2.extras.execute_values(cur, """
        INSERT INTO kpi_financiero_mensual
          (anio, mes, programa_codigo, anio_academico, ciclo_academico,
           ingresos_facturados, ingresos_cobrados, ingresos_pendientes,
           alumnos_activos, alumnos_morosos, nuevas_matriculas,
           tasa_cobranza, tasa_morosidad, ingreso_promedio_alumno, updated_at)
        VALUES %s
        ON CONFLICT (anio, mes, programa_codigo, anio_academico, ciclo_academico)
        DO UPDATE SET
          ingresos_facturados = EXCLUDED.ingresos_facturados,
          ingresos_cobrados   = EXCLUDED.ingresos_cobrados,
          ingresos_pendientes = EXCLUDED.ingresos_pendientes,
          alumnos_activos     = EXCLUDED.alumnos_activos,
          alumnos_morosos     = EXCLUDED.alumnos_morosos,
          tasa_cobranza       = EXCLUDED.tasa_cobranza,
          tasa_morosidad      = EXCLUDED.tasa_morosidad,
          ingreso_promedio_alumno = EXCLUDED.ingreso_promedio_alumno,
          updated_at          = EXCLUDED.updated_at
    """, rows)

    # Also populate fact_ingresos_matricula from silver fees (sample)
    cur.execute("TRUNCATE TABLE fact_ingresos_matricula")
    fee_rows = []
    for i in range(min(len(fees_data["student"]), 50000)):
        fee_rows.append((
            fees_data["student"][i],
            fees_data["program"][i] or "UNK",
            fees_data["acad_year"][i] or "",
            fees_data["acad_term"][i] or "",
            "Colegiatura" if "FEES" in str(fees_data["fee_id"][i]) else "Matrícula",
            fees_data["total"][i] or 0,
            fees_data["paid"][i] or 0,
            fees_data["pending"][i] or 0,
            "Efectivo",
            fees_data["status"][i] or "Unpaid",
            0,
            fees_data["due_date"][i],
            now,
        ))

    psycopg2.extras.execute_values(cur, """
        INSERT INTO fact_ingresos_matricula
          (alumno_codigo, programa_codigo, anio_academico, ciclo_academico,
           categoria_cobro, monto_facturado, monto_pagado, monto_pendiente,
           modo_pago, estado_cobro, dias_mora, fecha_vencimiento, updated_at)
        VALUES %s
    """, fee_rows[:50000])

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Financial KPIs materialized: {len(rows)} period-program combinations")


# ============================================================
# GOLD: Academic KPIs
# ============================================================

def materialize_academic_kpis(**ctx):
    _ensure_semantic_db()
    catalog = get_catalog()

    silver_grades   = catalog.load_table("silver.grades").scan().to_arrow()
    silver_students = catalog.load_table("silver.students").scan().to_arrow()

    if len(silver_grades) == 0:
        log.info("No grade data to aggregate")
        return

    # Student → program map
    student_program = dict(zip(
        silver_students["student_code"].to_pylist(),
        silver_students["program_code"].to_pylist(),
    ))

    grades_data = {
        "student":  silver_grades["student_code"].to_pylist(),
        "course":   silver_grades["course_code"].to_pylist(),
        "grade":    silver_grades["grade"].to_pylist(),
        "passed":   silver_grades["passed"].to_pylist(),
        "grade_pct": silver_grades["grade_pct"].to_pylist(),
    }

    kpis = defaultdict(lambda: {
        "grades": [], "passed": 0, "total": 0, "students": set()
    })

    n = len(grades_data["student"])
    for i in range(n):
        student = grades_data["student"][i]
        course  = grades_data["course"][i] or ""
        prog    = student_program.get(student, "UNK")
        prog_part = course.split("-")[0] if "-" in course else prog

        key = ("2024-2025", "Ciclo II", prog_part, course)
        kpis[key]["grades"].append(grades_data["grade_pct"][i] or 0)
        kpis[key]["passed"] += int(grades_data["passed"][i] or 0)
        kpis[key]["total"]  += 1
        kpis[key]["students"].add(student)

    conn = _get_pg()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE kpi_academico_periodo")

    rows = []
    now = datetime.utcnow().isoformat()
    for (acad_yr, acad_term, prog, course), v in kpis.items():
        if not v["grades"]:
            continue
        avg_grade = round(sum(v["grades"]) / len(v["grades"]), 2)
        tasa_apro = round(v["passed"] / v["total"] * 100, 2) if v["total"] > 0 else 0
        tasa_repr = round(100 - tasa_apro, 2)

        rows.append((
            acad_yr, acad_term, prog, course,
            len(v["students"]),
            avg_grade, tasa_apro, tasa_repr, 0.0, 0.0, 0.0,
            now,
        ))

    psycopg2.extras.execute_values(cur, """
        INSERT INTO kpi_academico_periodo
          (anio_academico, ciclo_academico, programa_codigo, curso_codigo,
           alumnos_matriculados, promedio_notas, tasa_aprobacion, tasa_reprobacion,
           tasa_desercion, promedio_asistencia, actividades_completadas_pct, updated_at)
        VALUES %s
        ON CONFLICT (anio_academico, ciclo_academico, programa_codigo, curso_codigo)
        DO UPDATE SET
          alumnos_matriculados = EXCLUDED.alumnos_matriculados,
          promedio_notas      = EXCLUDED.promedio_notas,
          tasa_aprobacion     = EXCLUDED.tasa_aprobacion,
          tasa_reprobacion    = EXCLUDED.tasa_reprobacion,
          updated_at          = EXCLUDED.updated_at
    """, rows)

    # Populate fact_calificaciones
    cur.execute("TRUNCATE TABLE fact_calificaciones")
    grade_rows = []
    for i in range(min(n, 50000)):
        student = grades_data["student"][i]
        course  = grades_data["course"][i] or ""
        prog    = student_program.get(student, "UNK")
        grade   = grades_data["grade"][i] or 0
        grade_pct = grades_data["grade_pct"][i] or 0

        grade_rows.append((
            student, course, prog,
            "2024-2025", "Ciclo II",
            round(grade, 2), 10.0,
            bool(grades_data["passed"][i]),
            date.today().isoformat(), "Final", 1,
        ))

    psycopg2.extras.execute_values(cur, """
        INSERT INTO fact_calificaciones
          (alumno_codigo, curso_codigo, programa_codigo, anio_academico, ciclo_academico,
           nota_final, nota_maxima, aprobado, fecha_evaluacion, tipo_evaluacion, intentos)
        VALUES %s
    """, grade_rows)

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Academic KPIs materialized: {len(rows)} course-period combinations")


def materialize_student_dimensions(**ctx):
    _ensure_semantic_db()
    catalog = get_catalog()
    silver_students = catalog.load_table("silver.students").scan().to_arrow()

    if len(silver_students) == 0:
        return

    conn = _get_pg()
    cur = conn.cursor()

    students = [
        (
            silver_students["student_code"][i].as_py(),
            silver_students["full_name"][i].as_py() or "",
            silver_students["gender"][i].as_py() or "",
            silver_students["date_of_birth"][i].as_py() or None,
            silver_students["joining_date"][i].as_py() or None,
            silver_students["program_code"][i].as_py() or "",
            silver_students["academic_year"][i].as_py() or "",
            "Activo" if silver_students["active"][i].as_py() else "Inactivo",
        )
        for i in range(len(silver_students))
    ]

    psycopg2.extras.execute_values(cur, """
        INSERT INTO dim_alumno
          (alumno_codigo, nombre_completo, genero, fecha_nacimiento,
           fecha_ingreso, programa_codigo, anio_academico, estado)
        VALUES %s
        ON CONFLICT (alumno_codigo) DO UPDATE SET
          nombre_completo = EXCLUDED.nombre_completo,
          estado          = EXCLUDED.estado,
          updated_at      = CURRENT_TIMESTAMP
    """, students)

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Loaded {len(students)} student records to dim_alumno")


def log_etl_run(**ctx):
    _ensure_semantic_db()
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO etl_run_log (dag_id, run_date, source, layer, status)
        VALUES (%s, %s, %s, %s, %s)
    """, ("04_silver_to_gold", datetime.utcnow(), "iceberg", "gold", "success"))
    conn.commit()
    cur.close()
    conn.close()
    log.info("ETL run logged")


with DAG(
    dag_id="04_silver_to_gold",
    description="Compute Gold KPIs from Silver and materialize to PostgreSQL for Metabase",
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gold", "kpi", "metabase"],
    max_active_runs=1,
) as dag:

    def _latest_silver(dt, **kwargs):
        from airflow.models import DagRun
        from airflow import settings
        session = settings.Session()
        run = session.query(DagRun).filter(
            DagRun.dag_id == "03_bronze_to_silver",
            DagRun.state == "success",
        ).order_by(DagRun.execution_date.desc()).first()
        return run.execution_date if run else dt

    wait_silver = ExternalTaskSensor(
        task_id="wait_silver_transform",
        external_dag_id="03_bronze_to_silver",
        execution_date_fn=_latest_silver,
        mode="reschedule",
        timeout=3600,
        poke_interval=30,
    )

    t_dims     = PythonOperator(task_id="materialize_student_dims",   python_callable=materialize_student_dimensions)
    t_fin_kpi  = PythonOperator(task_id="materialize_financial_kpis", python_callable=materialize_financial_kpis)
    t_acad_kpi = PythonOperator(task_id="materialize_academic_kpis",  python_callable=materialize_academic_kpis)
    t_log      = PythonOperator(task_id="log_etl_run",                python_callable=log_etl_run)

    wait_silver >> t_dims >> [t_fin_kpi, t_acad_kpi] >> t_log
