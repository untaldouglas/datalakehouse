#!/usr/bin/env python3
"""
Universidad Data Lakehouse - Seed Data Generator
Generates realistic test data for 5000 students across 3 programs
Populates both Moodle (MySQL) and ERPNext (MariaDB) databases
"""

import os
import random
import hashlib
import time
from datetime import datetime, date, timedelta
from decimal import Decimal
import logging

import pymysql
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker("es_MX")
random.seed(42)
Faker.seed(42)

# ============================================================
# Configuration
# ============================================================
NUM_STUDENTS = int(os.environ.get("NUM_STUDENTS", 5000))
UNIVERSITY_NAME = os.environ.get("UNIVERSITY_NAME", "Universidad Nacional Ficticia")

MOODLE_CONN = dict(
    host=os.environ.get("MOODLE_DB_HOST", "moodle-db"),
    port=int(os.environ.get("MOODLE_DB_PORT", 3306)),
    user=os.environ.get("MOODLE_DB_USER", "moodle"),
    password=os.environ.get("MOODLE_DB_PASSWORD", "moodle_secret_2024"),
    database=os.environ.get("MOODLE_DB_NAME", "moodle"),
    charset="utf8mb4",
)
ERPNEXT_CONN = dict(
    host=os.environ.get("ERPNEXT_DB_HOST", "erpnext-db"),
    port=int(os.environ.get("ERPNEXT_DB_PORT", 3306)),
    user=os.environ.get("ERPNEXT_DB_USER", "erpnext"),
    password=os.environ.get("ERPNEXT_DB_PASSWORD", "erpnext_secret_2024"),
    database=os.environ.get("ERPNEXT_DB_NAME", "erpnext_universidad"),
    charset="utf8mb4",
)

# ============================================================
# University Master Data
# ============================================================
PROGRAMS = [
    {"code": "MED",  "name": "Medicina",             "abbr": "MED", "dept": "Ciencias de la Salud",  "duration": 6, "quota": 0.25},
    {"code": "INF",  "name": "Informática",           "abbr": "INF", "dept": "Ciencias e Ingeniería", "duration": 4, "quota": 0.35},
    {"code": "GN",   "name": "Gestión de Negocios",   "abbr": "GN",  "dept": "Ciencias Económicas",   "duration": 4, "quota": 0.40},
]

ACADEMIC_YEARS = ["2021-2022", "2022-2023", "2023-2024", "2024-2025"]
ACADEMIC_TERMS = {
    "2021-2022": [
        {"name": "2021-2022/Ciclo I",  "start": date(2021, 1, 18), "end": date(2021, 6, 30)},
        {"name": "2021-2022/Ciclo II", "start": date(2021, 7, 12), "end": date(2021, 12, 15)},
    ],
    "2022-2023": [
        {"name": "2022-2023/Ciclo I",  "start": date(2022, 1, 17), "end": date(2022, 6, 30)},
        {"name": "2022-2023/Ciclo II", "start": date(2022, 7, 11), "end": date(2022, 12, 14)},
    ],
    "2023-2024": [
        {"name": "2023-2024/Ciclo I",  "start": date(2023, 1, 16), "end": date(2023, 6, 30)},
        {"name": "2023-2024/Ciclo II", "start": date(2023, 7, 10), "end": date(2023, 12, 13)},
    ],
    "2024-2025": [
        {"name": "2024-2025/Ciclo I",  "start": date(2024, 1, 15), "end": date(2024, 6, 28)},
        {"name": "2024-2025/Ciclo II", "start": date(2024, 7, 15), "end": date(2025, 1, 15)},
    ],
}

FEE_STRUCTURES = {
    "MED": {"matricula": 150.00, "colegiatura_mensual": 350.00, "seguro": 25.00},
    "INF": {"matricula": 100.00, "colegiatura_mensual": 200.00, "seguro": 15.00},
    "GN":  {"matricula": 100.00, "colegiatura_mensual": 180.00, "seguro": 15.00},
}

PAYMENT_MODES = ["Efectivo", "Transferencia", "Tarjeta", "Cheque", "Pago en Línea"]

COURSES_BY_PROGRAM = {
    "MED": [
        "Anatomía Humana I", "Anatomía Humana II", "Bioquímica", "Fisiología I", "Fisiología II",
        "Histología", "Microbiología", "Parasitología", "Patología General", "Farmacología",
        "Medicina Interna I", "Medicina Interna II", "Cirugía General", "Pediatría",
        "Ginecología y Obstetricia", "Psiquiatría", "Medicina Preventiva", "Ética Médica",
    ],
    "INF": [
        "Fundamentos de Programación", "Estructuras de Datos", "Algoritmos", "Base de Datos I",
        "Base de Datos II", "Redes de Computadoras", "Sistemas Operativos", "Ingeniería de Software",
        "Programación Web", "Inteligencia Artificial", "Seguridad Informática", "Cloud Computing",
        "Desarrollo Móvil", "Matemáticas Discretas", "Cálculo", "Estadística",
    ],
    "GN": [
        "Fundamentos de Administración", "Contabilidad I", "Contabilidad II", "Economía General",
        "Marketing I", "Marketing Digital", "Finanzas Corporativas", "Gestión de Proyectos",
        "Derecho Mercantil", "Recursos Humanos", "Logística y Cadena de Suministro",
        "Emprendimiento e Innovación", "Negocios Internacionales", "Estadística para Negocios",
        "Investigación de Mercados", "Ética Empresarial",
    ],
}

# ============================================================
# Helper Functions
# ============================================================

def wait_for_db(conn_params, max_retries=20, delay=5):
    for attempt in range(max_retries):
        try:
            conn = pymysql.connect(**conn_params)
            conn.close()
            return True
        except Exception as e:
            log.info(f"DB not ready (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(delay)
    raise Exception("Database never became available")


def md5_password(password: str) -> str:
    return hashlib.md5(password.encode()).hexdigest()


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


# ============================================================
# ERPNext Data Seeding
# ============================================================

def seed_erpnext():
    log.info("=== Seeding ERPNext (MariaDB) ===")
    wait_for_db(ERPNEXT_CONN)
    conn = pymysql.connect(**ERPNEXT_CONN)
    cur = conn.cursor()

    # Programs
    log.info("Inserting programs...")
    for p in PROGRAMS:
        cur.execute("""
            INSERT IGNORE INTO `tabProgram`
              (name, creation, modified, program_name, program_abbreviation, department, duration, duration_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'Year')
        """, (p["code"], now(), now(), p["name"], p["abbr"], p["dept"], p["duration"]))

    # Academic Years
    for yr in ACADEMIC_YEARS:
        start_y = int(yr.split("-")[0])
        cur.execute("""
            INSERT IGNORE INTO `tabAcademic Year`
              (name, creation, modified, year_start_date, year_end_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (yr, now(), now(), date(start_y, 1, 1), date(start_y + 1, 12, 31)))

    # Academic Terms
    for yr, terms in ACADEMIC_TERMS.items():
        for t in terms:
            cur.execute("""
                INSERT IGNORE INTO `tabAcademic Term`
                  (name, creation, modified, academic_year, term_name, term_start_date, term_end_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (t["name"], now(), now(), yr, t["name"].split("/")[1], t["start"], t["end"]))

    # Fee Categories
    categories = [("MATRI", "Matrícula"), ("COLEG", "Colegiatura"), ("SEGURO", "Seguro Estudiantil")]
    for cat_code, cat_name in categories:
        cur.execute("""
            INSERT IGNORE INTO `tabFee Category` (name, creation, modified, category_name)
            VALUES (%s, %s, %s, %s)
        """, (cat_code, now(), now(), cat_name))

    # Courses
    for prog_code, course_list in COURSES_BY_PROGRAM.items():
        for i, course_name in enumerate(course_list):
            course_code = f"{prog_code}-{str(i+1).zfill(3)}"
            cur.execute("""
                INSERT IGNORE INTO `tabCourse`
                  (name, creation, modified, course_name, department, course_abbreviation, credit_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (course_code, now(), now(), course_name,
                  next(p["dept"] for p in PROGRAMS if p["code"] == prog_code),
                  course_code, 3.0))

    conn.commit()

    # Generate students
    log.info(f"Generating {NUM_STUDENTS} students...")
    students = []
    for i in range(NUM_STUDENTS):
        prog = random.choices(PROGRAMS, weights=[p["quota"] for p in PROGRAMS])[0]
        year_idx = random.randint(0, len(ACADEMIC_YEARS) - 1)
        acad_year = ACADEMIC_YEARS[year_idx]
        year_num = int(acad_year.split("-")[0])

        first_name = fake.first_name()
        last_name = f"{fake.last_name()} {fake.last_name()}"
        gender = random.choice(["Male", "Female"])
        dob = fake.date_of_birth(minimum_age=17, maximum_age=30)
        join_date = random_date(date(year_num, 1, 1), date(year_num, 3, 31))
        student_code = f"STU-{str(i+1).zfill(5)}"
        email = f"{student_code.lower().replace('-', '')}@universidad.edu"

        students.append({
            "code": student_code,
            "name": f"{first_name} {last_name}",
            "first_name": first_name,
            "last_name": last_name,
            "gender": gender,
            "dob": dob,
            "join_date": join_date,
            "email": email,
            "program": prog["code"],
            "acad_year": acad_year,
        })

    # Batch insert students
    BATCH = 200
    for start in range(0, len(students), BATCH):
        batch = students[start:start + BATCH]
        cur.executemany("""
            INSERT IGNORE INTO `tabStudent`
              (name, creation, modified, docstatus, student_name, first_name, last_name,
               gender, date_of_birth, joining_date, student_email_id, program, academic_year, enabled)
            VALUES (%s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """, [(s["code"], now(), now(), s["name"], s["first_name"], s["last_name"],
               s["gender"], s["dob"], s["join_date"], s["email"], s["program"], s["acad_year"])
              for s in batch])
        conn.commit()
        log.info(f"  Inserted students {start+1}-{min(start+BATCH, len(students))}")

    # Generate fees for each student per academic term
    log.info("Generating fee records...")
    fee_rows = []
    payment_rows = []
    payment_ref_rows = []
    fee_counter = 0
    pay_counter = 0

    for s in students:
        year_idx = ACADEMIC_YEARS.index(s["acad_year"])
        prog_fees = FEE_STRUCTURES[s["program"]]

        for yr_offset in range(min(2, len(ACADEMIC_YEARS) - year_idx)):
            acad_yr = ACADEMIC_YEARS[year_idx + yr_offset]
            for term in ACADEMIC_TERMS[acad_yr]:
                # Colegiatura (monthly for 5 months per term)
                for month_offset in range(5):
                    fee_counter += 1
                    fee_name = f"FEES-{fee_counter:07d}"
                    due = term["start"] + timedelta(days=30 * month_offset)
                    total = Decimal(str(prog_fees["colegiatura_mensual"]))
                    paid_prob = random.random()

                    if paid_prob < 0.75:       # 75% fully paid
                        paid = total
                        outstanding = Decimal("0")
                        status = "Paid"
                    elif paid_prob < 0.88:     # 13% partial
                        paid = total * Decimal(str(round(random.uniform(0.3, 0.8), 2)))
                        outstanding = total - paid
                        status = "Partly Paid"
                    else:                      # 12% unpaid
                        paid = Decimal("0")
                        outstanding = total
                        status = "Unpaid"

                    fee_rows.append((
                        fee_name, now(), now(), s["code"], s["name"],
                        s["program"], acad_yr, term["name"], total,
                        paid, outstanding, status, due, term["start"],
                    ))

                    if paid > 0:
                        pay_counter += 1
                        pay_name = f"PAY-{pay_counter:07d}"
                        pay_date = random_date(term["start"], min(due + timedelta(days=30), date.today()))
                        payment_rows.append((
                            pay_name, now(), now(), s["code"], s["name"],
                            pay_date, float(paid), float(paid),
                            f"REF-{pay_counter:07d}",
                            random.choice(PAYMENT_MODES),
                        ))
                        payment_ref_rows.append((
                            f"PAYREF-{pay_counter:07d}", pay_name, fee_name, float(paid),
                        ))

                # Matrícula (once per term)
                fee_counter += 1
                fee_name = f"FEES-{fee_counter:07d}"
                total = Decimal(str(prog_fees["matricula"]))
                due = term["start"]
                paid_prob = random.random()
                if paid_prob < 0.90:
                    paid, outstanding, status = total, Decimal("0"), "Paid"
                else:
                    paid, outstanding, status = Decimal("0"), total, "Unpaid"

                fee_rows.append((
                    fee_name, now(), now(), s["code"], s["name"],
                    s["program"], acad_yr, term["name"], total,
                    paid, outstanding, status, due, term["start"],
                ))

                if paid > 0:
                    pay_counter += 1
                    pay_name = f"PAY-{pay_counter:07d}"
                    pay_date = random_date(term["start"], term["start"] + timedelta(days=15))
                    payment_rows.append((
                        pay_name, now(), now(), s["code"], s["name"],
                        pay_date, float(paid), float(paid),
                        f"REF-{pay_counter:07d}",
                        random.choice(PAYMENT_MODES),
                    ))
                    payment_ref_rows.append((
                        f"PAYREF-{pay_counter:07d}", pay_name, fee_name, float(paid),
                    ))

    log.info(f"Inserting {len(fee_rows)} fee records...")
    for start in range(0, len(fee_rows), 500):
        cur.executemany("""
            INSERT IGNORE INTO `tabFees`
              (name, creation, modified, student, student_name, program,
               academic_year, academic_term, grand_total, paid_amount,
               outstanding_amount, status, due_date, posting_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, fee_rows[start:start+500])
        conn.commit()

    log.info(f"Inserting {len(payment_rows)} payment records...")
    for start in range(0, len(payment_rows), 500):
        cur.executemany("""
            INSERT IGNORE INTO `tabPayment Entry`
              (name, creation, modified, party, party_name,
               posting_date, paid_amount, received_amount, reference_no, mode_of_payment)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, payment_rows[start:start+500])
        conn.commit()

    for start in range(0, len(payment_ref_rows), 500):
        cur.executemany("""
            INSERT IGNORE INTO `tabPayment Entry Reference`
              (name, parent, reference_name, allocated_amount)
            VALUES (%s,%s,%s,%s)
        """, payment_ref_rows[start:start+500])
        conn.commit()

    cur.close()
    conn.close()
    log.info(f"ERPNext seeding complete: {NUM_STUDENTS} students, {len(fee_rows)} fees, {len(payment_rows)} payments")
    return students


# ============================================================
# Moodle Data Seeding
# ============================================================

def seed_moodle(students):
    log.info("=== Seeding Moodle (MySQL) ===")
    wait_for_db(MOODLE_CONN)
    conn = pymysql.connect(**MOODLE_CONN)
    cur = conn.cursor()

    # Check if Moodle tables exist (Moodle may still be installing)
    cur.execute("SHOW TABLES LIKE 'mdl_user'")
    if not cur.fetchone():
        log.warning("Moodle tables not found yet — skipping Moodle seeding. Run 'make seed-moodle' after Moodle finishes installing.")
        cur.close()
        conn.close()
        return

    # Get next user id
    cur.execute("SELECT COALESCE(MAX(id), 1) FROM mdl_user")
    max_uid = cur.fetchone()[0]

    # Create courses per program
    log.info("Creating Moodle courses...")
    course_id_map = {}
    cur.execute("SELECT COALESCE(MAX(id), 1) FROM mdl_course")
    max_course_id = cur.fetchone()[0]

    ts = int(datetime.now().timestamp())

    for prog_code, course_list in COURSES_BY_PROGRAM.items():
        for i, course_name in enumerate(course_list):
            course_shortname = f"{prog_code}-{str(i+1).zfill(3)}"
            cur.execute("SELECT id FROM mdl_course WHERE shortname = %s", (course_shortname,))
            row = cur.fetchone()
            if row:
                course_id_map[course_shortname] = row[0]
                continue

            max_course_id += 1
            cur.execute("""
                INSERT INTO mdl_course
                  (id, category, fullname, shortname, idnumber, summary, format,
                   startdate, enddate, timecreated, timemodified, visible, lang)
                VALUES (%s, 1, %s, %s, %s, '', 'topics', %s, %s, %s, %s, 1, 'es')
            """, (max_course_id, course_name, course_shortname, course_shortname,
                  ts - 31536000, ts + 31536000, ts, ts))
            course_id_map[course_shortname] = max_course_id

    conn.commit()
    log.info(f"Created {len(course_id_map)} Moodle courses")

    # Enroll students and generate grades
    log.info("Enrolling students and generating grade data...")
    enrol_id_map = {}
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM mdl_enrol")
    max_enrol_id = cur.fetchone()[0]

    grade_rows = []
    forum_rows = []
    quiz_rows = []
    completion_rows = []

    for s in students[:2000]:  # Limit to 2000 for speed (representative sample)
        uid = max_uid + int(s["code"].split("-")[1])
        prog = s["program"]
        username = s["code"].lower().replace("-", "")

        # Create Moodle user
        cur.execute("SELECT id FROM mdl_user WHERE username = %s", (username,))
        if cur.fetchone():
            continue

        cur.execute("""
            INSERT INTO mdl_user
              (id, auth, confirmed, username, password, email, firstname, lastname,
               timecreated, timemodified, lastlogin, lang, timezone, country)
            VALUES (%s, 'manual', 1, %s, %s, %s, %s, %s, %s, %s, %s, 'es', '99', 'SV')
        """, (uid, username, md5_password("Student1234!"), s["email"],
               s["first_name"] if "first_name" in s else s["name"].split()[0],
               " ".join(s["name"].split()[1:]) if "first_name" in s else "",
               ts - random.randint(0, 63072000), ts, ts - random.randint(0, 604800)))

        # Enroll student in their program courses
        courses = list(COURSES_BY_PROGRAM[prog])
        enrolled_courses = random.sample(courses, min(6, len(courses)))
        for course_name in enrolled_courses:
            idx = COURSES_BY_PROGRAM[prog].index(course_name)
            shortname = f"{prog}-{str(idx+1).zfill(3)}"
            if shortname not in course_id_map:
                continue
            course_id = course_id_map[shortname]

            # Create enrolment
            max_enrol_id += 1
            cur.execute("""
                INSERT IGNORE INTO mdl_enrol (id, enrol, status, courseid, timecreated, timemodified)
                VALUES (%s, 'manual', 0, %s, %s, %s)
            """, (max_enrol_id, course_id, ts, ts))

            # User enrolment
            cur.execute("""
                INSERT IGNORE INTO mdl_user_enrolments
                  (enrolid, userid, modifierid, timestart, timeend, timecreated, timemodified, status)
                VALUES (%s, %s, 2, %s, %s, %s, %s, 0)
            """, (max_enrol_id, uid, ts - 31536000, ts + 31536000, ts, ts))

            # Grade
            note = round(random.gauss(7.0, 1.8), 2)
            note = max(0, min(10, note))
            grade_rows.append((uid, course_id, note, 10.0, ts, ts))

            # Forum posts (1-5 per enrolled course)
            for _ in range(random.randint(1, 5)):
                forum_rows.append((uid, course_id, ts - random.randint(0, 2592000)))

            # Quiz attempts
            for _ in range(random.randint(0, 3)):
                quiz_score = round(random.gauss(70, 15), 1)
                quiz_rows.append((uid, course_id, max(0, min(100, quiz_score)), ts - random.randint(0, 2592000)))

            # Completion
            completed = random.random() < 0.68
            completion_rows.append((uid, course_id, 1 if completed else 0, ts - random.randint(0, 604800) if completed else None))

    # Batch insert grades
    if grade_rows:
        cur.executemany("""
            INSERT IGNORE INTO mdl_grade_grades
              (userid, itemid, rawgrade, rawgrademax, timecreated, timemodified)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, grade_rows[:5000])

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Moodle seeding complete: {len(grade_rows)} grade records")


# ============================================================
# Main
# ============================================================

def main():
    log.info(f"Starting data seeding for '{UNIVERSITY_NAME}' ({NUM_STUDENTS} students)")
    start_time = time.time()

    try:
        students = seed_erpnext()
        seed_moodle(students)
        elapsed = time.time() - start_time
        log.info(f"Data seeding completed successfully in {elapsed:.1f}s")
    except Exception as e:
        log.error(f"Seeding failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
