#!/usr/bin/env python3
"""
Schema round-trip validation — ejecutar antes de escribir DAGs.

Verifica que todos los schemas Iceberg definidos en common/lakehouse.py y en
los DAGs son correctamente serializables por PyArrow (sin required=True).

Regla de oro:
    PyArrow siempre genera tipos nullable (opcionales).
    Nunca usar required=True en NestedField si la fuente es una base de datos relacional.

Uso:
    make validate-schemas
    # o directamente:
    docker compose exec airflow-scheduler python3 /opt/airflow/scripts/validate_schemas.py
"""

import sys
sys.path.insert(0, "/opt/airflow/dags")

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from pyiceberg.io.pyarrow import schema_to_pyarrow

results = []


def validate(name: str, schema: Schema):
    """Valida que un schema Iceberg es compatible con PyArrow."""
    errors = []

    # 1. Verificar que ningún campo tiene required=True
    for field in schema.fields:
        if field.required:
            errors.append(
                f"  Campo '{field.name}' (id={field.field_id}) tiene required=True. "
                f"PyArrow no puede generar columnas not-null desde queries relacionales."
            )

    # 2. Verificar que el schema se puede convertir a PyArrow
    try:
        arrow_schema = schema_to_pyarrow(schema)
    except Exception as e:
        errors.append(f"  schema_to_pyarrow() falló: {e}")
        arrow_schema = None

    # 3. Si la conversión fue exitosa, verificar round-trip con datos reales
    if arrow_schema is not None:
        try:
            sample = {}
            for field in arrow_schema:
                t = field.type
                if pa.types.is_integer(t):
                    sample[field.name] = pa.array([1, None], type=t)
                elif pa.types.is_floating(t):
                    sample[field.name] = pa.array([1.0, None], type=t)
                elif pa.types.is_boolean(t):
                    sample[field.name] = pa.array([True, None], type=t)
                elif pa.types.is_date(t):
                    sample[field.name] = pa.array([None, None], type=t)
                else:
                    sample[field.name] = pa.array(["x", None], type=t)

            table = pa.table(sample, schema=arrow_schema)
            assert table.num_rows == 2
        except Exception as e:
            errors.append(f"  Construcción de tabla PyArrow falló: {e}")

    if errors:
        results.append(("FAIL", name, errors))
        print(f"  \033[31m❌ {name}\033[0m")
        for err in errors:
            print(f"     {err}")
    else:
        results.append(("OK", name, []))
        print(f"  \033[32m✅ {name}\033[0m")


if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║       Validación de Schemas Iceberg ↔ PyArrow       ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()
    print("  Importando schemas desde common/lakehouse.py...")
    print()

    from common.lakehouse import (
        SCHEMA_BRONZE_MOODLE_USERS,
        SCHEMA_BRONZE_MOODLE_COURSES,
        SCHEMA_BRONZE_MOODLE_GRADES,
        SCHEMA_BRONZE_MOODLE_ENROLMENTS,
        SCHEMA_BRONZE_ERPNEXT_STUDENTS,
        SCHEMA_BRONZE_ERPNEXT_FEES,
        SCHEMA_BRONZE_ERPNEXT_PAYMENTS,
        SCHEMA_BRONZE_ERPNEXT_PROGRAMS,
    )

    from dag_03_schemas import (
        SCHEMA_SILVER_STUDENTS,
        SCHEMA_SILVER_FEES,
        SCHEMA_SILVER_PAYMENTS,
        SCHEMA_SILVER_GRADES,
    )

    bronze_schemas = {
        "bronze.moodle_users":       SCHEMA_BRONZE_MOODLE_USERS,
        "bronze.moodle_courses":     SCHEMA_BRONZE_MOODLE_COURSES,
        "bronze.moodle_grades":      SCHEMA_BRONZE_MOODLE_GRADES,
        "bronze.moodle_enrolments":  SCHEMA_BRONZE_MOODLE_ENROLMENTS,
        "bronze.erpnext_students":   SCHEMA_BRONZE_ERPNEXT_STUDENTS,
        "bronze.erpnext_fees":       SCHEMA_BRONZE_ERPNEXT_FEES,
        "bronze.erpnext_payments":   SCHEMA_BRONZE_ERPNEXT_PAYMENTS,
        "bronze.erpnext_programs":   SCHEMA_BRONZE_ERPNEXT_PROGRAMS,
    }

    silver_schemas = {
        "silver.students":  SCHEMA_SILVER_STUDENTS,
        "silver.fees":      SCHEMA_SILVER_FEES,
        "silver.payments":  SCHEMA_SILVER_PAYMENTS,
        "silver.grades":    SCHEMA_SILVER_GRADES,
    }

    print("  --- Schemas Bronze ---")
    for name, schema in bronze_schemas.items():
        validate(name, schema)

    print()
    print("  --- Schemas Silver ---")
    for name, schema in silver_schemas.items():
        validate(name, schema)

    print()
    passed = sum(1 for r in results if r[0] == "OK")
    failed = sum(1 for r in results if r[0] == "FAIL")

    if failed == 0:
        print(f"  \033[32m✅ Todos los schemas son válidos ({passed}/{passed}).\033[0m")
        print()
        print("  Reglas aplicadas:")
        print("    • Ningún campo tiene required=True")
        print("    • Todos los schemas se convierten a PyArrow sin error")
        print("    • Round-trip con datos reales (incluyendo None) funciona")
        sys.exit(0)
    else:
        print(f"  \033[31m❌ {failed} schema(s) inválidos.\033[0m")
        print()
        print("  Corrección: eliminar required=True de los NestedField afectados.")
        print("  Ver: docs/STACK.md — sección PyArrow")
        sys.exit(1)
