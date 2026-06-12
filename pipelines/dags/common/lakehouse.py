"""
Shared lakehouse utilities for all DAGs.
Handles PyIceberg catalog connections, schema definitions, and write helpers.
"""

import os
import logging
from typing import Optional
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, DateType,
    TimestampType, BooleanType, DecimalType, IntegerType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform, IdentityTransform

log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")
LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")


def get_catalog(branch: str = "main"):
    """Return a PyIceberg catalog connected to Nessie + MinIO."""
    return load_catalog(
        "nessie",
        **{
            "type": "nessie",
            "uri": NESSIE_URI,
            "ref": branch,
            "warehouse": f"s3://{LAKEHOUSE_BUCKET}",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def ensure_namespace(catalog, namespace: str):
    try:
        catalog.create_namespace(namespace)
        log.info(f"Created namespace: {namespace}")
    except Exception:
        pass  # Already exists


def upsert_iceberg(catalog, table_identifier: str, df: pa.Table, schema: Schema,
                   partition_spec: Optional[PartitionSpec] = None,
                   mode: str = "append"):
    """Create table if not exists and write data."""
    namespace, table_name = table_identifier.rsplit(".", 1)
    ensure_namespace(catalog, namespace)

    if not catalog.table_exists(table_identifier):
        log.info(f"Creating table {table_identifier}")
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec or PartitionSpec(),
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy",
            },
        )

    table = catalog.load_table(table_identifier)
    if mode == "overwrite":
        table.overwrite(df)
    else:
        table.append(df)

    log.info(f"Wrote {len(df)} rows to {table_identifier}")
    return len(df)


# ============================================================
# Iceberg Schemas (Bronze Layer)
# ============================================================

SCHEMA_MOODLE_USERS = Schema(
    NestedField(1,  "user_id",       LongType(),      required=True),
    NestedField(2,  "username",      StringType()),
    NestedField(3,  "email",         StringType()),
    NestedField(4,  "firstname",     StringType()),
    NestedField(5,  "lastname",      StringType()),
    NestedField(6,  "confirmed",     LongType()),
    NestedField(7,  "lang",          StringType()),
    NestedField(8,  "country",       StringType()),
    NestedField(9,  "timecreated",   LongType()),
    NestedField(10, "timemodified",  LongType()),
    NestedField(11, "lastlogin",     LongType()),
    NestedField(12, "_etl_loaded_at", StringType()),
)

SCHEMA_MOODLE_COURSES = Schema(
    NestedField(1,  "course_id",     LongType(),      required=True),
    NestedField(2,  "fullname",      StringType()),
    NestedField(3,  "shortname",     StringType()),
    NestedField(4,  "category",      LongType()),
    NestedField(5,  "visible",       LongType()),
    NestedField(6,  "startdate",     LongType()),
    NestedField(7,  "enddate",       LongType()),
    NestedField(8,  "timecreated",   LongType()),
    NestedField(9,  "timemodified",  LongType()),
    NestedField(10, "_etl_loaded_at", StringType()),
)

SCHEMA_MOODLE_GRADES = Schema(
    NestedField(1,  "grade_id",      LongType(),      required=True),
    NestedField(2,  "itemid",        LongType()),
    NestedField(3,  "userid",        LongType()),
    NestedField(4,  "rawgrade",      DoubleType()),
    NestedField(5,  "rawgrademax",   DoubleType()),
    NestedField(6,  "timecreated",   LongType()),
    NestedField(7,  "timemodified",  LongType()),
    NestedField(8,  "_etl_loaded_at", StringType()),
)

SCHEMA_MOODLE_ENROLMENTS = Schema(
    NestedField(1,  "enrol_id",      LongType(),      required=True),
    NestedField(2,  "enrolid",       LongType()),
    NestedField(3,  "userid",        LongType()),
    NestedField(4,  "status",        LongType()),
    NestedField(5,  "timestart",     LongType()),
    NestedField(6,  "timeend",       LongType()),
    NestedField(7,  "timecreated",   LongType()),
    NestedField(8,  "timemodified",  LongType()),
    NestedField(9,  "_etl_loaded_at", StringType()),
)

SCHEMA_ERPNEXT_STUDENTS = Schema(
    NestedField(1,  "name",          StringType(),    required=True),
    NestedField(2,  "student_name",  StringType()),
    NestedField(3,  "first_name",    StringType()),
    NestedField(4,  "last_name",     StringType()),
    NestedField(5,  "gender",        StringType()),
    NestedField(6,  "date_of_birth", StringType()),
    NestedField(7,  "joining_date",  StringType()),
    NestedField(8,  "program",       StringType()),
    NestedField(9,  "academic_year", StringType()),
    NestedField(10, "enabled",       LongType()),
    NestedField(11, "modified",      StringType()),
    NestedField(12, "_etl_loaded_at", StringType()),
)

SCHEMA_ERPNEXT_FEES = Schema(
    NestedField(1,  "name",              StringType(),    required=True),
    NestedField(2,  "student",           StringType()),
    NestedField(3,  "student_name",      StringType()),
    NestedField(4,  "program",           StringType()),
    NestedField(5,  "academic_year",     StringType()),
    NestedField(6,  "academic_term",     StringType()),
    NestedField(7,  "due_date",          StringType()),
    NestedField(8,  "posting_date",      StringType()),
    NestedField(9,  "grand_total",       DoubleType()),
    NestedField(10, "paid_amount",       DoubleType()),
    NestedField(11, "outstanding_amount", DoubleType()),
    NestedField(12, "status",            StringType()),
    NestedField(13, "modified",          StringType()),
    NestedField(14, "_etl_loaded_at",    StringType()),
)

SCHEMA_ERPNEXT_PAYMENTS = Schema(
    NestedField(1,  "name",            StringType(),    required=True),
    NestedField(2,  "party",           StringType()),
    NestedField(3,  "party_name",      StringType()),
    NestedField(4,  "posting_date",    StringType()),
    NestedField(5,  "paid_amount",     DoubleType()),
    NestedField(6,  "received_amount", DoubleType()),
    NestedField(7,  "reference_no",    StringType()),
    NestedField(8,  "mode_of_payment", StringType()),
    NestedField(9,  "modified",        StringType()),
    NestedField(10, "_etl_loaded_at",  StringType()),
)
