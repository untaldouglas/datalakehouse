-- ============================================================
-- Semantic Layer Schema — universidad_analytics
-- Populated every 12h by Airflow Gold DAG
-- Metabase dashboards read from these tables
-- ============================================================

\connect universidad_analytics

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_programa (
  programa_id       SERIAL PRIMARY KEY,
  programa_codigo   VARCHAR(20)  UNIQUE NOT NULL,
  programa_nombre   VARCHAR(255) NOT NULL,
  departamento      VARCHAR(100),
  duracion_anios    INT          DEFAULT 4,
  activo            BOOLEAN      DEFAULT TRUE,
  created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_alumno (
  alumno_id         SERIAL PRIMARY KEY,
  alumno_codigo     VARCHAR(50)  UNIQUE NOT NULL,
  nombre_completo   VARCHAR(255),
  genero            VARCHAR(20),
  fecha_nacimiento  DATE,
  fecha_ingreso     DATE,
  programa_codigo   VARCHAR(20),
  anio_academico    VARCHAR(20),
  estado            VARCHAR(30)  DEFAULT 'Activo',
  created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
  updated_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
  tiempo_id         SERIAL PRIMARY KEY,
  fecha             DATE         UNIQUE NOT NULL,
  anio              INT,
  mes               INT,
  trimestre         INT,
  semestre          INT,
  nombre_mes        VARCHAR(20),
  nombre_dia        VARCHAR(20),
  es_fin_semana     BOOLEAN,
  anio_academico    VARCHAR(20),
  ciclo_academico   VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_curso (
  curso_id          SERIAL PRIMARY KEY,
  curso_codigo      VARCHAR(50)  UNIQUE NOT NULL,
  curso_nombre      VARCHAR(255),
  programa_codigo   VARCHAR(20),
  creditos          NUMERIC(4,1),
  created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- FACT TABLES — FINANCIAL (Dashboard Gerencial)
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_ingresos_matricula (
  ingreso_id         SERIAL PRIMARY KEY,
  fecha_pago         DATE,
  alumno_codigo      VARCHAR(50),
  programa_codigo    VARCHAR(20),
  anio_academico     VARCHAR(20),
  ciclo_academico    VARCHAR(20),
  categoria_cobro    VARCHAR(100),
  monto_facturado    NUMERIC(15,2) DEFAULT 0,
  monto_pagado       NUMERIC(15,2) DEFAULT 0,
  monto_pendiente    NUMERIC(15,2) DEFAULT 0,
  modo_pago          VARCHAR(50),
  estado_cobro       VARCHAR(30),
  dias_mora          INT           DEFAULT 0,
  fecha_vencimiento  DATE,
  created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_pagos_detalle (
  pago_id            SERIAL PRIMARY KEY,
  fecha_pago         DATE NOT NULL,
  alumno_codigo      VARCHAR(50),
  programa_codigo    VARCHAR(20),
  referencia_pago    VARCHAR(100),
  monto              NUMERIC(15,2) DEFAULT 0,
  modo_pago          VARCHAR(50),
  concepto           VARCHAR(255),
  anio_academico     VARCHAR(20),
  ciclo_academico    VARCHAR(20),
  created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- FACT TABLES — ACADEMIC (Dashboard Académico)
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_matriculas (
  matricula_id       SERIAL PRIMARY KEY,
  alumno_codigo      VARCHAR(50),
  programa_codigo    VARCHAR(20),
  curso_codigo       VARCHAR(50),
  anio_academico     VARCHAR(20),
  ciclo_academico    VARCHAR(20),
  fecha_matricula    DATE,
  estado_matricula   VARCHAR(30)   DEFAULT 'Activa',
  created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_calificaciones (
  calificacion_id    SERIAL PRIMARY KEY,
  alumno_codigo      VARCHAR(50),
  curso_codigo       VARCHAR(50),
  programa_codigo    VARCHAR(20),
  anio_academico     VARCHAR(20),
  ciclo_academico    VARCHAR(20),
  nota_final         NUMERIC(5,2),
  nota_maxima        NUMERIC(5,2)  DEFAULT 10.0,
  aprobado           BOOLEAN,
  fecha_evaluacion   DATE,
  tipo_evaluacion    VARCHAR(50),
  intentos           INT           DEFAULT 1,
  created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_actividad_moodle (
  actividad_id         SERIAL PRIMARY KEY,
  alumno_codigo        VARCHAR(50),
  curso_codigo         VARCHAR(50),
  programa_codigo      VARCHAR(20),
  tipo_actividad       VARCHAR(50),
  fecha_actividad      DATE,
  completado           BOOLEAN      DEFAULT FALSE,
  tiempo_dedicado_min  INT          DEFAULT 0,
  created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- KPI TABLES (pre-computed for fast dashboard rendering)
-- ============================================================

CREATE TABLE IF NOT EXISTS kpi_financiero_mensual (
  kpi_id                  SERIAL PRIMARY KEY,
  anio                    INT,
  mes                     INT,
  programa_codigo         VARCHAR(20),
  anio_academico          VARCHAR(20),
  ciclo_academico         VARCHAR(20),
  ingresos_facturados     NUMERIC(15,2) DEFAULT 0,
  ingresos_cobrados       NUMERIC(15,2) DEFAULT 0,
  ingresos_pendientes     NUMERIC(15,2) DEFAULT 0,
  alumnos_activos         INT           DEFAULT 0,
  alumnos_morosos         INT           DEFAULT 0,
  nuevas_matriculas       INT           DEFAULT 0,
  tasa_cobranza           NUMERIC(5,2)  DEFAULT 0,
  tasa_morosidad          NUMERIC(5,2)  DEFAULT 0,
  ingreso_promedio_alumno NUMERIC(10,2) DEFAULT 0,
  updated_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (anio, mes, programa_codigo, anio_academico, ciclo_academico)
);

CREATE TABLE IF NOT EXISTS kpi_academico_periodo (
  kpi_id                      SERIAL PRIMARY KEY,
  anio_academico              VARCHAR(20),
  ciclo_academico             VARCHAR(20),
  programa_codigo             VARCHAR(20),
  curso_codigo                VARCHAR(50),
  alumnos_matriculados        INT          DEFAULT 0,
  promedio_notas              NUMERIC(5,2) DEFAULT 0,
  tasa_aprobacion             NUMERIC(5,2) DEFAULT 0,
  tasa_reprobacion            NUMERIC(5,2) DEFAULT 0,
  tasa_desercion              NUMERIC(5,2) DEFAULT 0,
  promedio_asistencia         NUMERIC(5,2) DEFAULT 0,
  actividades_completadas_pct NUMERIC(5,2) DEFAULT 0,
  updated_at                  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (anio_academico, ciclo_academico, programa_codigo, curso_codigo)
);

CREATE TABLE IF NOT EXISTS kpi_retencion_cohorte (
  kpi_id             SERIAL PRIMARY KEY,
  anio_ingreso       INT,
  programa_codigo    VARCHAR(20),
  anio_seguimiento   INT,
  anio_relativo      INT,
  alumnos_cohorte    INT          DEFAULT 0,
  alumnos_retenidos  INT          DEFAULT 0,
  tasa_retencion     NUMERIC(5,2) DEFAULT 0,
  updated_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (anio_ingreso, programa_codigo, anio_seguimiento)
);

CREATE TABLE IF NOT EXISTS etl_run_log (
  run_id          SERIAL PRIMARY KEY,
  dag_id          VARCHAR(100),
  run_date        TIMESTAMP,
  source          VARCHAR(50),
  layer           VARCHAR(20),
  records_read    BIGINT  DEFAULT 0,
  records_written BIGINT  DEFAULT 0,
  status          VARCHAR(20) DEFAULT 'success',
  error_msg       TEXT,
  duration_sec    INT     DEFAULT 0
);

-- ============================================================
-- INDEXES for dashboard performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_ing_programa ON fact_ingresos_matricula(programa_codigo);
CREATE INDEX IF NOT EXISTS idx_fact_ing_anio     ON fact_ingresos_matricula(anio_academico);
CREATE INDEX IF NOT EXISTS idx_fact_ing_estado   ON fact_ingresos_matricula(estado_cobro);
CREATE INDEX IF NOT EXISTS idx_fact_cal_alumno   ON fact_calificaciones(alumno_codigo);
CREATE INDEX IF NOT EXISTS idx_fact_cal_curso    ON fact_calificaciones(curso_codigo);
CREATE INDEX IF NOT EXISTS idx_kpi_fin_periodo   ON kpi_financiero_mensual(anio, mes);
CREATE INDEX IF NOT EXISTS idx_kpi_acad_periodo  ON kpi_academico_periodo(anio_academico, ciclo_academico);

-- ============================================================
-- SEED: static dimensions
-- ============================================================

INSERT INTO dim_programa (programa_codigo, programa_nombre, departamento, duracion_anios) VALUES
  ('MED', 'Medicina',             'Ciencias de la Salud',  6),
  ('INF', 'Informática',          'Ciencias e Ingeniería', 4),
  ('GN',  'Gestión de Negocios',  'Ciencias Económicas',   4)
ON CONFLICT (programa_codigo) DO NOTHING;

-- Time dimension: 2020-2026
INSERT INTO dim_tiempo (fecha, anio, mes, trimestre, semestre, nombre_mes, nombre_dia, es_fin_semana, anio_academico, ciclo_academico)
SELECT
  d::DATE,
  EXTRACT(YEAR  FROM d)::INT,
  EXTRACT(MONTH FROM d)::INT,
  EXTRACT(QUARTER FROM d)::INT,
  CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END,
  TO_CHAR(d, 'TMMonth'),
  TO_CHAR(d, 'TMDay'),
  EXTRACT(DOW FROM d) IN (0, 6),
  EXTRACT(YEAR FROM d)::TEXT || '-' || (EXTRACT(YEAR FROM d)::INT + 1)::TEXT,
  CASE WHEN EXTRACT(MONTH FROM d) BETWEEN 1 AND 6 THEN 'Ciclo I' ELSE 'Ciclo II' END
FROM generate_series('2020-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (fecha) DO NOTHING;

-- Grant the analytics user full access to all tables
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO analytics;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO analytics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO analytics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO analytics;
