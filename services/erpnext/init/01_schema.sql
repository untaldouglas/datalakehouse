-- ============================================================
-- ERPNext-compatible University Schema
-- Simulates key ERPNext Education Module tables
-- ============================================================

SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- Academic Programs
CREATE TABLE IF NOT EXISTS `tabProgram` (
  `name`                VARCHAR(140) NOT NULL,
  `creation`            DATETIME(6),
  `modified`            DATETIME(6),
  `modified_by`         VARCHAR(140),
  `owner`               VARCHAR(140),
  `docstatus`           TINYINT(1) DEFAULT 0,
  `program_name`        VARCHAR(140),
  `program_abbreviation` VARCHAR(50),
  `department`          VARCHAR(140),
  `duration`            INT DEFAULT 4,
  `duration_type`       VARCHAR(50) DEFAULT 'Year',
  `description`         TEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Academic Year
CREATE TABLE IF NOT EXISTS `tabAcademic Year` (
  `name`         VARCHAR(140) NOT NULL,
  `creation`     DATETIME(6),
  `modified`     DATETIME(6),
  `year_start_date` DATE,
  `year_end_date`   DATE,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Academic Term
CREATE TABLE IF NOT EXISTS `tabAcademic Term` (
  `name`          VARCHAR(140) NOT NULL,
  `creation`      DATETIME(6),
  `modified`      DATETIME(6),
  `academic_year` VARCHAR(140),
  `term_name`     VARCHAR(140),
  `term_start_date` DATE,
  `term_end_date`   DATE,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Students
CREATE TABLE IF NOT EXISTS `tabStudent` (
  `name`               VARCHAR(140) NOT NULL,
  `creation`           DATETIME(6),
  `modified`           DATETIME(6),
  `docstatus`          TINYINT(1) DEFAULT 1,
  `student_name`       VARCHAR(140),
  `first_name`         VARCHAR(140),
  `last_name`          VARCHAR(140),
  `gender`             VARCHAR(10),
  `date_of_birth`      DATE,
  `joining_date`       DATE,
  `student_email_id`   VARCHAR(140),
  `student_mobile_number` VARCHAR(30),
  `program`            VARCHAR(140),
  `academic_year`      VARCHAR(140),
  `enabled`            TINYINT(1) DEFAULT 1,
  `nationality`        VARCHAR(140) DEFAULT 'Salvadoreña',
  PRIMARY KEY (`name`),
  KEY `idx_program` (`program`),
  KEY `idx_academic_year` (`academic_year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Student Group (sections/classes)
CREATE TABLE IF NOT EXISTS `tabStudent Group` (
  `name`          VARCHAR(140) NOT NULL,
  `creation`      DATETIME(6),
  `modified`      DATETIME(6),
  `docstatus`     TINYINT(1) DEFAULT 1,
  `student_group_name` VARCHAR(140),
  `program`       VARCHAR(140),
  `academic_year` VARCHAR(140),
  `academic_term` VARCHAR(140),
  `batch_size`    INT DEFAULT 40,
  `active`        TINYINT(1) DEFAULT 1,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Student Group Student (enrollment link)
CREATE TABLE IF NOT EXISTS `tabStudent Group Student` (
  `name`          VARCHAR(140) NOT NULL,
  `parent`        VARCHAR(140),
  `student`       VARCHAR(140),
  `student_name`  VARCHAR(140),
  `active`        TINYINT(1) DEFAULT 1,
  PRIMARY KEY (`name`),
  KEY `idx_parent` (`parent`),
  KEY `idx_student` (`student`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fee Category
CREATE TABLE IF NOT EXISTS `tabFee Category` (
  `name`         VARCHAR(140) NOT NULL,
  `creation`     DATETIME(6),
  `modified`     DATETIME(6),
  `category_name` VARCHAR(140),
  `description`  TEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fee Structure
CREATE TABLE IF NOT EXISTS `tabFee Structure` (
  `name`          VARCHAR(140) NOT NULL,
  `creation`      DATETIME(6),
  `modified`      DATETIME(6),
  `docstatus`     TINYINT(1) DEFAULT 1,
  `program`       VARCHAR(140),
  `academic_year` VARCHAR(140),
  `academic_term` VARCHAR(140),
  `total_amount`  DECIMAL(20,2) DEFAULT 0,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fee Structure Component
CREATE TABLE IF NOT EXISTS `tabFee Structure Component` (
  `name`           VARCHAR(140) NOT NULL,
  `parent`         VARCHAR(140),
  `fee_category`   VARCHAR(140),
  `description`    VARCHAR(255),
  `amount`         DECIMAL(20,2) DEFAULT 0,
  PRIMARY KEY (`name`),
  KEY `idx_parent` (`parent`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Student Fees (individual fee invoices)
CREATE TABLE IF NOT EXISTS `tabFees` (
  `name`             VARCHAR(140) NOT NULL,
  `creation`         DATETIME(6),
  `modified`         DATETIME(6),
  `docstatus`        TINYINT(1) DEFAULT 1,
  `student`          VARCHAR(140),
  `student_name`     VARCHAR(140),
  `program`          VARCHAR(140),
  `academic_year`    VARCHAR(140),
  `academic_term`    VARCHAR(140),
  `fee_structure`    VARCHAR(140),
  `due_date`         DATE,
  `posting_date`     DATE,
  `grand_total`      DECIMAL(20,2) DEFAULT 0,
  `paid_amount`      DECIMAL(20,2) DEFAULT 0,
  `outstanding_amount` DECIMAL(20,2) DEFAULT 0,
  `status`           VARCHAR(50) DEFAULT 'Unpaid',
  `currency`         VARCHAR(10) DEFAULT 'USD',
  PRIMARY KEY (`name`),
  KEY `idx_student` (`student`),
  KEY `idx_program` (`program`),
  KEY `idx_academic_year` (`academic_year`),
  KEY `idx_status` (`status`),
  KEY `idx_due_date` (`due_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Payment Entry
CREATE TABLE IF NOT EXISTS `tabPayment Entry` (
  `name`             VARCHAR(140) NOT NULL,
  `creation`         DATETIME(6),
  `modified`         DATETIME(6),
  `docstatus`        TINYINT(1) DEFAULT 1,
  `payment_type`     VARCHAR(50) DEFAULT 'Receive',
  `party_type`       VARCHAR(50) DEFAULT 'Student',
  `party`            VARCHAR(140),
  `party_name`       VARCHAR(140),
  `posting_date`     DATE,
  `paid_amount`      DECIMAL(20,2) DEFAULT 0,
  `received_amount`  DECIMAL(20,2) DEFAULT 0,
  `reference_no`     VARCHAR(140),
  `mode_of_payment`  VARCHAR(140) DEFAULT 'Cash',
  `currency`         VARCHAR(10) DEFAULT 'USD',
  `remarks`          TEXT,
  PRIMARY KEY (`name`),
  KEY `idx_party` (`party`),
  KEY `idx_posting_date` (`posting_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Payment Entry Reference (links payment to fee)
CREATE TABLE IF NOT EXISTS `tabPayment Entry Reference` (
  `name`             VARCHAR(140) NOT NULL,
  `parent`           VARCHAR(140),
  `reference_doctype` VARCHAR(140) DEFAULT 'Fees',
  `reference_name`   VARCHAR(140),
  `allocated_amount` DECIMAL(20,2) DEFAULT 0,
  PRIMARY KEY (`name`),
  KEY `idx_parent` (`parent`),
  KEY `idx_reference` (`reference_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Course (subject/materia)
CREATE TABLE IF NOT EXISTS `tabCourse` (
  `name`         VARCHAR(140) NOT NULL,
  `creation`     DATETIME(6),
  `modified`     DATETIME(6),
  `course_name`  VARCHAR(140),
  `department`   VARCHAR(140),
  `course_abbreviation` VARCHAR(50),
  `description`  TEXT,
  `credit_hours` DECIMAL(4,1) DEFAULT 3,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Program Course (which courses belong to program)
CREATE TABLE IF NOT EXISTS `tabProgram Course` (
  `name`          VARCHAR(140) NOT NULL,
  `parent`        VARCHAR(140),
  `course`        VARCHAR(140),
  `course_name`   VARCHAR(140),
  `required`      TINYINT(1) DEFAULT 1,
  PRIMARY KEY (`name`),
  KEY `idx_parent` (`parent`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Student Log (attendance/academic events)
CREATE TABLE IF NOT EXISTS `tabStudent Log` (
  `name`         VARCHAR(140) NOT NULL,
  `creation`     DATETIME(6),
  `modified`     DATETIME(6),
  `student`      VARCHAR(140),
  `log_type`     VARCHAR(50),
  `time`         DATETIME(6),
  `skip_attendance` TINYINT(1) DEFAULT 0,
  PRIMARY KEY (`name`),
  KEY `idx_student` (`student`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
