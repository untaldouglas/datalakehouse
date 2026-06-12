-- ============================================================
-- Moodle Core Tables (subset needed for ETL pipeline)
-- Compatible with Moodle 4.x schema
-- ============================================================

SET NAMES utf8mb4;

-- Users
CREATE TABLE IF NOT EXISTS mdl_user (
  id            BIGINT(10)    NOT NULL AUTO_INCREMENT,
  auth          VARCHAR(20)   NOT NULL DEFAULT 'manual',
  confirmed     TINYINT(1)    NOT NULL DEFAULT 0,
  policyagreed  TINYINT(1)    NOT NULL DEFAULT 0,
  deleted       TINYINT(1)    NOT NULL DEFAULT 0,
  suspended     TINYINT(1)    NOT NULL DEFAULT 0,
  username      VARCHAR(100)  NOT NULL DEFAULT '',
  password      VARCHAR(255)  NOT NULL DEFAULT '',
  idnumber      VARCHAR(255)  NOT NULL DEFAULT '',
  firstname     VARCHAR(100)  NOT NULL DEFAULT '',
  lastname      VARCHAR(100)  NOT NULL DEFAULT '',
  email         VARCHAR(100)  NOT NULL DEFAULT '',
  emailstop     TINYINT(1)    NOT NULL DEFAULT 0,
  lang          VARCHAR(30)   NOT NULL DEFAULT 'es',
  timezone      VARCHAR(100)  NOT NULL DEFAULT '99',
  firstaccess   BIGINT(10)    NOT NULL DEFAULT 0,
  lastaccess    BIGINT(10)    NOT NULL DEFAULT 0,
  lastlogin     BIGINT(10)    NOT NULL DEFAULT 0,
  currentlogin  BIGINT(10)    NOT NULL DEFAULT 0,
  country       CHAR(2)       NOT NULL DEFAULT '',
  city          VARCHAR(120)  NOT NULL DEFAULT '',
  url           VARCHAR(255)  NOT NULL DEFAULT '',
  description   LONGTEXT,
  timecreated   BIGINT(10)    NOT NULL DEFAULT 0,
  timemodified  BIGINT(10)    NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY mdl_user_use_uix (username),
  KEY mdl_user_del_ix (deleted),
  KEY mdl_user_tim_ix (timemodified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert Moodle admin
INSERT IGNORE INTO mdl_user
  (id, auth, confirmed, username, password, firstname, lastname, email, lang, timecreated, timemodified)
VALUES
  (1, 'manual', 1, 'admin', 'notused', 'Admin', 'Lakehouse', 'admin@universidad.edu', 'es', UNIX_TIMESTAMP(), UNIX_TIMESTAMP()),
  (2, 'manual', 1, 'guest', 'notused', 'Guest', 'User', 'guest@universidad.edu', 'es', UNIX_TIMESTAMP(), UNIX_TIMESTAMP());

-- Course Categories
CREATE TABLE IF NOT EXISTS mdl_course_categories (
  id        BIGINT(10) NOT NULL AUTO_INCREMENT,
  name      VARCHAR(255) NOT NULL DEFAULT '',
  idnumber  VARCHAR(100) NOT NULL DEFAULT '',
  parent    BIGINT(10)   NOT NULL DEFAULT 0,
  sortorder BIGINT(10)   NOT NULL DEFAULT 0,
  visible   TINYINT(1)   NOT NULL DEFAULT 1,
  depth     BIGINT(10)   NOT NULL DEFAULT 0,
  path      VARCHAR(255) NOT NULL DEFAULT '',
  timemodified BIGINT(10) NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO mdl_course_categories (id, name, idnumber, parent, depth, path, timemodified)
VALUES
  (1, 'Medicina',             'MED',  0, 1, '/1',   UNIX_TIMESTAMP()),
  (2, 'Informática',          'INF',  0, 1, '/2',   UNIX_TIMESTAMP()),
  (3, 'Gestión de Negocios',  'GN',   0, 1, '/3',   UNIX_TIMESTAMP());

-- Courses
CREATE TABLE IF NOT EXISTS mdl_course (
  id            BIGINT(10)   NOT NULL AUTO_INCREMENT,
  category      BIGINT(10)   NOT NULL DEFAULT 0,
  fullname      VARCHAR(254) NOT NULL DEFAULT '',
  shortname     VARCHAR(255) NOT NULL DEFAULT '',
  idnumber      VARCHAR(100) NOT NULL DEFAULT '',
  summary       LONGTEXT,
  format        VARCHAR(21)  NOT NULL DEFAULT 'topics',
  showgrades    SMALLINT(4)  NOT NULL DEFAULT 1,
  visible       TINYINT(1)   NOT NULL DEFAULT 1,
  startdate     BIGINT(10)   NOT NULL DEFAULT 0,
  enddate       BIGINT(10)   NOT NULL DEFAULT 0,
  timecreated   BIGINT(10)   NOT NULL DEFAULT 0,
  timemodified  BIGINT(10)   NOT NULL DEFAULT 0,
  lang          VARCHAR(30)  NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY mdl_cour_sho_uix (shortname),
  KEY mdl_cour_cat_ix (category),
  KEY mdl_cour_tim_ix (timemodified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO mdl_course (id, category, fullname, shortname, format, timecreated, timemodified)
VALUES (1, 0, 'Sitio', 'SITE', 'site', UNIX_TIMESTAMP(), UNIX_TIMESTAMP());

-- Enrolment methods
CREATE TABLE IF NOT EXISTS mdl_enrol (
  id            BIGINT(10) NOT NULL AUTO_INCREMENT,
  enrol         VARCHAR(20) NOT NULL DEFAULT '',
  status        BIGINT(10)  NOT NULL DEFAULT 0,
  courseid      BIGINT(10)  NOT NULL,
  sortorder     BIGINT(10)  NOT NULL DEFAULT 0,
  timecreated   BIGINT(10)  NOT NULL DEFAULT 0,
  timemodified  BIGINT(10)  NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY mdl_enro_cou_ix (courseid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- User enrolments
CREATE TABLE IF NOT EXISTS mdl_user_enrolments (
  id            BIGINT(10) NOT NULL AUTO_INCREMENT,
  status        BIGINT(10) NOT NULL DEFAULT 0,
  enrolid       BIGINT(10) NOT NULL,
  userid        BIGINT(10) NOT NULL,
  timestart     BIGINT(10) NOT NULL DEFAULT 0,
  timeend       BIGINT(10) NOT NULL DEFAULT 0,
  modifierid    BIGINT(10) NOT NULL DEFAULT 0,
  timecreated   BIGINT(10) NOT NULL DEFAULT 0,
  timemodified  BIGINT(10) NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY mdl_userenro_enruse_uix (enrolid, userid),
  KEY mdl_userenro_use_ix (userid),
  KEY mdl_userenro_tim_ix (timemodified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Grade items (per course)
CREATE TABLE IF NOT EXISTS mdl_grade_items (
  id            BIGINT(10) NOT NULL AUTO_INCREMENT,
  courseid      BIGINT(10),
  categoryid    BIGINT(10),
  itemname      VARCHAR(255),
  itemtype      VARCHAR(30) NOT NULL DEFAULT '',
  itemmodule    VARCHAR(30),
  iteminfo      LONGTEXT,
  idnumber      VARCHAR(255),
  gradetype     SMALLINT(4) NOT NULL DEFAULT 1,
  grademax      DECIMAL(10,5) NOT NULL DEFAULT 100.00000,
  grademin      DECIMAL(10,5) NOT NULL DEFAULT 0.00000,
  gradepass     DECIMAL(10,5) NOT NULL DEFAULT 0.00000,
  mult          DECIMAL(10,5) NOT NULL DEFAULT 1.00000,
  locked        BIGINT(10)  NOT NULL DEFAULT 0,
  timecreated   BIGINT(10),
  timemodified  BIGINT(10),
  PRIMARY KEY (id),
  KEY mdl_graditem_cou_ix (courseid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Grade grades (actual student grades)
CREATE TABLE IF NOT EXISTS mdl_grade_grades (
  id            BIGINT(10)    NOT NULL AUTO_INCREMENT,
  itemid        BIGINT(10)    NOT NULL,
  userid        BIGINT(10)    NOT NULL,
  rawgrade      DECIMAL(10,5),
  rawgrademax   DECIMAL(10,5) NOT NULL DEFAULT 100.00000,
  rawgrademin   DECIMAL(10,5) NOT NULL DEFAULT 0.00000,
  finalgrade    DECIMAL(10,5),
  hidden        BIGINT(10)    NOT NULL DEFAULT 0,
  locked        BIGINT(10)    NOT NULL DEFAULT 0,
  timecreated   BIGINT(10),
  timemodified  BIGINT(10),
  PRIMARY KEY (id),
  UNIQUE KEY mdl_gradgrad_iteuse_uix (itemid, userid),
  KEY mdl_gradgrad_use_ix (userid),
  KEY mdl_gradgrad_tim_ix (timemodified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Course completions
CREATE TABLE IF NOT EXISTS mdl_course_completions (
  id              BIGINT(10) NOT NULL AUTO_INCREMENT,
  userid          BIGINT(10) NOT NULL DEFAULT 0,
  course          BIGINT(10) NOT NULL DEFAULT 0,
  timeenrolled    BIGINT(10) NOT NULL DEFAULT 0,
  timestarted     BIGINT(10) NOT NULL DEFAULT 0,
  timecompleted   BIGINT(10),
  reaggregate     BIGINT(10) NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY mdl_courcomp_usecou_uix (userid, course),
  KEY mdl_courcomp_cou_ix (course)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Forum posts
CREATE TABLE IF NOT EXISTS mdl_forum_posts (
  id            BIGINT(10)   NOT NULL AUTO_INCREMENT,
  discussion    BIGINT(10)   NOT NULL DEFAULT 0,
  parent        BIGINT(10)   NOT NULL DEFAULT 0,
  userid        BIGINT(10)   NOT NULL DEFAULT 0,
  created       BIGINT(10)   NOT NULL DEFAULT 0,
  modified      BIGINT(10)   NOT NULL DEFAULT 0,
  mailed        TINYINT(2)   NOT NULL DEFAULT 0,
  subject       VARCHAR(255) NOT NULL DEFAULT '',
  message       LONGTEXT     NOT NULL,
  PRIMARY KEY (id),
  KEY mdl_forupost_use_ix (userid),
  KEY mdl_forupost_cre_ix (created)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Quiz attempts
CREATE TABLE IF NOT EXISTS mdl_quiz_attempts (
  id            BIGINT(10)  NOT NULL AUTO_INCREMENT,
  quiz          BIGINT(10)  NOT NULL DEFAULT 0,
  userid        BIGINT(10)  NOT NULL DEFAULT 0,
  attempt       MEDIUMINT(6) NOT NULL DEFAULT 0,
  sumgrades     DECIMAL(10,5),
  state         VARCHAR(16)  NOT NULL DEFAULT 'inprogress',
  timestart     BIGINT(10)  NOT NULL DEFAULT 0,
  timefinish    BIGINT(10)  NOT NULL DEFAULT 0,
  timemodified  BIGINT(10)  NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY mdl_quizatte_use_ix (userid),
  KEY mdl_quizatte_tim_ix (timemodified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
