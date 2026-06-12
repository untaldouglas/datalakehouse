# ============================================================
# Universidad Data Lakehouse - Makefile
# Usage: make <target>
# ============================================================

.PHONY: help up down restart logs status seed seed-moodle setup-dremio \
        open-moodle open-airflow open-minio open-dremio open-metabase \
        clean reset lint check-env

# Load .env if it exists
-include .env
export

COMPOSE = docker compose
COMPOSE_FILE = docker-compose.yml

## help: Show this help message
help:
	@echo ""
	@echo "  Universidad Data Lakehouse"
	@echo "  =========================="
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
	@echo ""

## check-env: Verify .env file exists and has required keys
check-env:
	@test -f .env || (echo "ERROR: .env file not found. Run: cp .env.example .env" && exit 1)
	@echo "✓ .env file found"

## up: Start all services (first time takes 5-10 min for Moodle install)
up: check-env
	@echo "→ Starting Universidad Data Lakehouse..."
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --build
	@echo ""
	@echo "Services starting. Use 'make status' to monitor health."
	@echo ""
	@echo "  Moodle:   http://localhost:$(MOODLE_PORT)    (admin / $(MOODLE_ADMIN_PASSWORD))"
	@echo "  Airflow:  http://localhost:$(AIRFLOW_PORT)   (admin / $(AIRFLOW_ADMIN_PASSWORD))"
	@echo "  MinIO:    http://localhost:$(MINIO_CONSOLE_PORT)  ($(MINIO_ROOT_USER) / $(MINIO_ROOT_PASSWORD))"
	@echo "  Dremio:   http://localhost:$(DREMIO_UI_PORT)  ($(DREMIO_ADMIN_USER) / $(DREMIO_ADMIN_PASSWORD))"
	@echo "  Metabase: http://localhost:$(METABASE_PORT)"
	@echo ""

## up-core: Start only lakehouse core (MinIO + Nessie + Dremio) without source systems
up-core: check-env
	$(COMPOSE) -f $(COMPOSE_FILE) up -d minio minio-setup nessie dremio

## down: Stop all services (preserves data volumes)
down:
	$(COMPOSE) -f $(COMPOSE_FILE) down

## restart: Restart all services
restart: down up

## restart-service: Restart a specific service (usage: make restart-service s=airflow-scheduler)
restart-service:
	$(COMPOSE) -f $(COMPOSE_FILE) restart $(s)

## logs: Follow logs from all services
logs:
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=100

## logs-service: Follow logs from specific service (usage: make logs-service s=airflow-scheduler)
logs-service:
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200 $(s)

## status: Show health status of all containers
status:
	@$(COMPOSE) -f $(COMPOSE_FILE) ps

## seed: Populate Moodle and ERPNext with university test data (5000 students)
seed:
	@echo "→ Running data seeder..."
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm data-seeder python /opt/airflow/scripts/seed_data.py
	@echo "✓ Seed data loaded"

## seed-moodle: Seed only Moodle (run after Moodle finishes its initial install)
seed-moodle:
	@echo "→ Seeding Moodle data (requires Moodle install to be complete)..."
	$(COMPOSE) -f $(COMPOSE_FILE) exec moodle-db mysql \
	  -u$(MOODLE_DB_USER) -p$(MOODLE_DB_PASSWORD) $(MOODLE_DB_NAME) \
	  -e "SHOW TABLES LIKE 'mdl_user';"
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm data-seeder python /opt/airflow/scripts/seed_data.py

## setup-dremio: Configure Dremio sources and virtual datasets
setup-dremio:
	@echo "→ Configuring Dremio..."
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm \
	  -e DREMIO_HOST=http://dremio:9047 \
	  -e DREMIO_ADMIN_USER=$(DREMIO_ADMIN_USER) \
	  -e DREMIO_ADMIN_PASSWORD=$(DREMIO_ADMIN_PASSWORD) \
	  data-seeder python /opt/airflow/scripts/setup_dremio.py
	@echo "✓ Dremio configured"

## trigger-etl: Manually trigger all ETL DAGs
trigger-etl:
	@echo "→ Triggering ETL pipeline..."
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-scheduler \
	  airflow dags trigger 01_moodle_to_bronze
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-scheduler \
	  airflow dags trigger 02_erpnext_to_bronze
	@echo "✓ ETL DAGs triggered (Silver and Gold will run automatically)"

## trigger-full-etl: Trigger all 4 DAGs in sequence (development/testing)
trigger-full-etl:
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-scheduler airflow dags trigger 01_moodle_to_bronze
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-scheduler airflow dags trigger 02_erpnext_to_bronze
	sleep 30
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-scheduler airflow dags trigger 03_bronze_to_silver
	sleep 30
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-scheduler airflow dags trigger 04_silver_to_gold

## open-moodle: Open Moodle in browser
open-moodle:
	open http://localhost:$(MOODLE_PORT) 2>/dev/null || xdg-open http://localhost:$(MOODLE_PORT)

## open-airflow: Open Airflow in browser
open-airflow:
	open http://localhost:$(AIRFLOW_PORT) 2>/dev/null || xdg-open http://localhost:$(AIRFLOW_PORT)

## open-minio: Open MinIO console in browser
open-minio:
	open http://localhost:$(MINIO_CONSOLE_PORT) 2>/dev/null || xdg-open http://localhost:$(MINIO_CONSOLE_PORT)

## open-dremio: Open Dremio in browser
open-dremio:
	open http://localhost:$(DREMIO_UI_PORT) 2>/dev/null || xdg-open http://localhost:$(DREMIO_UI_PORT)

## open-metabase: Open Metabase in browser
open-metabase:
	open http://localhost:$(METABASE_PORT) 2>/dev/null || xdg-open http://localhost:$(METABASE_PORT)

## open-all: Open all UIs in browser
open-all: open-moodle open-airflow open-minio open-dremio open-metabase

## nessie-branches: List Nessie branches
nessie-branches:
	curl -s http://localhost:$(NESSIE_PORT)/api/v2/trees | python3 -m json.tool

## nessie-tables: List Iceberg tables in Nessie
nessie-tables:
	curl -s http://localhost:$(NESSIE_PORT)/api/v2/trees/main/entries | python3 -m json.tool

## minio-ls: List MinIO buckets and objects
minio-ls:
	$(COMPOSE) -f $(COMPOSE_FILE) exec minio mc ls local/

## clean: Stop services and remove volumes (DESTROYS ALL DATA)
clean:
	@echo "WARNING: This will delete ALL data including databases and lakehouse."
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ]
	$(COMPOSE) -f $(COMPOSE_FILE) down -v --remove-orphans
	@echo "✓ All data removed"

## reset: Full reset - clean + rebuild + start (DESTROYS ALL DATA)
reset: clean up

## ps: Alias for status
ps: status

## lint: Check Python code in dags/ and scripts/
lint:
	@which ruff >/dev/null 2>&1 || pip install ruff -q
	ruff check pipelines/dags/ pipelines/scripts/ --select E,W,F --ignore E501
	@echo "✓ Lint passed"

## build: Build/rebuild Docker images without starting
build:
	$(COMPOSE) -f $(COMPOSE_FILE) build
