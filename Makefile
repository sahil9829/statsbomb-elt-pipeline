# Common workflows. Local targets need Python 3.11 + Java 17 (see requirements-dev.txt).
LAKE_ROOT      ?= ./data_lake
COMPETITIONS   ?= 43:3
MAX_MATCHES    ?= 8
TF_ENV         ?= dev

export DBT_PROFILES_DIR   := $(CURDIR)/dbt
export DBT_PROCESSED_ROOT := $(abspath $(LAKE_ROOT))/processed/statsbomb

.PHONY: help install lint test extract transform dbt-deps dbt-build dbt-docs pipeline \
        up down tf-init tf-plan tf-apply clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

install:  ## Install dev dependencies into the active virtualenv
	pip install -r requirements-dev.txt

lint:  ## flake8 + terraform fmt check
	flake8 . --count --statistics
	terraform -chdir=terraform fmt -check -recursive

test:  ## Unit tests (extractor + PySpark)
	pytest -v

extract:  ## Land raw JSON in $(LAKE_ROOT)
	python scripts/extract_statsbomb.py --lake-root $(LAKE_ROOT) --competitions $(COMPETITIONS) --max-matches $(MAX_MATCHES)

transform:  ## Run the PySpark job locally
	python spark/jobs/statsbomb_transform.py --lake-root $(LAKE_ROOT)

dbt-deps:
	cd dbt && dbt deps

dbt-build: dbt-deps  ## Build + test the star schema on DuckDB
	cd dbt && dbt build --target duckdb

dbt-docs:  ## Generate and serve dbt docs (lineage graph) on :8081
	cd dbt && dbt docs generate --target duckdb && dbt docs serve --port 8081

pipeline: extract transform dbt-build  ## Full local run: extract -> Spark -> dbt

up:  ## Start Airflow (http://localhost:8080, airflow/airflow)
	mkdir -p logs plugins data_lake
	docker compose up airflow-init
	docker compose up -d

down:
	docker compose down

tf-init:  ## terraform init with environments/$(TF_ENV).s3.tfbackend
	terraform -chdir=terraform init -backend-config=environments/$(TF_ENV).s3.tfbackend

tf-plan:  ## terraform plan for $(TF_ENV)
	terraform -chdir=terraform plan -var-file=environments/$(TF_ENV).tfvars -out=$(TF_ENV).tfplan

tf-apply:  ## Apply the saved plan
	terraform -chdir=terraform apply $(TF_ENV).tfplan

clean:  ## Remove local data and build artefacts
	rm -rf data_lake dbt/target dbt/dbt_packages dbt/logs dbt/*.duckdb spark-warehouse metastore_db
