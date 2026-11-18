# Sports Analytics ELT Pipeline ⚽️

An end-to-end ELT pipeline that ingests [StatsBomb open data](https://github.com/statsbomb/open-data), transforms it with **PySpark**, models it into a tested **dbt star schema**, and serves it through **AWS Athena**. All the infrastructure is defined in **Terraform**, and **Apache Airflow** orchestrates the runs.

The whole pipeline also runs **locally with no AWS account**: Spark reads and writes a local folder, and dbt targets DuckDB. CI uses this to run the full pipeline on real data for every PR.

## 🏗️ Architecture

```mermaid
flowchart LR
    API[StatsBomb open data<br/>GitHub JSON] -->|extract_statsbomb.py| RAW[(S3 raw/<br/>JSON)]
    RAW -->|PySpark<br/>Glue 5.0 or local| PROC[(S3 processed/<br/>Parquet)]
    PROC -.registered by Terraform.-> GLUE[Glue Data Catalog]
    GLUE --> DBT[dbt on Athena<br/>staging → intermediate → marts]
    DBT --> MARTS[(Star schema<br/>Iceberg tables)]
    MARTS --> ATHENA[Athena workgroup<br/>SQL / BI]
    AF{{Airflow}} -.orchestrates.-> API & RAW & DBT
```

| Layer | Tech | What it does |
|---|---|---|
| Orchestration | Airflow 2.10 (Docker) | `extract_raw → spark_transform → dbt.deps → dbt.source_freshness → dbt.build` |
| Extract | Python, requests, boto3 | Competitions, matches, events and lineups for configurable competition-seasons. Incremental, retried, parallel. |
| Transform | PySpark 3.5 | Explicit schemas, flattens nested JSON, primary-key quality gates, typed Parquet. Runs on **AWS Glue** or `spark-submit`. |
| Model | dbt 1.11 (Athena / DuckDB) | Staging views, intermediate views, star-schema marts, 140+ data tests, unit tests |
| Infra | Terraform (AWS provider 6) | S3, Glue Catalog and job, Athena, IAM, CloudWatch |
| Serving | Athena engine v3 | Workgroup with a per-query scan limit and saved example queries |

## ⭐ Star schema

```mermaid
erDiagram
    fct_events }o--|| dim_date : match_date_key
    fct_events }o--|| dim_competition : competition_key
    fct_events }o--|| dim_team : "team_key / opponent_team_key"
    fct_events }o--|| dim_player : player_key
    fct_events }o--|| dim_event_type : event_type_key
    fct_events }o--|| dim_play_pattern : play_pattern_key

    fct_player_match }o--|| dim_date : match_date_key
    fct_player_match }o--|| dim_competition : competition_key
    fct_player_match }o--|| dim_player : player_key
    fct_player_match }o--|| dim_team : "team_key / opponent_team_key"

    fct_matches }o--|| dim_date : match_date_key
    fct_matches }o--|| dim_competition : competition_key
    fct_matches }o--|| dim_team : "home_team_key / away_team_key"
    fct_matches }o--|| dim_venue : venue_key
    fct_matches }o--|| dim_referee : referee_key
```

| Fact | Grain | Example measures |
|---|---|---|
| `fct_events` | one on-pitch event | location, end location, xG, shot distance, `is_pass` / `is_goal` / `is_key_pass` (0/1, additive) |
| `fct_player_match` | player × match (full squad) | minutes played, passes, pass completion, shots, goals, xG, assists, tackles, pressures, cards |
| `fct_matches` | match | score, result, team shots / xG / passes, extra time / penalties flags |

Design choices:
* **Surrogate keys** come from `dbt_utils.generate_surrogate_key`. Natural ids stay on every dimension.
* **Unknown members**: each dimension has an `Unknown` row (id `-1`), and the facts map missing keys to it. A fact foreign key is therefore never NULL, and every join is a safe inner join.
* **Conformed dimensions**: `dim_date`, `dim_team`, `dim_player` and `dim_competition` are shared by all three facts.
* **Role-playing dimensions**: `dim_team` plays home, away, team and opponent.
* **Degenerate dimensions** such as `match_id`, `period` and `competition_stage_name` live on the facts.
* **Penalty shoot-outs** (period 5) are excluded from goals and xG, so the event-level goals match the published score. A singular test checks this.

## 🧪 Testing

| Where | What |
|---|---|
| `spark/tests` (pytest) | Each transformation runs on hand-built StatsBomb payloads. These tests cover flattening, nullable sub-objects, short coordinate arrays, minutes, cards and the quality gates. **Each output schema is asserted column-for-column against `contracts/processed_tables.json`**, the same file Terraform uses to create the Glue tables, so Spark and the catalog can't drift. |
| `tests/` (pytest) | Extractor (HTTP mocked): S3/local layout, incremental skip, `--force`, non-zero exit on failure. DAG integrity: imports cleanly, task order, runner switching. |
| dbt data tests | `unique` / `not_null` on every key, `relationships` on **every fact foreign key**, accepted values and ranges (xG 0–1, minutes 0–135), and a custom `pitch_coordinate` generic test |
| dbt singular tests | Score reconciles with goal events. Exactly 11 starters per side. xG agrees across the three fact grains. |
| dbt unit tests | Match result derivation, and minutes played for the final-whistle and unused-sub cases |
| CI | Lint, then unit tests, DAG checks, a **real end-to-end run** (extract → Spark → `dbt build` on DuckDB), `terraform fmt` + `validate`, and the image build |

## 🚀 Running it

### 1. Fully local (no AWS)

```bash
python3.11 -m venv .venv && source .venv/bin/activate
make install
make pipeline                       # extract 8 World Cup matches -> Spark -> dbt build
make pipeline MAX_MATCHES=0         # ...or the whole tournament (64 matches, ~230k events)
make dbt-docs                       # lineage graph at http://localhost:8081
```

The marts end up in `dbt/statsbomb.duckdb` (schema `statsbomb_marts`).

### 2. Airflow in Docker

```bash
cp .env.example .env                # defaults = local mode
make up                             # http://localhost:8080  (airflow / airflow)
```

Unpause and trigger `statsbomb_elt`.

### 3. On AWS

```bash
cp terraform/environments/dev.s3.tfbackend.example terraform/environments/dev.s3.tfbackend   # edit state bucket
make tf-init tf-plan tf-apply TF_ENV=dev
terraform -chdir=terraform output -raw dotenv >> .env      # Glue job, Athena, schema prefix ...
```

Then run the DAG with `SPARK_RUNNER=glue` and `DBT_TARGET=athena`. Use credentials that can assume `pipeline_role_arn`. Terraform never creates access keys.

Terraform provisions:
* **S3** lake, Athena-results and artifacts buckets: encrypted, public access blocked, TLS-only, with lifecycle rules. The lake bucket is versioned and moves raw data to Standard-IA after 30 days.
* **Glue Catalog** databases for the `processed`, `staging`, `intermediate` and `marts` layers, plus the processed tables generated from the schema contract.
* **Glue 5.0 job** that runs the exact same `spark/jobs/statsbomb_transform.py`, with continuous logging and the Spark UI enabled.
* **Athena** workgroup (engine v3, enforced results location, per-query scan cutoff) with saved example queries.
* **IAM**: a least-privilege Glue role (read `raw/`, write `processed/`) and a pipeline role/policy for Airflow.

## ⚙️ Configuration

| Variable | Default | |
|---|---|---|
| `STATSBOMB_COMPETITIONS` | `43:3` | `competition_id:season_id` pairs, comma separated |
| `STATSBOMB_MAX_MATCHES` | – | Cap per season (dev/CI) |
| `LAKE_ROOT` | `./data_lake` | Local path or `s3://bucket` |
| `SPARK_RUNNER` | `local` | `local` (spark-submit in the worker) or `glue` |
| `DBT_TARGET` | `duckdb` | `duckdb` or `athena` |
| `DBT_SCHEMA_PREFIX` | `statsbomb` | Must match the Glue database prefix (`terraform output`) |

## 📁 Layout

```
contracts/processed_tables.json   schema contract shared by Spark tests + Terraform
dags/statsbomb_elt_dag.py         Airflow DAG
scripts/extract_statsbomb.py      extract -> raw zone
spark/jobs/statsbomb_transform.py PySpark job (local or Glue)
spark/tests/                      PySpark unit tests
dbt/models/staging/               1:1 cleaned views over the processed tables
dbt/models/intermediate/          reusable joins and aggregations
dbt/models/marts/core/            dim_* and fct_* (star schema) + unit tests
dbt/tests/                        singular tests + custom generic test
terraform/                        AWS infrastructure
docker/airflow/                   Airflow image (Java, PySpark, dbt venv)
```

## Notes

* The Spark job does a **full refresh**: the raw zone keeps the complete history, so every run rebuilds the processed zone deterministically. A natural next step is to partition the events by competition-season and make `fct_events` an incremental Iceberg `merge`.
* Data is © StatsBomb, provided under their [open data licence](https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf). Credit StatsBomb if you publish analysis built on it.
