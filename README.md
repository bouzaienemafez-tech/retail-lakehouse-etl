# Retail Lakehouse ETL

A production-inspired **medallion architecture** project that transforms retail events into reliable customer and product metrics. It is deliberately self-contained, so a recruiter can run it without cloud credentials or private data.

## Why it stands out

- Uses a clear **Bronze → Silver → Gold** design rather than a single cleaning script.
- Enforces a data contract and records rejected rows instead of silently dropping bad data.
- Produces decision-ready monthly revenue and customer metrics.
- Includes a standard-library test suite and an architecture suitable for Airflow, dbt, Spark, or a cloud warehouse later.

## Data flow

```
Raw JSON events → Bronze immutable copy → Silver validated records → Gold KPI tables
```

| Layer | Purpose | Output |
| --- | --- | --- |
| Bronze | Preserve source fidelity | Timestamped JSON lines |
| Silver | Validate types, required fields and business rules | Clean order facts + rejection log |
| Gold | Aggregate for BI and reporting | Monthly product and customer metrics |

## Run it

```bash
python3 src/pipeline.py
python3 -m unittest discover -s tests
```

The pipeline generates source events, runs the three layers, and writes outputs under `data/`. Generated outputs are ignored by Git.

## Production roadmap

1. Orchestrate daily loads with Airflow or Prefect.
2. Store Bronze data in S3 or Azure Data Lake and Silver/Gold tables in Snowflake or BigQuery.
3. Replace local checks with Great Expectations and alert on quality failures.
4. Serve the Gold layer to Power BI or Looker.

## Skills demonstrated

`Python` · `ETL` · `Medallion Architecture` · `Data Quality` · `Data Contracts` · `Incremental Processing` · `Analytics Engineering`
