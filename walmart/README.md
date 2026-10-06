# dbt project: `walmart`

This folder is the dbt project (`DBT_PROJECT_DIR`). It is documented as part of the pipeline in
the [repository README](../README.md), which covers every layer, the Airflow DAGs that run it, and
the known issues.

Quick reference:

```bash
cd walmart
export DBT_PROJECT_DIR=$PWD DBT_PROFILES_DIR=$PWD

dbt source freshness
dbt run  --select silver_tech
dbt test --select silver_tech
dbt run  --select silver_biz
dbt test --select silver_biz
dbt run  --select gold/ephemeral
dbt snapshot
dbt run  --select gold/fact
dbt docs generate
```

| Path | Contents |
|---|---|
| `models/source/sources.yml` | the `walmart.bronze` source |
| `models/silver_tech/` | 6 incremental models, `properties.yml` with the key tests |
| `models/silver_biz/obt_biz.sql` | the business wide table, built from a Jinja config list |
| `models/gold/ephemeral/` | 5 inlined models that feed the snapshots |
| `models/gold/fact/fact_orders.sql` | the star-schema fact |
| `snapshots/` | 5 SCD2 snapshots, timestamp strategy |
| `macros/custom_schema.sql` | schema naming without dbt's target-schema prefix |
| `tests/test_obt.sql` | singular test for NULL surrogate keys in `obt_biz` |

`profiles.yml` is git-ignored and reads the Databricks host, HTTP path and token from environment
variables. See the root README for the full variable list.
