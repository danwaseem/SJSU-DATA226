# ================================================================
# Lab 2: Stock Analytics ELT + dbt Orchestration DAG
#
# This DAG:
#   1. Runs `dbt deps`
#   2. Runs `dbt run` (models: staging + marts)
#   3. Runs `dbt snapshot`
#   4. Runs `dbt test`
#
# Requirements from professor:
#   - ELT done in dbt (views/tables in ANALYTICS schema)
#   - dbt scheduled from Airflow
#   - Snapshots + data tests included
#   - Uses proper Airflow DAG + scheduling
# ================================================================

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def dbt_bash_command(dbt_subcommand: str) -> str:
    """
    Build a robust bash command that:
      - cd's into the dbt project directory
      - Locates dbt binary (local or on PATH), or falls back to `python -m dbt.cli.main`
      - Executes the provided subcommand with --profiles-dir
    """
    return f"""
    cd {DBT_PROJECT_DIR}
    export DBT_PROFILES_DIR={DBT_PROFILES_DIR}

    if [ -x /home/airflow/.local/bin/dbt ]; then
      DBT_BIN=/home/airflow/.local/bin/dbt
    elif command -v dbt >/dev/null 2>&1; then
      DBT_BIN="$(command -v dbt)"
    else
      DBT_BIN="python -m dbt.cli.main"
    fi

    $DBT_BIN {dbt_subcommand} --profiles-dir "$DBT_PROFILES_DIR"
    """.strip()


with DAG(
    dag_id="stock_elt_dbt",
    start_date=datetime(2025, 10, 5),
    schedule=None, 
    catchup=False,
    tags=["ELT", "dbt", "StockAnalytics"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_bash_command("deps"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_bash_command("run --target prod"),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_bash_command("snapshot --target prod"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_bash_command("test --target prod"),
    )

    dbt_deps >> dbt_run >> dbt_snapshot >> dbt_test
