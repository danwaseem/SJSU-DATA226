# ================================================================
# Lab 1: Stock Price Prediction (AMZN & META)
#
# DAG 1: ETL_DAG
#   - Fetch 180 days of daily OHLCV data from yfinance
#   - Load into USER_DB_HEDGEHOG.RAW.MARKET_DATA
#
# DAG 2: Stock_Forecasting_DAG
#   - Verify ETL data exists
#   - Train Snowflake ML Forecast model
#   - Forecast 14 days ahead
#   - Union history + forecast into COMBINED table
#
# Both DAGs:
#   - Use SnowflakeHook connection 'snowflake_conn'
#   - Use SQL transactions with BEGIN / COMMIT / ROLLBACK
# ================================================================

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import yfinance as yf
import pandas as pd

DB = "USER_DB_HEDGEHOG" 


# -----------------------------------------------------------------
# Helper: get Snowflake cursor from Airflow Connection
# -----------------------------------------------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


# ================================================================
# DAG 1: ETL_DAG
# ================================================================

@task
def extract_finance(tickers, days: int = 180):
    """
    Fetch N days of daily OHLCV data for all tickers from yfinance.
    """
    rows = []
    for sym in tickers:
        df = yf.Ticker(sym).history(
            period=f"{days}d",
            interval="1d",
            auto_adjust=False,
        )
        if df is None or df.empty:
            continue

        # Sometimes 'Close' is missing; fallback to 'Adj Close'
        if "Close" not in df.columns and "Adj Close" in df.columns:
            df["Close"] = df["Adj Close"]

        needed = ["Open", "High", "Low", "Close", "Volume"]
        if not all(col in df.columns for col in needed):
            continue

        df = df[needed].dropna(subset=needed)

        for dt, r in df.iterrows():
            rows.append(
                {
                    "symbol": sym,
                    "date": pd.to_datetime(dt).strftime("%Y-%m-%d"),
                    "open": float(r["Open"]),
                    "high": float(r["High"]),
                    "low": float(r["Low"]),
                    "close": float(r["Close"]),
                    "volume": 0 if pd.isna(r["Volume"]) else int(r["Volume"]),
                }
            )
    return rows


@task
def transform_finance(raw_rows):
    """
    Basic cleaning: drop missing / negative values.
    """
    cleaned = []
    for r in raw_rows:
        required_keys = ("symbol", "date", "open", "high", "low", "close", "volume")
        if not all(k in r for k in required_keys):
            continue
        if any(r[k] is None for k in required_keys):
            continue
        if r["volume"] < 0:
            continue
        cleaned.append(r)
    return cleaned


@task
def load_to_snowflake(records, target_table: str):
    """
    Create RAW table if not exists, delete existing rows for symbols
    (idempotent), then insert new data. Uses transactions.
    """
    cur = return_snowflake_conn()
    try:
        # Ensure table exists
        cur.execute("BEGIN;")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                DATE       DATE            NOT NULL,
                OPEN       FLOAT           NOT NULL,
                HIGH       FLOAT           NOT NULL,
                LOW        FLOAT           NOT NULL,
                CLOSE      FLOAT           NOT NULL,
                VOLUME     BIGINT          NOT NULL,
                SYMBOL     VARCHAR(10)     NOT NULL,
                CREATED_AT TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
            );
            """
        )
        cur.execute("COMMIT;")

        if not records:
            return

        symbols = sorted({r["symbol"] for r in records})
        sym_list = ",".join(f"'{s}'" for s in symbols)

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table} WHERE SYMBOL IN ({sym_list});")

        for r in records:
            cur.execute(
                f"""
                INSERT INTO {target_table}
                    (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL)
                VALUES (
                    TO_DATE(%(date)s, 'YYYY-MM-DD'),
                    %(open)s, %(high)s, %(low)s,
                    %(close)s, %(volume)s, %(symbol)s
                );
                """,
                r,
            )

        cur.execute("COMMIT;")
    except Exception:  # noqa: BLE001
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id="ETL_DAG1",
    start_date=datetime(2025, 10, 5),
    schedule="30 2 * * *",  # 02:30 AM
    catchup=False,
    tags=["ETL", "StockAPI"],
) as etl_dag:
    tickers = ["META", "AMZN"]
    target_table = f"{DB}.RAW.MARKET_DATA"

    extracted = extract_finance(tickers)
    transformed = transform_finance(extracted)
    loaded = load_to_snowflake(transformed, target_table)

    # Trigger forecasting DAG when ETL finishes successfully
    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_stock_etl_dag",
        trigger_dag_id="Stock_ETL",   # must match forecast_dag dag_id
        wait_for_completion=True,    # or True if you want ETL_DAG1 to wait
        reset_dag_run=True,
    )

    extracted >> transformed >> loaded >> trigger_forecast


# ================================================================
# DAG 2: Stock_Forecasting_DAG
# ================================================================

@task
def verify_etl(target_table: str):
    """
    Ensure ETL table has data before forecasting.
    """
    cur = return_snowflake_conn()
    result = cur.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
    if result == 0:
        raise ValueError(f"No data in {target_table}. Run ETL_DAG first.")


@task
def train_model(target_table: str, view_name: str, model_name: str, tickers):
    """
    Create training view and Snowflake ML Forecast model.
    """
    cur = return_snowflake_conn()
    sym_list = ",".join(f"'{s}'" for s in tickers)

    try:
        cur.execute("BEGIN;")

        cur.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT
                TO_TIMESTAMP_NTZ(DATE) AS DATE_v1,
                CLOSE,
                SYMBOL
            FROM {target_table}
            WHERE SYMBOL IN ({sym_list});
            """
        )

        cur.execute(
            f"""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name}(
                INPUT_DATA         => SYSTEM$REFERENCE('VIEW', '{view_name}'),
                SERIES_COLNAME     => 'SYMBOL',
                TIMESTAMP_COLNAME  => 'DATE_v1',
                TARGET_COLNAME     => 'CLOSE',
                CONFIG_OBJECT      => {{ 'ON_ERROR': 'SKIP' }}
            );
            """
        )

        cur.execute("COMMIT;")
    except Exception:  # noqa: BLE001
        cur.execute("ROLLBACK;")
        raise


@task
def predict_and_combine(
    model_name: str,
    forecast_table: str,
    combined_table: str,
    target_table: str,
    tickers,
):
    """
    Run FORECAST, materialize results, and union with history.
    """
    cur = return_snowflake_conn()
    view_clean = f"{DB}.RAW.MARKET_DATA_FORECAST_CLEAN"
    sym_list = ",".join(f"'{s}'" for s in tickers)

    try:
        # Run forecast and capture RESULT_SCAN into table
        cur.execute(
            f"""
            BEGIN
                CALL {model_name}!FORECAST(
                    FORECASTING_PERIODS => 14,
                    CONFIG_OBJECT       => {{ 'prediction_interval': 0.95 }}
                );
                LET x := SQLID;
                CREATE OR REPLACE TABLE {forecast_table} AS
                SELECT * FROM TABLE(RESULT_SCAN(:x));
            END;
            """
        )

        # Clean forecast view
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW {view_clean} AS
            SELECT
                REPLACE(series, '"','') AS SYMBOL,
                CAST(ts AS TIMESTAMP_NTZ) AS DATE_v1,
                forecast,
                lower_bound,
                upper_bound
            FROM {forecast_table};
            """
        )

        # Union history + forecast
        cur.execute(
            f"""
            CREATE OR REPLACE TABLE {combined_table} AS
            SELECT
                SYMBOL,
                DATE,
                CLOSE AS ACTUAL,
                NULL AS FORECAST,
                NULL AS LOWER_BOUND,
                NULL AS UPPER_BOUND
            FROM {target_table}
            WHERE SYMBOL IN ({sym_list})

            UNION ALL

            SELECT
                SYMBOL,
                CAST(DATE_v1 AS DATE),
                NULL AS ACTUAL,
                FORECAST,
                LOWER_BOUND,
                UPPER_BOUND
            FROM {view_clean};
            """
        )
    except Exception:  # noqa: BLE001
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id="Stock_ETL",
    start_date=datetime(2025, 10, 5),
    schedule=None,  # will be triggered by ETL_DAG1
    catchup=False,
    tags=["Forecast", "SnowflakeML"],
) as forecast_dag:
    tickers = ["META", "AMZN"]
    target_table = f"{DB}.RAW.MARKET_DATA"
    view_name = f"{DB}.RAW.MARKET_DATA_v1"
    model_name = f"{DB}.RAW.STOCK_PRICE_MODEL"
    forecast_table = f"{DB}.RAW.MARKET_DATA_FORECAST_RAW"
    combined_table = f"{DB}.RAW.MARKET_DATA_COMBINED"

    v = verify_etl(target_table)
    t = train_model(target_table, view_name, model_name, tickers)
    p = predict_and_combine(model_name, forecast_table, combined_table, target_table, tickers)

    # Trigger dbt DAG after forecasting is done
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_stock_elt_dbt",
        trigger_dag_id="stock_elt_dbt",
        wait_for_completion=True,   # or True if you want Stock_ETL to wait
        reset_dag_run=True,
    )

    v >> t >> p >> trigger_dbt

