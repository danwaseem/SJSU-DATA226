# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def fetch_last_90d(symbol):
    api_key = Variable.get('alpha_api_key')
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    r = requests.get(url)
    data = r.json()

    from datetime import date
    cutoff = datetime.today().date() - timedelta(days=90)
    results = []

    for d in data["Time Series (Daily)"]:
        trade_date = datetime.strptime(d, "%Y-%m-%d").date()
        if trade_date >= cutoff:
            stock_info = data["Time Series (Daily)"][d]
            stock_info["date"] = d
            results.append(stock_info)
    return results


@task
def load_to_snowflake(cur, records, symbol):
    target_table = "RAW.STOCK_API"  # DB/schema come from Snowflake connection
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            symbol VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open  NUMBER(18,4),
            close NUMBER(18,4),
            high  NUMBER(18,4),
            low   NUMBER(18,4),
            volume NUMBER(38,0),
            CONSTRAINT pk_symbol_date PRIMARY KEY (symbol, trade_date) NOT ENFORCED
        );""")
        cur.execute(f"DELETE FROM {target_table}")

        for r in records:
            trade_date = r["date"]
            open_ = r["1. open"]
            high_ = r["2. high"]
            low_ = r["3. low"]
            close_ = r["4. close"]
            volume_ = r["5. volume"]

            insert_sql = f"""INSERT INTO {target_table}
                (symbol, trade_date, open, close, high, low, volume)
                VALUES ('{symbol}', TO_DATE('{trade_date}','YYYY-MM-DD'),
                        {open_}, {close_}, {high_}, {low_}, {volume_})"""
            cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise


with DAG(
    dag_id='AlphaVantage_to_Snowflake',
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=['ETL', 'StockAPI'],
    schedule='30 2 * * *'
) as dag:

    symbol = "ELV"
    cur = return_snowflake_conn()

    fetch_task = fetch_last_90d(symbol)
    load_task = load_to_snowflake(cur, fetch_task, symbol)

    fetch_task >> load_task