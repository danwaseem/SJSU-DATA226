# ================================================================
# Project: Alpha Vantage to Snowflake ETL Pipeline
#
# Description:
# This Airflow DAG connects to the Alpha Vantage stock market API,
# fetches the last 90 days of daily price data for a given symbol,
# and loads it into a Snowflake table.
#
# Requirements:
# • Store your Alpha Vantage API key securely as an Airflow Variable:
#       alpha_api_key
#
# Author: Danish Waseem
# Course: DATA226 - HW 5
# ================================================================

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import snowflake.connector
import requests


# ----------------------------------------------------------------
# Helper Function: return_snowflake_conn()
# ----------------------------------------------------------------
# Creates a connection to Snowflake using Airflow's SnowflakeHook.
# The connection ID 'snowflake_conn' must be configured in Airflow.
# Returns a cursor object to execute SQL queries.
# ----------------------------------------------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


# ----------------------------------------------------------------
# Task 1: fetch_last_90d()
# ----------------------------------------------------------------
# Fetches the last 90 days of daily OHLCV (Open, High, Low, Close,
# Volume) data for a specified stock symbol from Alpha Vantage API.
#
# Parameters:
#   symbol (str): Stock ticker symbol (e.g., 'ELV', 'AAPL')
#
# Returns:
#   List of dictionaries with stock price data for each date.
# ----------------------------------------------------------------
@task
def fetch_last_90d(symbol):
    # Get the API key securely from Airflow Variables
    api_key = Variable.get('alpha_api_key')

    # Alpha Vantage endpoint for daily time series
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"

    # Make GET request to the API
    r = requests.get(url)
    data = r.json()

    # Calculate the cutoff date (90 days ago from today)
    cutoff = datetime.today().date() - timedelta(days=90)
    results = []

    # Parse the API JSON response
    for d in data["Time Series (Daily)"]:
        trade_date = datetime.strptime(d, "%Y-%m-%d").date()
        if trade_date >= cutoff:
            stock_info = data["Time Series (Daily)"][d]
            stock_info["date"] = d  # add date key for reference
            results.append(stock_info)

    return results


# ----------------------------------------------------------------
# Task 2: load_to_snowflake()
# ----------------------------------------------------------------
# Creates a target table in Snowflake (if it doesn’t exist) and
# inserts the 90-day Alpha Vantage data into it.
#
# The table name used: RAW.STOCK_API
#
# Parameters:
#   cur (cursor): Active Snowflake cursor connection
#   records (list): List of daily stock data from fetch_last_90d()
#   symbol (str): Stock ticker symbol (e.g., 'ELV')
#
# Uses SQL transactions (BEGIN/COMMIT/ROLLBACK) to ensure atomicity.
# ----------------------------------------------------------------
@task
def load_to_snowflake(cur, records, symbol):
    target_table = "RAW.STOCK_API"  # destination table in Snowflake

    try:
        # Begin SQL transaction
        cur.execute("BEGIN;")

        # Create table if it does not already exist
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                open  NUMBER(18,4),
                close NUMBER(18,4),
                high  NUMBER(18,4),
                low   NUMBER(18,4),
                volume NUMBER(38,0),
                CONSTRAINT pk_symbol_date PRIMARY KEY (symbol, trade_date) NOT ENFORCED
            );
        """)

        # Delete existing records to refresh dataset (optional)
        cur.execute(f"DELETE FROM {target_table}")

        # Insert each record individually
        for r in records:
            trade_date = r["date"]
            open_ = r["1. open"]
            high_ = r["2. high"]
            low_ = r["3. low"]
            close_ = r["4. close"]
            volume_ = r["5. volume"]

            # Build INSERT SQL command
            insert_sql = f"""
                INSERT INTO {target_table}
                (symbol, trade_date, open, close, high, low, volume)
                VALUES (
                    '{symbol}',
                    TO_DATE('{trade_date}','YYYY-MM-DD'),
                    {open_}, {close_}, {high_}, {low_}, {volume_}
                );
            """
            cur.execute(insert_sql)

        # Commit the transaction if all inserts succeed
        cur.execute("COMMIT;")
        print(f"[SUCCESS] Loaded {len(records)} records for {symbol} into {target_table}")

    except Exception as e:
        # Roll back in case of any failure
        cur.execute("ROLLBACK;")
        print(f"[ERROR] Failed to load data for {symbol}: {e}")
        raise


# ----------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------
# DAG: AlphaVantage_to_Snowflake
# Description: Fetches 90 days of stock data for 'ELV' and loads
# it into Snowflake's RAW.STOCK_API table.
#
# Schedule: Runs daily at 2:30 AM UTC
# ----------------------------------------------------------------
with DAG(
    dag_id='AlphaVantage_to_Snowflake',
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=['ETL', 'StockAPI'],
    schedule='30 2 * * *'  # cron format (02:30 UTC)
) as dag:

    # Stock symbol to load
    symbol = "ELV"

    # Get a Snowflake connection cursor
    cur = return_snowflake_conn()

    # Define the Airflow task flow
    fetch_task = fetch_last_90d(symbol)
    load_task = load_to_snowflake(cur, fetch_task, symbol)

    # Task dependency chain
    fetch_task >> load_task
