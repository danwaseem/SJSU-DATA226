# ELT DAG: Create session_summary joined table with duplicate check
# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages

from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

def return_snowflake_conn():
    """Return a Snowflake cursor using the configured connection."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(schema, table, select_sql, primary_key=None):
    """
    Create Table As Select (CTAS) with primary key uniqueness check.
    
    Args:
        schema: Target schema name
        table: Target table name
        select_sql: SQL query to populate the table
        primary_key: Column name to check for uniqueness (optional)
    """
    logging.info(f"Creating table: {schema}.{table}")
    logging.info(f"SQL Query: {select_sql}")
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        
        # Step 1: Create temporary table with the SELECT query
        temp_table_sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(f"Creating temp table: {temp_table_sql}")
        cur.execute(temp_table_sql)
        
        # Step 2: Check for duplicate records (primary key uniqueness)
        if primary_key is not None:
            logging.info(f"Checking primary key uniqueness for: {primary_key}")
            
            duplicate_check_sql = f"""
                SELECT {primary_key}, COUNT(1) AS cnt 
                FROM {schema}.temp_{table}
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 1
            """
            logging.info(f"Duplicate check SQL: {duplicate_check_sql}")
            cur.execute(duplicate_check_sql)
            result = cur.fetchone()
            
            if result:
                pk_value, count = result[0], int(result[1])
                logging.info(f"Max duplicate count: {count} for {primary_key}={pk_value}")
                
                if count > 1:
                    error_msg = f"Primary key uniqueness check failed! Found {count} records for {primary_key}={pk_value}"
                    logging.error(error_msg)
                    cur.execute("ROLLBACK;")
                    raise Exception(error_msg)
                else:
                    logging.info("Primary key uniqueness check passed!")
            else:
                logging.warning("No records found in temp table")
        
        # Step 3: Create main table if it doesn't exist (empty structure)
        main_table_creation_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;
        """
        logging.info("Creating main table structure if not exists")
        cur.execute(main_table_creation_sql)
        
        # Step 4: Swap temp table with main table
        swap_sql = f"ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"
        logging.info(f"Swapping tables: {swap_sql}")
        cur.execute(swap_sql)
        
        # Step 5: Get final row count
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
        row_count = cur.fetchone()[0]
        logging.info(f"Successfully created {schema}.{table} with {row_count} rows")
        
        cur.execute("COMMIT;")
        return f"Table {schema}.{table} created with {row_count} rows"
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error in CTAS operation: {e}")
        raise

with DAG(
    dag_id='ELT_Build_Session_Summary',
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=['ELT', 'Analytics'],
    schedule='45 2 * * *',  # Run daily at 2:45 AM (after ETL DAG)
    description='Create session_summary analytics table by joining user_session_channel and session_timestamp'
) as dag:
    
    # Define target schema and table
    schema = "analytics"
    table = "session_summary"
    
    # Define the SELECT query to join the two tables
    select_sql = """
        SELECT 
            u.userId,
            u.sessionId,
            u.channel,
            s.ts
        FROM raw.user_session_channel u
        JOIN raw.session_timestamp s 
            ON u.sessionId = s.sessionId
    """
    
    # Execute CTAS with primary key check (Extra point: duplicate check)
    run_ctas(schema, table, select_sql, primary_key='sessionId')