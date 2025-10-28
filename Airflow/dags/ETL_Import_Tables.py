# ETL DAG: Import user_session_channel and session_timestamp tables
# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

def return_snowflake_conn():
    """Return a Snowflake cursor using the configured connection."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_tables():
    """Create the raw tables if they don't exist."""
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        
        logging.info("Creating user_session_channel table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                userId int NOT NULL,
                sessionId varchar(32) PRIMARY KEY,
                channel varchar(32) DEFAULT 'direct'  
            );
        """)
        
        logging.info("Creating session_timestamp table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                sessionId varchar(32) PRIMARY KEY,
                ts timestamp  
            );
        """)
        
        cur.execute("COMMIT;")
        logging.info("Tables created successfully")
        return "Tables created"
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error creating tables: {e}")
        raise

@task
def create_stage():
    """Create external stage pointing to S3 bucket."""
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        
        logging.info("Creating external stage...")
        cur.execute("""
            CREATE OR REPLACE STAGE raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """)
        
        cur.execute("COMMIT;")
        logging.info("Stage created successfully")
        return "Stage created"
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error creating stage: {e}")
        raise

@task
def load_user_session_channel():
    """Load data into user_session_channel table from S3."""
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        
        logging.info("Loading user_session_channel data...")
        
        # Truncate table before loading
        cur.execute("TRUNCATE TABLE raw.user_session_channel;")
        
        # Copy data from S3
        cur.execute("""
            COPY INTO raw.user_session_channel
            FROM @raw.blob_stage/user_session_channel.csv;
        """)
        
        # Get row count
        cur.execute("SELECT COUNT(*) FROM raw.user_session_channel;")
        count = cur.fetchone()[0]
        logging.info(f"Loaded {count} rows into user_session_channel")
        
        cur.execute("COMMIT;")
        return f"Loaded {count} rows"
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error loading user_session_channel: {e}")
        raise

@task
def load_session_timestamp():
    """Load data into session_timestamp table from S3."""
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        
        logging.info("Loading session_timestamp data...")
        
        # Truncate table before loading
        cur.execute("TRUNCATE TABLE raw.session_timestamp;")
        
        # Copy data from S3
        cur.execute("""
            COPY INTO raw.session_timestamp
            FROM @raw.blob_stage/session_timestamp.csv;
        """)
        
        # Get row count
        cur.execute("SELECT COUNT(*) FROM raw.session_timestamp;")
        count = cur.fetchone()[0]
        logging.info(f"Loaded {count} rows into session_timestamp")
        
        cur.execute("COMMIT;")
        return f"Loaded {count} rows"
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error loading session_timestamp: {e}")
        raise

with DAG(
    dag_id='ETL_Import_User_Session_Tables',
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=['ETL', 'Import'],
    schedule='0 2 * * *',  # Run daily at 2 AM
    description='Import user_session_channel and session_timestamp tables from S3 to Snowflake'
) as dag:
    
    # Define task dependencies
    tables_created = create_tables()
    stage_created = create_stage()
    user_channel_loaded = load_user_session_channel()
    timestamp_loaded = load_session_timestamp()
    
    # Set up the workflow
    tables_created >> stage_created >> [user_channel_loaded, timestamp_loaded]