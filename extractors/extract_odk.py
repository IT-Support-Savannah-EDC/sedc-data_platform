import os
import sys
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import DBAPIError, OperationalError
from pyodk.client import Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# 1. Setup Environment and Configurations
load_dotenv("/opt/data_platform/config/.env")
TARGET_SCHEMA = "data_raw"

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def validate_config():
    required_vars = {"EPU_ASSISTANT_URL": "EPU Assistant Webhook URL", "PROJECT_ID": "ODK Project ID", "DATABASE_URL": "PostgreSQL Database URL"}
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.critical(f"❌ MISSING CONFIGURATION: {', '.join(missing)}")
        sys.exit(1)
    return int(os.getenv("PROJECT_ID")), os.getenv("DATABASE_URL")

PROJECT_ID, DB_URL = validate_config()
engine = create_engine(DB_URL, pool_pre_ping=True)
client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")

# 2. Resilience Retry Decorators
retry_api = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60), reraise=True)
retry_db = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=5, max=50),
                 retry=retry_if_exception_type((DBAPIError, OperationalError)), reraise=True)

@retry_db
def sync_schema(df, table_name):
    """Safely adds missing columns to the raw database table based on incoming DataFrame."""
    if df.empty: 
        return
    
    with engine.connect() as conn:
        # Verify the table actually exists first
        exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = :s AND tablename = :t)")
        if not conn.execute(exists_query, {"s": TARGET_SCHEMA, "t": table_name}).scalar(): 
            return
        
        # Get existing columns
        query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t")
        existing_cols = {row[0] for row in conn.execute(query, {"s": TARGET_SCHEMA, "t": table_name}).fetchall()}
    
    new_cols = [col for col in df.columns if col not in existing_cols]
    if new_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(new_cols)} columns to {TARGET_SCHEMA}.{table_name}")
        with engine.begin() as transaction_conn:
            for col in new_cols:
                # Wrap column names in double quotes to handle system fields safely
                transaction_conn.execute(text(f'ALTER TABLE "{TARGET_SCHEMA}"."{table_name}" ADD COLUMN "{col}" TEXT'))

def get_smart_master_clock(dataset_name):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": 
        base_name = "customer_db"
    raw_table = f"entity_{base_name}"
    
    inspector = inspect(engine)
    if inspector.has_table(raw_table, schema=TARGET_SCHEMA):
        try:
            query = text(f'SELECT MAX(GREATEST("__system_createdAt", COALESCE("__system_updatedAt", "__system_createdAt"))) FROM "{TARGET_SCHEMA}"."{raw_table}";')
            with engine.connect() as conn:
                res = conn.execute(query).scalar()
                if res: 
                    return pd.Timestamp(res).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception as e:
            logger.warning(f"⚠️ Could not read clock from {TARGET_SCHEMA}.{raw_table}: {e}")
    return None

@retry_api
def discover_datasets(project_id):
    response = client.get(f"projects/{project_id}/datasets")
    response.raise_for_status()
    return [ds['name'] for ds in response.json()]

@retry_api
def fetch_entities(project_id, dataset_name, params=None):
    response = client.get(f"projects/{project_id}/datasets/{dataset_name}.svc/Entities", params=params)
    response.raise_for_status()
    return response.json()

def upsert_raw_data(df, table_name, conflict_key="__id"):
    if df.empty: 
        return 0
    
    staging_table = f"temp_{table_name}_{int(time.time())}"
    
    # Ensure baseline table framework exists
    df.head(0).to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False)
    sync_schema(df, table_name)
    
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{TARGET_SCHEMA}"."{table_name}" INCLUDING ALL)'))
            
            # 🚨 FIX: Replaced method='multi' with chunksize=1000 to prevent RAM explosions
            df.to_sql(staging_table, conn, if_exists='append', index=False, chunksize=1000)
            
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_{conflict_key}_idx" ON "{TARGET_SCHEMA}"."{table_name}" ("{conflict_key}");'))
            
            cols = [f'"{c}"' for c in df.columns]
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
            
            upsert_query = text(f"""
                INSERT INTO "{TARGET_SCHEMA}"."{table_name}" ({", ".join(cols)}) 
                SELECT {", ".join(cols)} FROM "{staging_table}" 
                ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str}
            """)
            conn.execute(upsert_query)
            return len(df)
    finally:
        with engine.connect() as cleanup_conn:
            cleanup_conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
            cleanup_conn.commit()
            
def sync_dataset_raw(dataset_name, project_id):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": 
        base_name = "customer_db"
    db_table_name = f"entity_{base_name}"
    
    last_update = get_smart_master_clock(dataset_name)
    params = {"$filter": f"__system/updatedAt gt {last_update}"} if last_update else {}
    
    res = fetch_entities(project_id, dataset_name, params=params)
    if not res or 'value' not in res or not res['value']:
        logger.info(f"⏸️ No updates found for dataset: {dataset_name}")
        return
    
    df = pd.json_normalize(res['value'], sep='_')
    count = upsert_raw_data(df, db_table_name, conflict_key="__id")
    logger.info(f"✅ Written {count} records into {TARGET_SCHEMA}.{db_table_name}")

if __name__ == "__main__":
    try:
        logger.info("🎬 Initializing Adjusted ODK Raw Extractor Engine...")
        for dataset in discover_datasets(PROJECT_ID):
            sync_dataset_raw(dataset, PROJECT_ID)
    except Exception as e:
        logger.critical(f"💥 Ingestion engine halted: {e}", exc_info=True)
    finally:
        engine.dispose()
