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
    required_vars = {"EPU_ASSISTANT_URL": "Webhook URL", "PROJECT_ID": "Project ID", "DATABASE_URL": "DB URL"}
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.critical(f"❌ MISSING CONFIGURATION: {', '.join(missing)}")
        sys.exit(1)
    return int(os.getenv("PROJECT_ID")), os.getenv("DATABASE_URL")

PROJECT_ID, DB_URL = validate_config()
engine = create_engine(DB_URL, pool_pre_ping=True)
client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")

retry_db = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=5, max=50),
                 retry=retry_if_exception_type((DBAPIError, OperationalError)), reraise=True)

@retry_db
def sync_schema(df, table_name):
    if df.empty: return
    with engine.connect() as conn:
        exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = :s AND tablename = :t)")
        if not conn.execute(exists_query, {"s": TARGET_SCHEMA, "t": table_name}).scalar(): return
        
        query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t")
        existing_cols = {row[0] for row in conn.execute(query, {"s": TARGET_SCHEMA, "t": table_name}).fetchall()}
    
    new_cols = [col for col in df.columns if col not in existing_cols]
    if new_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(new_cols)} columns to {TARGET_SCHEMA}.{table_name}")
        with engine.begin() as transaction_conn:
            for col in new_cols:
                transaction_conn.execute(text(f'ALTER TABLE "{TARGET_SCHEMA}"."{table_name}" ADD COLUMN "{col}" TEXT'))

def get_smart_master_clock(dataset_name):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": base_name = "customer_db"

    raw_table = f"entity_{base_name}"
    refined_table = base_name
    
    inspector = inspect(engine)

    # Priority 1
    if inspector.has_table(refined_table, schema="data_refined"):
        target_schema = "data_refined"
        target_table = refined_table

    # Priority 2
    elif inspector.has_table(raw_table, schema="data_raw"):
        target_schema = "data.raw"
        target_table = raw_table
    # Priority 3
    else:
        return None
    try: 
        # This allows Postgres to use indexes and execute instantly.
        query = text(f'''
            SELECT 
                MAX("__system_updatedAt") as max_up, 
                MAX("__system_createdAt") as max_cr 
            FROM "data_refined"."{refined_table}";
        ''')
        with engine.connect() as conn:
            res = conn.execute(query).fetchone()
            if res and res[0] is not None:
                return pd.Timestamp(res[0]).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception as e:
        logger.warning(f"⚠️ Could not read clock from data_refined.{refined_table}: {e}")
return None

def fetch_entities_paginated(project_id, dataset_name, params=None):
    """
    FIX: Prevents OOM crashes by strictly paginating the API call.
    Yields data in safe 2,000-record chunks.
    """
    if params is None: params = {}
    skip = 0
    top = 2000  
    
    while True:
        current_params = params.copy()
        current_params['$top'] = top
        current_params['$skip'] = skip
        
        logger.info(f"📡 Downloading API chunk: records {skip} to {skip+top}...")
        response = client.get(f"projects/{project_id}/datasets/{dataset_name}.svc/Entities", params=current_params)
        response.raise_for_status()
        
        data = response.json().get('value', [])
        if not data:
            break
            
        yield data
        
        if len(data) < top:
            break 
            
        skip += top

def discover_datasets(project_id):
    response = client.get(f"projects/{project_id}/datasets")
    response.raise_for_status()
    return [ds['name'] for ds in response.json()]

def upsert_raw_data(df, table_name, conflict_key="__id"):
    if df.empty: return 0
    staging_table = f"temp_{table_name}_{int(time.time())}"
    df.head(0).to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False)
    sync_schema(df, table_name)
    
    # FIX: Everything now happens strictly inside ONE connection context to prevent table leaks
    with engine.begin() as conn:
        try:
            conn.execute(text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{TARGET_SCHEMA}"."{table_name}" INCLUDING ALL)'))
            df.to_sql(staging_table, conn, if_exists='append', index=False, chunksize=1000)
            conn.execute(text(f'CREATE UNIQUE INDEX ON "{staging_table}" ("{conflict_key}");'))
            
            cols = [f'"{c}"' for c in df.columns]
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
            conn.execute(text(f'INSERT INTO "{TARGET_SCHEMA}"."{table_name}" ({", ".join(cols)}) SELECT {", ".join(cols)} FROM "{staging_table}" ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str}'))
        finally:
            # Drop command stays inside the same transaction
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
            
    return len(df)

def sync_dataset_raw(dataset_name, project_id):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": base_name = "customer_db"
    db_table_name = f"entity_{base_name}"
    
    last_update = get_smart_master_clock(dataset_name)
    params = {"$filter": f"__system/updatedAt gt {last_update}"} if last_update else {}
    
    total_written = 0
    # Process the data in strict memory-safe pages
    for page_records in fetch_entities_paginated(project_id, dataset_name, params=params):
        df = pd.json_normalize(page_records, sep='_')
        
        # FIX: Stringify complex dictionaries/lists so psycopg2 doesn't crash or spike memory
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].astype(str)
                
        count = upsert_raw_data(df, db_table_name, conflict_key="__id")
        total_written += count
        
    if total_written > 0:
        logger.info(f"✅ Securely committed {total_written} records to {TARGET_SCHEMA}.{db_table_name}")
    else:
        logger.info(f"⏸️ No delta updates found for '{dataset_name}'.")

if __name__ == "__main__":
    try:
        logger.info("🎬 Initializing High-Performance Paginated Extractor...")
        for dataset in discover_datasets(PROJECT_ID):
            sync_dataset_raw(dataset, PROJECT_ID)
    except Exception as e:
        logger.critical(f"💥 Ingestion engine halted: {e}", exc_info=True)
    finally:
        engine.dispose()
