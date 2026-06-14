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
TARGET_SCHEMA = "data_raw"  # Updated per consolidated schema requirement

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def validate_config():
    required_vars = {
        "EPU_ASSISTANT_URL": "EPU Assistant Webhook URL",
        "PROJECT_ID": "ODK Project ID (Numerical)",
        "DATABASE_URL": "PostgreSQL Database URL"
    }
    missing = []
    env_data = {}
    for var, friendly_name in required_vars.items():
        val = os.getenv(var)
        if not val:
            missing.append(f"{var} ({friendly_name})")
        else:
            env_data[var] = val
    if missing:
        logger.critical("❌ MISSING CONFIGURATION:\n" + "\n".join([f" {m}" for m in missing]))
        sys.exit(1)
    env_data["PROJECT_ID"] = int(env_data["PROJECT_ID"])
    return env_data

env_data = validate_config()
PROJECT_ID = env_data["PROJECT_ID"]
engine = create_engine(env_data["DATABASE_URL"], pool_pre_ping=True)
client = Client(config_path="/opt/data_platform/config/pyodk_config.toml")

# 2. Resilience Retry Decorators
retry_api = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60), reraise=True)
retry_db = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=5, max=50),
                  retry=retry_if_exception_type((DBAPIError, OperationalError)), reraise=True)

# 3. Dynamic Schema Evolution
@retry_db
def sync_schema(df, table_name):
    if df.empty: return
    with engine.connect() as conn:
        exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = :s AND tablename = :t)")
        if not conn.execute(exists_query, {"s": TARGET_SCHEMA, "t": table_name}).scalar():
            return
        query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t")
        existing_cols = {row[0] for row in conn.execute(query, {"s": TARGET_SCHEMA, "t": table_name}).fetchall()}
    
    new_cols = [col for col in df.columns if col not in existing_cols]
    if new_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(new_cols)} columns to {TARGET_SCHEMA}.{table_name}")
        with engine.begin() as transaction_conn:
            for col in new_cols:
                transaction_conn.execute(text(f'ALTER TABLE "{TARGET_SCHEMA}"."{table_name}" ADD COLUMN "{col}" TEXT'))

# 4. Bi-Schema Aware Master Clock
def get_smart_master_clock(dataset_name):
    """
    Scans both data_raw and data_refined schemas for historical milestones.
    If no tables exist in either location, safely returns None to trigger a full fetch.
    """
    # Normalize name mapping: Sync central 'Customers_DB' directly with warehouse 'customer_db'
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db":
        base_name = "customer_db"
        
    raw_table = f"entity_{base_name}"
    refined_table = base_name
    
    inspector = inspect(engine)
    found_milestones = []
    
    # 1. Inspect the Raw Schema
    if inspector.has_table(raw_table, schema=TARGET_SCHEMA):
        try:
            raw_query = text(f'SELECT MAX(GREATEST("__system_createdAt", COALESCE("__system_updatedAt", "__system_createdAt"))) FROM "{TARGET_SCHEMA}"."{raw_table}";')
            with engine.connect() as conn:
                res = conn.execute(raw_query).scalar()
                if res:
                    found_milestones.append(pd.Timestamp(res))
        except Exception as e:
            logger.warning(f"⚠️ Could not read clock from {TARGET_SCHEMA}.{raw_table}: {e}")
            
    # 2. Inspect the Refined Schema
    if inspector.has_table(refined_table, schema="data_refined"):
        try:
            refined_query = text(f'SELECT MAX(GREATEST("__createdat", COALESCE("__updatedat", "__createdat"))) FROM "data_refined"."{refined_table}";')
            with engine.connect() as conn:
                res = conn.execute(refined_query).scalar()
                if res:
                    found_milestones.append(pd.Timestamp(res))
        except Exception as e:
            logger.warning(f"⚠️ Could not read clock from data_refined.{refined_table}: {e}")

    # 3. Decision Engine
    if found_milestones:
        max_milestone = max(found_milestones)
        ts_str = max_milestone.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        logger.info(f"🎯 Milestone Found! Syncing updates newer than: {ts_str}")
        return ts_str
        
    logger.info(f"ℹ️ No baseline discovered in {TARGET_SCHEMA} or data_refined for '{dataset_name}'. Preparing full historical fetch.")
    return None

# 5. ODK API Integration Functions
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

# 6. Schema-Isolated Upsert Engine
def upsert_raw_data(df, table_name, conflict_key="__id"):
    if df.empty: return 0
    staging_table = f"temp_{table_name}_{int(time.time())}"
    
    df.head(0).to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False)
    sync_schema(df, table_name)
    
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{TARGET_SCHEMA}"."{table_name}" INCLUDING ALL)'))
            df.to_sql(staging_table, conn, if_exists='append', index=False, method='multi')
            
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_{conflict_key}_idx" ON "{TARGET_SCHEMA}"."{table_name}" ("{conflict_key}");'))            
            
            columns = [f'"{col}"' for col in df.columns]
            column_str = ", ".join(columns)
            update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != conflict_key])
            
            upsert_query = text(f"""
                INSERT INTO "{TARGET_SCHEMA}"."{table_name}" ({column_str})
                SELECT {column_str} FROM "{staging_table}"
                ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str}
            """)
            conn.execute(upsert_query)
            return len(df)
    finally:
        with engine.connect() as cleanup_conn:
            cleanup_conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
            cleanup_conn.commit()

def sync_dataset_raw(dataset_name, project_id, dry_run=False):
    # Standardize table naming metrics internally
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db":
        base_name = "customer_db"
    db_table_name = f"entity_{base_name}"
    
    logger.info(f"🧬 --- Processing Ingestion for Dataset: {dataset_name} ---")
    
    last_update_time = get_smart_master_clock(dataset_name)
    api_params = {}
    if last_update_time:
        api_params["$filter"] = f"__system/updatedAt gt {last_update_time}"
    
    entities_response = fetch_entities(project_id, dataset_name, params=api_params)
    if not entities_response or 'value' not in entities_response or not entities_response['value']:
        logger.info(f"⏸️ No delta updates found for '{dataset_name}'. Schema matches baseline.")
        return

    df = pd.json_normalize(entities_response['value'], sep='_')
    logger.info(f"📡 API retrieved {len(df)} records.")

    if dry_run:
        logger.info(f"🧪 [DRY RUN SUCCESS]: Data verified for {db_table_name}. Columns: {len(df.columns)}. Rows: {len(df)}")
        return

    count = upsert_raw_data(df, db_table_name, conflict_key="__id")
    logger.info(f"✅ Successfully written {count} records into {TARGET_SCHEMA}.{db_table_name}")

if __name__ == "__main__":
    DRY_RUN_TOGGLE = True 
    
    logger.info("🎬 Initializing Adjusted ODK Raw Extractor Engine...")
    try:
        datasets = discover_datasets(PROJECT_ID)
        for dataset in datasets:
            sync_dataset_raw(dataset, PROJECT_ID, dry_run=DRY_RUN_TOGGLE)
    except Exception as e:
        logger.critical(f"💥 Ingestion engine halted: {e}", exc_info=True)
