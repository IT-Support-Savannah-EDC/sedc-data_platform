import os
import sys
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import DBAPIError, OperationalError
from pyodk.client import Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

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
client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")

# 2. Resilience Retry Decorators
retry_api = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60), reraise=True)
retry_db = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=5, max=50),
                  retry=retry_if_exception_type((DBAPIError, OperationalError)), reraise=True)

# 3. Dynamic Schema Evolution (Schema-Aware)
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

# 4. Smart Master Clock (Prevents Duplicate 156k Row Downloads)
def get_smart_master_clock(dataset_name):
    """
    Defensively checks for table existence using SQLAlchemy Inspector.
    Prevents UndefinedTable exceptions from bypassing the refined data fallback.
    """
    raw_table = f"entity_{dataset_name.replace(' ', '_').lower()}"
    inspector = inspect(engine)
    
    # 1. Safely check if the Raw Table exists in data_raw_odk
    if inspector.has_table(raw_table, schema=TARGET_SCHEMA):
        try:
            raw_query = text(f'SELECT MAX(GREATEST("__system_createdAt", COALESCE("__system_updatedAt", "__system_createdAt"))) FROM "{TARGET_SCHEMA}"."{raw_table}";')
            with engine.connect() as conn:
                result = conn.execute(raw_query).scalar()
                if result:
                    return pd.Timestamp(result).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception as e:
            logger.warning(f"⚠️ Could not parse raw master clock for {raw_table}: {e}")
    else:
        logger.info(f"ℹ️ Raw table {TARGET_SCHEMA}.{raw_table} does not exist yet. Checking Refined schema fallback...")

    # 2. Fallback: If it's the Customers_DB dataset, reference the manual 156k seed table
    if dataset_name.lower() in ["customers_db", "customers", "customer_db"]:
        if inspector.has_table("customer_db", schema="data_refined"):
            try:
                refined_query = text('SELECT MAX(GREATEST("__createdat", COALESCE("__updatedat", "__createdat"))) FROM data_refined.customer_db;')
                with engine.connect() as conn:
                    result = conn.execute(refined_query).scalar()
                    if result:
                        ts = pd.Timestamp(result).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        logger.info(f"🎯 Milestone Match! Anchoring OData filter to Refined Baseline: {ts}")
                        return ts
            except Exception as e:
                logger.warning(f"⚠️ Refined fallback lookup failed: {e}")
        else:
            logger.warning("⚠️ High-priority check failed: data_refined.customer_db table not found.")        
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

# 6. High-Performance Schema-Isolated Upsert Engine
def upsert_raw_data(df, table_name, conflict_key="__id"):
    if df.empty: return 0
    staging_table = f"temp_{table_name}_{int(time.time())}"
    
    # Structural step: Ensure target schema table exists before altering or inserting
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
    logger.info(f"🧬 --- Extracting Raw Entities for Dataset: {dataset_name} ---")
    db_table_name = f"{dataset_name.replace(' ', '_').lower()}"
    
    last_update_time = get_smart_master_clock(dataset_name)
    api_params = {}
    if last_update_time:
        api_params["$filter"] = f"__system/updatedAt gt {last_update_time}"
    
    entities_response = fetch_entities(project_id, dataset_name, params=api_params)
    if not entities_response or 'value' not in entities_response or not entities_response['value']:
        logger.info(f"⏸️ No delta updates found for dataset '{dataset_name}'. Schema is up to date.")
        return

    df = pd.json_normalize(entities_response['value'], sep='_')
    logger.info(f"📡 API retrieved {len(df)} new/modified records.")

    if dry_run:
        logger.info(f"🧪 [DRY RUN SUCCESS]: Data verified. Column count: {len(df.columns)}. Rows found: {len(df)}")
        print(df.head(2))
        return

    count = upsert_raw_data(df, db_table_name, conflict_key="__id")
    logger.info(f"✅ Successfully written {count} records into {TARGET_SCHEMA}.{db_table_name}")

if __name__ == "__main__":
    # Test with DRY_RUN = True first to verify credentials and clock logic safely
    DRY_RUN_TOGGLE = True 
    
    logger.info("🎬 Initializing Modular ODK Raw Extractor Test...")
    try:
        datasets = discover_datasets(PROJECT_ID)
        for dataset in datasets:
            sync_dataset_raw(dataset, PROJECT_ID, dry_run=DRY_RUN_TOGGLE)
    except Exception as e:
        logger.critical(f"💥 Testing phase aborted due to error: {e}", exc_info=True)
