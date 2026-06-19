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

def get_smart_master_clock(object_name, is_form=False):
    """
    Dynamically resolves high-water mark timestamps across forms and datasets.
    Checks data_refined first (for down-stream logic), falling back to data_raw.
    """
    # 1. Standardize base string formatting
    if is_form:
        base_name = object_name.replace('-', '_').replace(' ', '_').lower()
        refined_table = f"form_{base_name}"
        raw_table = f"form_{base_name}_main"
    else:
        base_name = object_name.replace(' ', '_').lower()
        if base_name == "customers_db": 
            base_name = "customer_db"
        refined_table = base_name
        raw_table = f"entity_{base_name}"
    
    inspector = inspect(engine)
    target_schema = None
    target_table = None

    # 2. Determine highest priority target table available
    if inspector.has_table(refined_table, schema="data_refined"):
        target_schema = "data_refined"
        target_table = refined_table
    elif inspector.has_table(raw_table, schema="data_raw"):
        target_schema = "data_raw"
        target_table = raw_table
    else:
        return None
        
    try: 
        # 3. Dynamic Column Discovery to handle case variants & single vs double underscore formatting
        existing_columns = [c['name'].lower() for c in inspector.get_columns(target_table, schema=target_schema)]
        
        target_col = None
        if is_form:
            # Form timelines prioritize submission or update tracking fields
            priority_cols = ['_system_submissiondate', '__system_submissiondate', '_system_updatedat', '__system_updatedat']
        else:
            # Dataset timelines prioritize system update tracking fields
            priority_cols = ['__system_updatedat', '_system_updatedat', '__system_createdat', '_system_createdat']
            
        for col in priority_cols:
            if col in existing_columns:
                target_col = col
                break
                
        # Safe structural fallback if no priority meta field matches perfectly
        if not target_col:
            fallback_match = [c for c in existing_columns if 'system' in c and ('date' in c or 'at' in c)]
            if fallback_match:
                target_col = fallback_match[0]
                
        if not target_col:
            logger.warning(f"⚠️ No master clock column found in table {target_schema}.{target_table}")
            return None

        # 4. Fetch the absolute maximal timestamp value
        query = text(f'SELECT MAX("{target_col}") FROM "{target_schema}"."{target_table}";')
        with engine.connect() as conn:
            max_val = conn.execute(query).scalar()
            
            if max_val:
                if isinstance(max_val, pd.Timestamp) or hasattr(max_val, 'strftime'):
                    return max_val.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                return str(max_val)
    except Exception as e:
        logger.warning(f"⚠️ Could not read clock from {target_schema}.{target_table}: {e}")
    return None

def discover_forms(project_id):
    logger.info("🔒 Fetching all active Forms from ODK Central...")
    try:
        response = client.get(f"projects/{project_id}/forms", timeout=10)
        response.raise_for_status()
        return [form['xmlFormId'] for form in response.json()]
    except Exception as err:
        logger.error(f"❌ Failed to discover forms from ODK Central: {err}")
        raise err

def discover_datasets(project_id):
    logger.info("🔒 Attempting to connect to ODK Central and fetch datasets...")
    try:
        response = client.get(f"projects/{project_id}/datasets", timeout=10)
        response.raise_for_status()
        return [ds['name'] for ds in response.json()]
    except Exception as err:
        logger.error(f"❌ Network connection failed while hitting ODK Central: {err}")
        raise err

def fetch_entities_paginated(project_id, dataset_name, params=None):
    if params is None: params = {}
    skip = 0
    top = 2000  
    
    while True:
        current_params = params.copy()
        current_params['$top'] = top
        current_params['$skip'] = skip
        
        logger.info(f"📡 Downloading Dataset API chunk: records {skip} to {skip+top}...")
        response = client.get(f"projects/{project_id}/datasets/{dataset_name}.svc/Entities", timeout=10, params=current_params)
        response.raise_for_status()
        
        data = response.json().get('value', [])
        if not data:
            break
            
        yield data
        if len(data) < top:
            break 
        skip += top

def fetch_form_submissions_paginated(project_id, form_id, table_endpoint, params=None):
    if params is None: params = {}
    skip = 0
    top = 2000
    
    while True:
        current_params = params.copy()
        current_params['$top'] = top
        current_params['$skip'] = skip
        
        logger.info(f"📡 Downloading Form ({form_id}) OData chunk for '{table_endpoint}': records {skip} to {skip+top}...")
        response = client.get(f"projects/{project_id}/forms/{form_id}.svc/{table_endpoint}", timeout=15, params=current_params)
        response.raise_for_status()
        
        data = response.json().get('value', [])
        if not data:
            break
            
        yield data
        if len(data) < top:
            break
        skip += top

def upsert_raw_data(df, table_name, conflict_key="__id"):
    if df.empty: return 0
    staging_table = f"temp_{table_name}_{int(time.time())}"
    
    # 1. Ensure target table exists
    df.head(0).to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False)
    sync_schema(df, table_name)
    
    # 2. DYNAMIC FIX: Guarantee that the permanent target table has a unique index on the conflict key
    with engine.begin() as label_conn:
        # Check if an index already handles this column to avoid duplicate index errors
        index_check = text("""
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = :schema 
              AND tablename = :table 
              AND indexdef LIKE :col_match;
        """)
        has_index = label_conn.execute(index_check, {
            "schema": TARGET_SCHEMA,
            "table": table_name,
            "col_match": f'%("{conflict_key}")%'
        }).scalar()
        
        if not has_index:
            logger.info(f"🔑 Creating missing Unique Constraint on permanent table: {TARGET_SCHEMA}.{table_name} ({conflict_key})")
            # Build a deterministic index name to avoid naming collisions
            idx_name = f"idx_uq_{table_name}_{conflict_key.strip('_')}"
            label_conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{TARGET_SCHEMA}"."{table_name}" ("{conflict_key}");'))

    # 3. Proceed with the atomic staging upsert safely
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{TARGET_SCHEMA}"."{table_name}" INCLUDING ALL)'))
        df.to_sql(staging_table, conn, if_exists='append', index=False, chunksize=1000)
        
        conn.execute(text(f'CREATE UNIQUE INDEX ON "{staging_table}" ("{conflict_key}");'))
        
        cols = [f'"{c}"' for c in df.columns]
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
        
        conn.execute(text(f'INSERT INTO "{TARGET_SCHEMA}"."{table_name}" ({", ".join(cols)}) SELECT {", ".join(cols)} FROM "{staging_table}" ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str}'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
            
    return len(df)

def sync_form_raw(form_id, project_id):
    base_name = form_id.replace('-', '_').replace(' ', '_').lower()
    main_table_name = f"form_{base_name}_main"
    
    # Unified incremental tracking protocol using smart clock 
    last_update_str = get_smart_master_clock(form_id, is_form=True)
    last_update = pd.to_datetime(last_update_str) if last_update_str else None

    params = {}
    if last_update:
        buffered_time = last_update - pd.Timedelta(minutes=5)
        iso_timestamp = buffered_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        params["$filter"] = f"(__system/submissionDate gt {iso_timestamp}) or (__system/updatedAt gt {iso_timestamp})"
        logger.info(f"📡 Applying Form OData filter with 5-minute rolling safety buffer: {params['$filter']}")
    else:
        logger.info(f"📡 Initiating full baseline synchronization for Form '{form_id}'.")

    try:
        svc_response = client.get(f"projects/{project_id}/forms/{form_id}.svc", timeout=10)
        svc_response.raise_for_status()
        tables = [table["url"] for table in svc_response.json().get("value", [])]
    except Exception as e:
        logger.error(f"❌ Failed to retrieve OData service document structure for Form {form_id}: {e}")
        return

    for table_endpoint in tables:
        if table_endpoint == "Submissions":
            db_table_name = main_table_name
        else:
            clean_suffix = table_endpoint.replace("Submissions.", "").replace(".", "_").lower()
            db_table_name = f"form_{base_name}_{clean_suffix}"

        total_written = 0
        for page_records in fetch_form_submissions_paginated(project_id, form_id, table_endpoint, params=params):
            if not page_records:
                continue

            df = pd.json_normalize(page_records, sep='_')
            df.columns = [col.replace('-', '_').replace('__', '_').lower().strip() for col in df.columns]

            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[col] = df[col].astype(str)

            # Correctly target the primary key AFTER the columns have been stripped/cleaned
            if "_id" in df.columns:
                conflict_key = "_id"
            elif "__id" in df.columns:
                conflict_key = "__id"
            elif "meta_instanceid" in df.columns:
                conflict_key = "meta_instanceid"
            else:
                # Absolute last resort fallback
                id_cols = [c for c in df.columns if c.endswith('_id')]
                conflict_key = id_cols[0] if id_cols else df.columns[0]

            # Safely deduplicate the DataFrame *before* SQL insertion to prevent transient duplicates
            if conflict_key in df.columns:
                df = df.drop_duplicates(subset=[conflict_key], keep='last')

            count = upsert_raw_data(df, db_table_name, conflict_key=conflict_key)
            total_written += count

        if total_written > 0:
            logger.info(f"✅ Securely committed {total_written} records to {TARGET_SCHEMA}.{db_table_name}")

def sync_dataset_raw(dataset_name, project_id):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": base_name = "customer_db"
    db_table_name = f"entity_{base_name}"
    
    last_update = get_smart_master_clock(dataset_name, is_form=False)
    
    params = {}
    if last_update:
        try:
            buffer_time = pd.to_datetime(last_update) - pd.Timedelta(minutes=5)
            last_update_buffered = buffer_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            params = {"$filter": f"__system/updatedAt gt {last_update_buffered}"}
            logger.info(f"📡 Requesting dataset deltas with 5-minute rolling safety buffer: {params['$filter']}")
        except Exception as e:
            params = {"$filter": f"__system/updatedAt gt {last_update}"}
    else:
        logger.info(f"📡 Requesting full baseline dataset download.")
    
    total_written = 0
    for page_records in fetch_entities_paginated(project_id, dataset_name, params=params):
        if not page_records:
            continue
            
        df = pd.json_normalize(page_records, sep='_')

        cleaned_cols = []
        for col in df.columns:
            new_col = col.replace('properties_', '')
            new_col = new_col.replace('-', '_').lower().strip()
            cleaned_cols.append(new_col)
        df.columns = cleaned_cols

        if last_update and '__system_updatedat' in df.columns:
            df = df[df['__system_updatedat'] > last_update]
            
        if df.empty:
            continue
        
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].astype(str)
                
        count = upsert_raw_data(df, db_table_name, conflict_key="__id")
        total_written += count
        
    if total_written > 0:
        logger.info(f"✅ Securely committed {total_written} new/updated records to {TARGET_SCHEMA}.{db_table_name}")
    else:
        logger.info(f"⏸️ No delta updates found for dataset '{dataset_name}'.")

if __name__ == "__main__":
    try:
        logger.info("🎬 [Phase 1/3] Initializing High-Performance Paginated Extractor...")
        
        # 1. Relational Form Sync Pipeline (Extracts comprehensive structural repeat groups safely)
        logger.info("--- STARTING FORM EXTRACTION (RELATIONAL REPEATS) ---")
        for form in discover_forms(PROJECT_ID):
            try:
                sync_form_raw(form, PROJECT_ID)
            except Exception as e:
                logger.error(f"❌ Synchronization failed for Form '{form}': {e}", exc_info=True)
        
        # 2. Stateful Entity Dataset Sync Pipeline
        logger.info("--- STARTING DATASET EXTRACTION ---")
        for dataset in discover_datasets(PROJECT_ID):
            try:
                sync_dataset_raw(dataset, PROJECT_ID)
            except Exception as e:
                logger.error(f"❌ Synchronization failed for Dataset '{dataset}': {e}", exc_info=True)
                
        logger.info("🏁 [Phase 1/3] Extraction Operations Completed. Handoff to Cleaner...")
    except Exception as e:
        logger.critical(f"💥 [Phase 1/3] Ingestion engine halted: {e}", exc_info=True)
    finally:
        engine.dispose()
