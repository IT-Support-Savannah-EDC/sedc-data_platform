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

@retry_db
def get_smart_master_clock(name, sync_type="dataset"):
    """
    The Smart Master Clock.
    Dynamically scans 'data_refined' (Priority 1) and 'data_raw' (Priority 2) schemas
    to find the most recent record timestamp for either a Form or a Dataset.
    Resolves column names dynamically to prevent database casing exceptions.
    """
    # Normalize the base name to match database conventions
    base_name = name.replace(' ', '_').replace('-', '_').lower()
    if base_name == "customers_db": 
        base_name = "customer_db"

    # 1. Dynamically resolve table name candidates based on type
    if sync_type == "form":
        refined_candidates = [base_name, f"form_{base_name}"]
        raw_candidates = [f"form_{base_name}_main", f"{base_name}_main", f"form_{base_name}", base_name]
    else:  # dataset
        refined_candidates = [base_name, f"entity_{base_name}"]
        raw_candidates = [f"entity_{base_name}", base_name]

    inspector = inspect(engine)
    target_schema = None
    target_table = None

    # Priority 1: Scan 'data_refined'
    for table_cand in refined_candidates:
        if inspector.has_table(table_cand, schema="data_refined"):
            target_schema = "data_refined"
            target_table = table_cand
            break

    # Priority 2: Fall back to scan 'data_raw'
    if not target_schema:
        for table_cand in raw_candidates:
            if inspector.has_table(table_cand, schema="data_raw"):
                target_schema = "data_raw"
                target_table = table_cand
                break

    # If no table exists yet in either schema, trigger a clean full extract
    if not target_schema or not target_table:
        logger.debug(f"⏰ No tracking table found for {sync_type} '{name}' in data_refined/data_raw. Performing full baseline extraction.")
        return None

    try:
        # 2. Dynamic Column Resolution: Query actual columns to avoid casing & missing-column crashes
        existing_columns = [col['name'] for col in inspector.get_columns(target_table, schema=target_schema)]
        existing_cols_lower = [c.lower() for c in existing_columns]
        col_mapping = dict(zip(existing_cols_lower, existing_columns))

        # Known time-tracking identifiers used across ODK forms and datasets
        time_candidates = [
            '__system_updatedat', '__system_submissiondate', '__system_createdat',
            '__system.updatedat', '__system.submissiondate', '__system.createdat'
        ]
        
        # Match only columns that strictly exist in this target table
        matched_cols = [col_mapping[cand] for cand in time_candidates if cand in col_mapping]

        if not matched_cols:
            logger.warning(f"⚠️ No system time columns found in '{target_schema}'.'{target_table}'. Falling back to full extract.")
            return None

        # 3. Formulate dynamic SQL expression using GREATEST (Postgres safely ignores NULLs in GREATEST)
        quoted_cols = [f'"{c}"' for c in matched_cols]
        query_expr = quoted_cols[0] if len(quoted_cols) == 1 else f'GREATEST({", ".join(quoted_cols)})'
        
        query = text(f'SELECT MAX({query_expr}) FROM "{target_schema}"."{target_table}"')

        with engine.connect() as conn:
            result = conn.execute(query).scalar()
            
            if result:
                # 4. Enforce strict UTC conversion to prevent OData protocol misalignments
                ts = pd.to_datetime(result, utc=True)
                iso_string = ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                logger.info(f"⏰ Smart Master Clock milestone for {sync_type} '{name}' ({target_schema}.{target_table}): {iso_string}")
                return iso_string

    except Exception as e:
        logger.warning(f"⚠️ Could not read Smart Master Clock from '{target_schema}'.'{target_table}': {e}. Falling back to full extract.")
    
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
    df.head(0).to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False)
    sync_schema(df, table_name)
    
    with engine.begin() as conn:
        try:
            conn.execute(text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{TARGET_SCHEMA}"."{table_name}" INCLUDING ALL)'))
            df.to_sql(staging_table, conn, if_exists='append', index=False, chunksize=1000)
            conn.execute(text(f'CREATE UNIQUE INDEX ON "{staging_table}" ("{conflict_key}");'))
            
            cols = [f'"{c}"' for c in df.columns]
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
            conn.execute(text(f'INSERT INTO "{TARGET_SCHEMA}"."{table_name}" ({", ".join(cols)}) SELECT {", ".join(cols)} FROM "{staging_table}" ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str}'))
        finally:
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
            
    return len(df)

def sync_form_raw(form_id, project_id):
    base_name = form_id.replace('-', '_').replace(' ', '_').lower()
    main_table_name = f"form_{base_name}_main"
    
    inspector = inspect(engine)
    last_update = None
    
    if inspector.has_table(main_table_name, schema=TARGET_SCHEMA):
        try:
            query = text(f'SELECT MAX("__system_submissiondate") FROM "{TARGET_SCHEMA}"."{main_table_name}"')
            with engine.connect() as conn:
                max_val = conn.execute(query).scalar()
                if max_val:
                    last_update = pd.to_datetime(max_val)
        except Exception as e:
            logger.warning(f"⚠️ Could not read clock from form table {main_table_name}: {e}")

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

            conflict_key = "__id" if "__id" in df.columns else "id" if "id" in df.columns else None
            if not conflict_key:
                id_cols = [c for c in df.columns if 'id' in c]
                conflict_key = id_cols[0] if id_cols else df.columns[0]

            count = upsert_raw_data(df, db_table_name, conflict_key=conflict_key)
            total_written += count

        if total_written > 0:
            logger.info(f"✅ Securely committed {total_written} records to {TARGET_SCHEMA}.{db_table_name}")

def sync_dataset_raw(dataset_name, project_id):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": base_name = "customer_db"
    db_table_name = f"entity_{base_name}"
    
    last_update = get_smart_master_clock(dataset_name)
    
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
