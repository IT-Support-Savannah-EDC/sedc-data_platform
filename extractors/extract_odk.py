import os
from pathlib import Path
import sys
import uuid
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DBAPIError, OperationalError
from pyodk.client import Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
#
BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / "config" / ".env"

# 1. Setup Environment and Configurations
load_dotenv(dotenv_path=env_path)
TARGET_SCHEMA = "data_raw"

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def validate_config():
    required_vars = {"PROJECT_ID", "DATABASE_URL"}
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.critical(f"❌ MISSING CONFIGURATION: {', '.join(missing)}")
        sys.exit(1)
    return int(os.getenv("PROJECT_ID")), os.getenv("DATABASE_URL")

PROJECT_ID, DB_URL = validate_config()
engine = create_engine(DB_URL, pool_pre_ping=True)
client = Client(config_path=BASE_DIR / "config" / ".pyodk_config.toml")

retry_db = retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=5, max=50),
                retry=retry_if_exception_type((DBAPIError, OperationalError)), reraise=True)

def get_smart_master_clock(table_name):
    """
    Simplified Clock: Queries the standardized 'odk_timestamp' column directly.
    """
    query = text(f'SELECT MAX(odk_timestamp) FROM "{TARGET_SCHEMA}"."{table_name}";')
    try:
        with engine.connect() as conn:
            max_val = conn.execute(query).scalar()
            if max_val:
                if isinstance(max_val, pd.Timestamp) or hasattr(max_val, 'strftime'):
                    return max_val.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                return str(max_val)
    except Exception:
        # Table likely doesn't exist yet, which is normal for a fresh run
        pass
    return None

# --- [ KEEP DISCOVERY & PAGINATION EXACTLY AS THEY WERE ] ---
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
        
        logger.info(f"📡 Downloading Dataset chunk: records {skip} to {skip+top}...")
        response = client.get(f"projects/{project_id}/datasets/{dataset_name}.svc/Entities", timeout=30, params=current_params)
        response.raise_for_status()
        
        data = response.json().get('value', [])
        if not data: break
            
        yield data
        if len(data) < top: break 
        skip += top

def fetch_form_submissions_paginated(project_id, form_id, table_endpoint, params=None):
    if params is None: params = {}
    skip = 0
    top = 1500
    
    while True:
        current_params = params.copy()
        current_params['$top'] = top
        current_params['$skip'] = skip
        
        logger.info(f"📡 Downloading Form ({form_id}) chunk for '{table_endpoint}': records {skip} to {skip+top}...")
        response = client.get(f"projects/{project_id}/forms/{form_id}.svc/{table_endpoint}", timeout=1000, params=current_params)
        response.raise_for_status()
        
        data = response.json().get('value', [])
        if not data: break
            
        yield data
        if len(data) < top: break
        skip += top
# ------------------------------------------------------------

@retry_db
def upsert_raw_data(df, table_name):
    if df.empty: return 0
    staging_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
    conflict_key = "_id"
    
    # 1. Ensure target table exists with the JSONB dtype mapping
    df.head(0).to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False, dtype={'raw_record': JSONB})
    
    # 2. Guarantee unique index on the standard '_id' column
    with engine.begin() as conn:
        conn.execute(text(f'''
            CREATE UNIQUE INDEX IF NOT EXISTS "idx_uq_{table_name}_id" 
            ON "{TARGET_SCHEMA}"."{table_name}" ("{conflict_key}");
        '''))

    # 3. Atomic staging upsert using ON COMMIT DROP
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{TARGET_SCHEMA}"."{table_name}" INCLUDING ALL) ON COMMIT DROP'))
        df.to_sql(staging_table, conn, if_exists='append', index=False, chunksize=1000, dtype={'raw_record': JSONB})
        
        cols = [f'"{c}"' for c in df.columns]
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
        
        upsert_query = f'''
            INSERT INTO "{TARGET_SCHEMA}"."{table_name}" ({", ".join(cols)}) 
            SELECT {", ".join(cols)} FROM "{staging_table}" 
            ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str}
        '''
        conn.execute(text(upsert_query))
            
    return len(df)

def parse_pure_raw(page_records):
    """
    Transforms ODK JSON arrays into a strict 4-column structural DataFrame:
    _id, odk_timestamp, raw_record (JSON), and extracted_at.
    """
    parsed = []
    extract_time = pd.Timestamp.utcnow()
    
    for row in page_records:
        # 1. Resolve Unique ID safely
        record_id = row.get('__id') or row.get('_id') or row.get('meta', {}).get('instanceID') or row.get('id')
        if not record_id:
            # Fallback for orphaned repeat groups: Hash the JSON to ensure it gets saved
            record_id = uuid.uuid5(uuid.NAMESPACE_OID, json.dumps(row)).hex
            
        # 2. Resolve Master Timestamp safely (if it exists)
        sys_block = row.get('__system', {})
        record_ts = sys_block.get('updatedAt') or sys_block.get('submissionDate') or sys_block.get('createdAt')
        
        parsed.append({
            '_id': str(record_id),
            'odk_timestamp': pd.to_datetime(record_ts) if record_ts else None,
            'raw_record': row, # Will be mapped to JSONB via SQLAlchemy
            'extracted_at': extract_time
        })
        
    return pd.DataFrame(parsed)

def sync_form_raw(form_id, project_id):
    base_name = form_id.replace('-', '_').replace(' ', '_').lower()
    main_table_name = f"form_{base_name}_main"
    
    last_update_str = get_smart_master_clock(main_table_name)
    last_update = pd.to_datetime(last_update_str) if last_update_str else None

    base_params = {}
    if last_update:
        buffered_time = last_update - pd.Timedelta(minutes=5)
        iso_timestamp = buffered_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        base_params["$filter"] = f"(__system/submissionDate gt {iso_timestamp}) or (__system/updatedAt gt {iso_timestamp})"
        logger.info(f"📡 Filter applied with 5-minute buffer: {base_params['$filter']}")
    else:
        logger.info(f"📡 Full baseline sync for Form '{form_id}'.")

    try:
        svc_response = client.get(f"projects/{project_id}/forms/{form_id}.svc", timeout=10)
        svc_response.raise_for_status()
        tables = [table["url"] for table in svc_response.json().get("value", [])]
    except Exception as e:
        logger.error(f"❌ Failed to retrieve OData service document structure for Form {form_id}: {e}")
        return

    for table_endpoint in tables:
        db_table_name = main_table_name if table_endpoint == "Submissions" else f"form_{base_name}_{table_endpoint.replace('Submissions.', '').replace('.', '_').lower()}"

        # Strip $filter for sub-tables because ODK doesn't support __system queries on repeat groups
        current_params = base_params.copy()
        if table_endpoint != "Submissions" and "$filter" in current_params:
            current_params.pop("$filter", None)

        total_written = 0
        for page_records in fetch_form_submissions_paginated(project_id, form_id, table_endpoint, params=current_params):
            if not page_records: continue

            # PURE EXTRACTION: Convert to 4-column schema without touching the JSON
            df = parse_pure_raw(page_records)
            df = df.drop_duplicates(subset=['_id'], keep='last')
            
            total_written += upsert_raw_data(df, db_table_name)

        if total_written > 0:
            logger.info(f"✅ Securely committed {total_written} raw records to {TARGET_SCHEMA}.{db_table_name}")

def sync_dataset_raw(dataset_name, project_id):
    base_name = dataset_name.replace(' ', '_').lower()
    if base_name == "customers_db": base_name = "customer_db"
    db_table_name = f"entity_{base_name}"
    
    last_update = get_smart_master_clock(db_table_name)
    
    params = {}
    if last_update:
        buffer_time = pd.to_datetime(last_update) - pd.Timedelta(minutes=5)
        last_update_buffered = buffer_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        params = {"$filter": f"__system/updatedAt gt {last_update_buffered}"}
        logger.info(f"📡 Dataset delta requested: {params['$filter']}")
    else:
        logger.info(f"📡 Full baseline dataset download.")
    
    total_written = 0
    for page_records in fetch_entities_paginated(project_id, dataset_name, params=params):
        if not page_records: continue
            
        # PURE EXTRACTION: Convert to 4-column schema without touching the JSON
        df = parse_pure_raw(page_records)
        df = df.drop_duplicates(subset=['_id'], keep='last')
        
        total_written += upsert_raw_data(df, db_table_name)
        
    if total_written > 0:
        logger.info(f"✅ Securely committed {total_written} raw entity records to {TARGET_SCHEMA}.{db_table_name}")

if __name__ == "__main__":
    try:
        logger.info("🎬 [Phase 1/3] Initializing Pure Raw Data Extractor...")
        
        logger.info("--- STARTING FORM EXTRACTION ---")
        for form in discover_forms(PROJECT_ID):
            try:
                sync_form_raw(form, PROJECT_ID)
            except Exception as e:
                logger.error(f"❌ Sync failed for Form '{form}': {e}", exc_info=True)
        
        logger.info("--- STARTING DATASET EXTRACTION ---")
        for dataset in discover_datasets(PROJECT_ID):
            try:
                sync_dataset_raw(dataset, PROJECT_ID)
            except Exception as e:
                logger.error(f"❌ Sync failed for Dataset '{dataset}': {e}", exc_info=True)
                
        logger.info("🏁 [Phase 1/3] Extraction Operations Completed.")
    except Exception as e:
        logger.critical(f"💥 [Phase 1/3] Ingestion engine halted: {e}", exc_info=True)
    finally:
        engine.dispose()
