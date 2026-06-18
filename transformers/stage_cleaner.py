import os
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROXY_BASE_URL = "http://134.209.178.35:5050/media"
MEDIA_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.mp4', '.3gp', '.wav', '.mp3')

def get_engine():
    load_dotenv("/opt/data_platform/config/.env")
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        raise ValueError("❌ DATABASE_URL missing.")
    return create_engine(db_uri, pool_pre_ping=True)

def discover_raw_tables():
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='data_raw')
    logger.info(f"🔍 Discovered {len(tables)} tables in 'data_raw': {tables}")
    return tables

def clean_and_vectorize_media(df, dataset_name):
    if df.empty:
        return df
        
    df.columns = [c.lower().strip() for c in df.columns]
    
    id_col = next((c for c in ['__id', 'id', 'uuid'] if c in df.columns), None)
    if not id_col:
        logger.warning(f"⚠️ No ID column found for {dataset_name}. Skipping vectorization.")
        return df

    for col in df.columns:
        sample = df[col].dropna().astype(str).head(15)
        if any(val.lower().endswith(MEDIA_EXTENSIONS) for val in sample):
            proxy_col_name = f"{col}_proxy_url"
            logger.info(f"🔗 Vectorizing '{col}' -> '{proxy_col_name}'")
            
            df[proxy_col_name] = df.apply(
                lambda r: f"{PROXY_BASE_URL}/{dataset_name}/{r[id_col]}/{r[col]}"
                if pd.notna(r[col]) and str(r[col]).strip() != "" else None,
                axis=1
            )
    return df

def sync_staging_schema(engine, df, staging_table, conflict_key):
    """Ensures staging table exists and automatically adds new columns."""
    target_schema = "data_staging"
    with engine.connect() as conn:
        exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = :s AND tablename = :t)")
        exists = conn.execute(exists_query, {"s": target_schema, "t": staging_table}).scalar()

    # 1. Base table creation if it does not exist
    if not exists:
        logger.info(f"✨ Target table {target_schema}.{staging_table} missing. Building structure...")
        df.head(0).to_sql(staging_table, engine, schema=target_schema, if_exists='replace', index=False)
        with engine.begin() as conn:
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{staging_table}_idx" ON "{target_schema}"."{staging_table}" ("{conflict_key}");'))
        return

    # 2. Schema Evolution
    with engine.connect() as conn:
        query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t")
        existing_cols = {row[0] for row in conn.execute(query, {"s": target_schema, "t": staging_table}).fetchall()}

    new_cols = [col for col in df.columns if col not in existing_cols]
    if new_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(new_cols)} columns to {target_schema}.{staging_table}")
        with engine.begin() as transaction_conn:
            for col in new_cols:
                transaction_conn.execute(text(f'ALTER TABLE "{target_schema}"."{staging_table}" ADD COLUMN "{col}" TEXT'))

def upsert_to_staging(df, table_name, chunk_idx):
    if df.empty:
        return
        
    engine = get_engine()
    staging_table = f"stage_{table_name.lower()}"
    conflict_key = next((c for c in ['__id', 'id'] if c in df.columns), None)
    
    if not conflict_key:
        logger.error(f"❌ Aborting upsert for {table_name}: No valid conflict key.")
        return

    # Synchronize schema to allow dynamic column additions (e.g. proxy URLs)
    sync_staging_schema(engine, df, staging_table, conflict_key)

    logger.info(f"🔄 Committing chunk {chunk_idx} to data_staging.{staging_table} ({len(df)} rows)...")
    
    temp_holder = f"temp_{staging_table}_{chunk_idx}_{int(pd.Timestamp.now().timestamp())}"
    with engine.begin() as conn:
        df.to_sql(temp_holder, conn, if_exists='replace', index=False, chunksize=1000)
        cols_str = ", ".join([f'"{c}"' for c in df.columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
        
        upsert_sql = f"""
            INSERT INTO data_staging."{staging_table}" ({cols_str})
            SELECT {cols_str} FROM "{temp_holder}"
            ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str};
        """
        conn.execute(text(upsert_sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{temp_holder}"'))
    logger.info(f"✅ Chunk {chunk_idx} committed successfully.")

def clear_raw_table(engine, raw_table_name):
    """
    Safely purges data from the raw landing table after successful staging ingestion.
    """
    logger.info(f"🧹 [PURGE] Initiating safe clear for data_raw.\"{raw_table_name}\"...")
    with engine.begin() as conn:
        # TRUNCATE is faster and cleaner than DELETE for resetting ETL landing tables
        conn.execute(text(f'TRUNCATE TABLE data_raw."{raw_table_name}" RESTART IDENTITY;'))
    logger.info(f"✨ [PURGE SUCCESS] data_raw.\"{raw_table_name}\" has been emptied cleanly.")

def run_cleaning_pipeline():
    logger.info("🎬 [Phase 2/3] Starting Staging Cleaning Pipeline...")
    raw_tables = discover_raw_tables()
    engine = get_engine()
    
    for raw_table in raw_tables:
        if raw_table.startswith('temp_'): 
            continue

        clean_dataset_name = raw_table.replace('entity_', '')
        if clean_dataset_name == "staff_register":
            clean_dataset_name = "Staff_Register"
        
        chunk_idx = 0
        try:
            # Process table in chunks sequentially
            for chunk_df in pd.read_sql_table(raw_table, con=engine, schema='data_raw', chunksize=2500):
                chunk_idx += 1
                logger.info(f"📦 Processing {raw_table} [Chunk {chunk_idx}]...")
                
                df_cleaned = clean_and_vectorize_media(chunk_df, clean_dataset_name)
                upsert_to_staging(df_cleaned, clean_dataset_name, chunk_idx)
            
            # --- SAFE CLEARANCE ZONE ---
            # If the loop naturally finishes without raising an Exception, we proceed to purge.
            if chunk_idx > 0:
                clear_raw_table(engine, raw_table)
            else:
                logger.info(f"ℹ️ Table data_raw.\"{raw_table}\" was empty. No data clearance required.")

        except Exception as table_error:
            # Catch failures at the individual table level to prevent catastrophic drops
            logger.error(
                f"❌ [DATA PROTECTION] Critical failure processing table data_raw.\"{raw_table}\" on chunk {chunk_idx}. "
                f"Purge operation aborted to prevent data loss. Error: {table_error}"
            )
            raise table_error
            
    logger.info("🏁 [Phase 2/3] Cleaning Pipeline Operations Completed. Handoff to Loader...")

if __name__ == "__main__":
    try:
        run_cleaning_pipeline()
    except Exception as e:
        logger.critical(f"💥 [Phase 2/3] Staging pipeline halted: {e}", exc_info=True)
