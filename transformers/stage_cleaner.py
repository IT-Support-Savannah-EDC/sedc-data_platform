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

def upsert_to_staging(df, table_name, chunk_idx):
    if df.empty:
        return
        
    engine = get_engine()
    staging_table = f"stage_{table_name}"
    conflict_key = next((c for c in ['__id', 'id'] if c in df.columns), None)
    
    if not conflict_key:
        logger.error(f"❌ Aborting upsert for {table_name}: No valid conflict key.")
        return

    # Transactional Log
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

def run_cleaning_pipeline():
    logger.info("🎬 Starting Staging Cleaning Pipeline...")
    raw_tables = discover_raw_tables()
    
    for raw_table in raw_tables:
        if raw_table.startswith('temp_'): continue
            
        clean_dataset_name = raw_table.replace("entity_", "")
        engine = get_engine()
        
        chunk_idx = 0
        for chunk_df in pd.read_sql_table(raw_table, con=engine, schema='data_raw', chunksize=2500):
            chunk_idx += 1
            logger.info(f"📦 Processing {raw_table} [Chunk {chunk_idx}]...")
            
            df_cleaned = clean_and_vectorize_media(chunk_df, clean_dataset_name)
            upsert_to_staging(df_cleaned, clean_dataset_name, chunk_idx)
            
    logger.info("🏁 Cleaning Pipeline Operations Completed.")

if __name__ == "__main__":
    try:
        run_cleaning_pipeline()
    except Exception as e:
        logger.critical(f"💥 Staging pipeline halted: {e}", exc_info=True)
