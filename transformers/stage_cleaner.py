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
    """Deferred engine creation initialization to avoid early import configuration block errors."""
    load_dotenv("/opt/data_platform/config/.env")
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        raise ValueError("❌ DATABASE_URL missing from deployment configuration environment mappings.")
    return create_engine(db_uri, pool_pre_ping=True)

def discover_raw_tables():
    engine = get_engine()
    inspector = inspect(engine)
    return inspector.get_table_names(schema='data_raw')

def clean_and_vectorize_media(df, dataset_name):
    if df.empty:
        return df
        
    df.columns = [c.lower().strip() for c in df.columns]
    
    id_col = None
    for candidate in ['__id', 'id', 'uuid']:
        if candidate in df.columns:
            id_col = candidate
            break
            
    if not id_col:
        return df

    for col in df.columns:
        sample = df[col].dropna().astype(str).head(15)
        if any(val.lower().endswith(MEDIA_EXTENSIONS) for val in sample):
            proxy_col_name = f"{col}_proxy_url"
            logger.info(f"🔗 Vectorizing media proxy mappings onto column: '{col}'")
            
            df[proxy_col_name] = df.apply(
                lambda r: f"{PROXY_BASE_URL}/{dataset_name}/{r[id_col]}/{r[col]}"
                if pd.notna(r[col]) and str(r[col]).strip() != "" else None,
                axis=1
            )
            
    return df

def upsert_to_staging(df, table_name):
    if df.empty:
        return
        
    engine = get_engine()
    staging_table = f"stage_{table_name}"
    conflict_key = '__id' if '__id' in df.columns else 'id' if 'id' in df.columns else None
    
    if not conflict_key:
        return

    with engine.begin() as schema_conn:
        schema_conn.execute(text("CREATE SCHEMA IF NOT EXISTS data_staging;"))
        df.head(0).to_sql(staging_table, schema_conn, schema='data_staging', if_exists='append', index=False)
        schema_conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{staging_table}_{conflict_key}_idx" ON data_staging."{staging_table}" ("{conflict_key}");'))

    temp_holder = f"temp_{staging_table}_{int(pd.Timestamp.now().timestamp())}"
    with engine.begin() as conn:
        # 🚨 FIX: Added chunksize here as well to protect the database insert
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

def run_cleaning_pipeline():
    logger.info("🎬 Initializing Staging Cleaning Pipeline Operations...")
    raw_tables = discover_raw_tables()
    
    for raw_table in raw_tables:
        if raw_table.startswith('temp_'):
            continue
            
        logger.info(f"📦 Processing raw table {raw_table} in safe memory chunks...")
        engine = get_engine()
        clean_dataset_name = raw_table.replace("entity_", "")
        
        # 🚨 FIX: Chunking the data read. Instead of loading everything into RAM at once,
        # it streams 2,500 rows at a time, processes them, and flushes memory.
        for chunk_df in pd.read_sql_table(raw_table, con=engine, schema='data_raw', chunksize=2500):
            df_cleaned = clean_and_vectorize_media(chunk_df, clean_dataset_name)
            upsert_to_staging(df_cleaned, clean_dataset_name)
            
    logger.info("✅ Cleaning Pipeline Operations Completed.")

if __name__ == "__main__":
    try:
        run_cleaning_pipeline()
    except Exception as e:
        logger.critical(f"💥 Staging pipeline halted: {e}", exc_info=True)
