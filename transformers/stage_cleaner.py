import os
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_engine():
    # Load env every time we request the engine to ensure it's fresh
    load_dotenv("/opt/data_platform/config/.env")
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        raise ValueError("DATABASE_URL not found in .env file!")
    return create_engine(db_uri, pool_pre_ping=True)

# Define your application server production proxy route prefix
PROXY_BASE_URL = "http://134.209.178.35:5050/media"
MEDIA_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.mp4', '.3gp', '.wav', '.mp3')

def discover_raw_tables():
    """Scans the data_raw schema for ingest tables to process."""
    engine = get_engine
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='data_raw')
    return tables

def clean_and_vectorize_media(df, dataset_name):
    """
    Standardizes dataframe attributes and runs vectorized mapping over any 
    columns containing media filenames to produce valid local application proxy urls.
    """
    if df.empty:
        return df
        
    # Standardize columns to lower case, stripping out spaces/symbols
    df.columns = [c.lower().strip() for c in df.columns]
    
    # Identify unique identifier anchors
    id_col = None
    for candidate in ['__id', 'id', 'uuid']:
        if candidate in df.columns:
            id_col = candidate
            break
            
    if not id_col:
        logger.warning(f"⚠️ No entity tracking ID anchor found for {dataset_name}. Skipping URL generation.")
        return df

    # Find columns containing files based on value pattern extensions
    for col in df.columns:
        # Check a sample head chunk to see if files are present
        sample = df[col].dropna().astype(str).head(15)
        if any(val.lower().endswith(MEDIA_EXTENSIONS) for val in sample):
            proxy_col_name = f"{col}_proxy_url"
            logger.info(f"🔗 Vectorizing media proxy mappings onto column: '{col}' -> '{proxy_col_name}'")
            
            # Use Pandas vectorized string mapping to instantly construct thousands of links
            df[proxy_col_name] = df.apply(
                lambda r: f"{PROXY_BASE_URL}/{dataset_name}/{r[id_col]}/{r[col]}"
                if pd.notna(r[col]) and str(r[col]).strip() != "" else None,
                axis=1
            )
            
    return df

def upsert_to_staging(df, table_name):
    """Atomic multi-row upsert directly into the staging schema environment."""
    if df.empty:
        return
        
    staging_table = f"stage_{table_name}"
    # Target our unique id column as the merge anchor
    conflict_key = '__id' if '__id' in df.columns else 'id' if 'id' in df.columns else None
    
    if not conflict_key:
        logger.error(f"❌ Cannot map atomic upsert for table {staging_table} without a primary key.")
        return

    # Ensure staging schema framework exists space
    with engine.begin() as schema_conn:
        schema_conn.execute(text("CREATE SCHEMA IF NOT EXISTS data_staging;"))
        # Initialize an empty mirror blueprint if running for the first time
        df.head(0).to_sql(staging_table, schema_conn, schema='data_staging', if_exists='append', index=False)
        
        # Enforce unique index requirement constraints for the UPSERT logic
        schema_conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS "{staging_table}_{conflict_key}_idx" 
            ON data_staging."{staging_table}" ("{conflict_key}");
        """))

    # Execute dynamic upsert using a local temporary staging table
    temp_holder = f"temp_{staging_table}"
    with engine.begin() as conn:
        df.to_sql(temp_holder, conn, if_exists='replace', index=False)
        
        cols_str = ", ".join([f'"{c}"' for c in df.columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != conflict_key])
        
        upsert_sql = f"""
            INSERT INTO data_staging."{staging_table}" ({cols_str})
            SELECT {cols_str} FROM "{temp_holder}"
            ON CONFLICT ("{conflict_key}") DO UPDATE SET {update_str};
        """
        conn.execute(text(upsert_sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{temp_holder}"'))
        logger.info(f"🚀 Fortified data_staging.{staging_table} with {len(df)} cleaned records successfully.")

def run_cleaning_pipeline():
    logger.info("🎬 Initializing Staging Cleaning Pipeline Operations...")
    raw_tables = discover_raw_tables()
    
    if not raw_tables:
        logger.info("⏸️ No raw data ingest objects found inside data_raw schema.")
        return
        
    for raw_table in raw_tables:
        logger.info(f"📦 Extracting data from raw source space: data_raw.{raw_table}")
        
        # Extract everything out from raw layer ingest space
        df_raw = pd.read_sql_table(raw_table, con=engine, schema='data_raw')
        
        # Clean data structures and dynamically construct proxy paths via Vectorization
        # e.g., if raw_table is "entity_staff_register", dataset name passed is "staff_register"
        clean_dataset_name = raw_table.replace("entity_", "")
        df_cleaned = clean_and_vectorize_media(df_raw, clean_dataset_name)
        
        # Persist down into data_staging
        upsert_to_staging(df_cleaned, clean_dataset_name)

if __name__ == "__main__":
    run_cleaning_pipeline()
