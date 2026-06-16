import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add framework root to sys.path to ensure seamless local cross-directory imports
sys.path.append("/opt/data_platform")

from transformers.stage_cleaner import transform_media_payloads

load_dotenv("/opt/data_platform/config/.env")

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

def load_all_media_assets():
    """
    Reads raw payloads from data_raw, processes them via stage_cleaner, 
    and writes out isolated, deduplicated media tracking tables.
    """
    logger.info("🎬 Initializing Media Transformation and Load Sequence...")
    
    # 1. Extract raw entries from the data_raw landing zone
    with engine.connect() as conn:
        raw_payloads = conn.execute(text("""
            SELECT form_id, group_name, raw_json 
            FROM data_raw.staging_media_payloads
        """)).mappings().all()
        
    if not raw_payloads:
        logger.info("⏸️ No raw media payloads found in data_raw. Process skipped.")
        return

    # 2. Transform raw JSON strings into clean DataFrames via stage_cleaner
    df_main, dict_repeats = transform_media_payloads(raw_payloads)
    
    with engine.begin() as conn:
        # --- PHASE A: WRITE MAIN FORM ATTACHMENTS ---
        if not df_main.empty:
            for form_id, sub_df in df_main.groupby('form_id'):
                table_name = f"data_refined.{form_id}_main_media"
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        media_id BIGSERIAL PRIMARY KEY,
                        submission_uuid TEXT NOT NULL,
                        form_field_name TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        live_url TEXT NOT NULL,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT uq_{form_id}_main_media UNIQUE (submission_uuid, form_field_name)
                    );
                """))
                
                for _, row in sub_df.iterrows():
                    conn.execute(text(f"""
                        INSERT INTO {table_name} (submission_uuid, form_field_name, file_name, live_url)
                        VALUES (:submission_uuid, :form_field_name, :file_name, :live_url)
                        ON CONFLICT (submission_uuid, form_field_name) DO NOTHING;
                    """), row.to_dict())
                logger.info(f"🚀 Refined Main media table updated: {table_name}")

        # --- PHASE B: WRITE REPEAT GROUP ATTACHMENTS ---
        for target_table_suffix, sub_df in dict_repeats.items():
            if sub_df.empty:
                continue
                
            table_name = f"data_refined.{target_table_suffix}_media"
            
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    media_id BIGSERIAL PRIMARY KEY,
                    repeat_row_uuid TEXT NOT NULL,
                    submission_uuid TEXT NOT NULL,
                    form_field_name TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    live_url TEXT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_{target_table_suffix}_media UNIQUE (repeat_row_uuid, form_field_name)
                );
            """))
            
            for _, row in sub_df.iterrows():
                conn.execute(text(f"""
                    INSERT INTO {table_name} (repeat_row_uuid, submission_uuid, form_field_name, file_name, live_url)
                    VALUES (:repeat_row_uuid, :submission_uuid, :form_field_name, :file_name, :live_url)
                    ON CONFLICT (repeat_row_uuid, form_field_name) DO NOTHING;
                """), row.to_dict())
            logger.info(f"🚀 Refined Repeat media table updated: {table_name}")

        # --- PHASE C: HOUSEKEEPING AND CLEANUP ---
        # Empty the landing table to keep subsequent pipeline steps fast and efficient
        conn.execute(text("TRUNCATE TABLE data_raw.staging_media_payloads;"))
        logger.info("实时 🧹 Cleared data_raw.staging_media_payloads source records.")

if __name__ == "__main__":
    try:
        load_all_media_assets()
        logger.info("✅ Media processing step completed successfully.")
    except Exception as e:
        logger.critical(f"💥 Media loader halted due to an unhandled exception: {e}", exc_info=True)
        sys.exit(1)
