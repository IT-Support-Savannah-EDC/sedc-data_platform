import os
import sys
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv("/opt/data_platform/config/.env")

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

engine = create_engine(os.getenv("DATABASE_URL"))

def get_shared_columns(conn, source_schema, source_table, target_schema, target_table):
    """Finds the overlapping columns between raw and refined to prevent insertion crashes."""
    query = text("""
        SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t
    """)
    raw_cols = {row[0] for row in conn.execute(query, {"s": source_schema, "t": source_table}).fetchall()}
    refined_cols = {row[0] for row in conn.execute(query, {"s": target_schema, "t": target_table}).fetchall()}
    return list(raw_cols.intersection(refined_cols))

def direct_upsert(raw_table, refined_table, pk_column="__id"):
    logger.info(f"🔄 Moving data from data_raw.{raw_table} -> data_refined.{refined_table}...")
    
    with engine.begin() as conn:
        # 1. Verify target table exists
        exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = 'data_refined' AND tablename = :t)")
        if not conn.execute(exists_query, {"t": refined_table}).scalar():
            logger.warning(f"⚠️ Target table data_refined.{refined_table} does not exist. Skipping.")
            return

        # 2. Get shared columns
        shared_cols = get_shared_columns(conn, "data_raw", raw_table, "data_refined", refined_table)
        if not shared_cols:
            logger.warning(f"⚠️ No matching columns found for {raw_table}. Skipping.")
            return

        # 3. Build dynamic SQL for the Upsert
        col_str = ", ".join([f'"{c}"' for c in shared_cols])
        
        # Only update columns that aren't the Primary Key
        update_cols = [c for c in shared_cols if c != pk_column]
        if update_cols:
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            do_clause = f"DO UPDATE SET {update_str}"
        else:
            do_clause = "DO NOTHING"

        upsert_query = text(f"""
            INSERT INTO "data_refined"."{refined_table}" ({col_str})
            SELECT {col_str} FROM "data_raw"."{raw_table}"
            ON CONFLICT ("{pk_column}") {do_clause}
        """)
        
        result = conn.execute(upsert_query)
        logger.info(f"✅ Upsert complete for {refined_table}. Rows affected: {result.rowcount}")

if __name__ == "__main__":
    logger.info("🎬 Initializing Raw-to-Refined Direct Load...")
    try:
        # Define the tables you want to move from raw to refined here.
        # Format: (raw_table_name, refined_table_name, primary_key)
        pipelines = [
            ("entity_customer_db", "customer_db", "__id"),
            # Add future tables here: ("entity_meter_installation", "meter_installations", "__id")
        ]
        
        for raw, refined, pk in pipelines:
            direct_upsert(raw, refined, pk)
            
    except Exception as e:
        logger.critical(f"💥 Load engine halted: {e}", exc_info=True)
        sys.exit(1)
