import os
import sys
import logging
from sqlalchemy import create_engine, text, inspect
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
    """Finds intersecting columns between raw and refined tables."""
    query = text("""
        SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t
    """)
    raw_cols = {row[0] for row in conn.execute(query, {"s": source_schema, "t": source_table}).fetchall()}
    refined_cols = {row[0] for row in conn.execute(query, {"s": target_schema, "t": target_table}).fetchall()}
    return list(raw_cols.intersection(refined_cols))

def sync_refined_schema(conn, raw_table, refined_table):
    """Ensures target table exists and matches columns found in raw."""
    # 1. If target table doesn't exist, create it with a primary key constraint
    exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = 'data_refined' AND tablename = :t)")
    if not conn.execute(exists_query, {"t": refined_table}).scalar():
        logger.info(f"✨ Target table data_refined.{refined_table} missing. Building structure...")
        conn.execute(text(f'CREATE TABLE "data_refined"."{refined_table}" (LIKE "data_raw"."{raw_table}" INCLUDING ALL);'))
        
        # Explicitly enforce Primary Key on __id to anchor the upsert
        try:
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD CONSTRAINT "{refined_table}_pk" PRIMARY KEY ("__id");'))
        except Exception:
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{refined_table}__id_idx" ON "data_refined"."{refined_table}" ("__id");'))
        return

    # 2. Schema Evolution: If it does exist, catch any new columns added by the field teams
    raw_col_query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_raw' AND table_name = :t")
    refined_col_query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_refined' AND table_name = :t")
    
    raw_cols = {row[0] for row in conn.execute(raw_col_query, {"t": raw_table}).fetchall()}
    refined_cols = {row[0] for row in conn.execute(refined_col_query, {"t": refined_table}).fetchall()}
    
    missing_cols = [col for col in raw_cols if col not in refined_cols]
    if missing_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(missing_cols)} new columns to data_refined.{refined_table}")
        for col in missing_cols:
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD COLUMN "{col}" TEXT;'))

def move_and_clear_table(raw_table, pk_column="__id"):
    # Determine the clean warehouse name by dropping the landing prefix 'entity_'
    refined_table = raw_table.replace("entity_", "")
    
    with engine.begin() as conn:
        # Check if raw table has any data before doing anything
        row_check = conn.execute(text(f'SELECT COUNT(1) FROM "data_raw"."{raw_table}";')).scalar()
        if row_check == 0:
            logger.info(f"⏸️ data_raw.{raw_table} is empty. Skipping processing block.")
            return

        # Ensure schema structure alignment
        sync_refined_schema(conn, raw_table, refined_table)
        
        # Calculate matching operational columns
        shared_cols = get_shared_columns(conn, "data_raw", raw_table, "data_refined", refined_table)
        col_str = ", ".join([f'"{c}"' for c in shared_cols])
        
        update_cols = [c for c in shared_cols if c != pk_column]
        if update_cols:
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            do_clause = f"DO UPDATE SET {update_str}"
        else:
            do_clause = "DO NOTHING"

        # 1. Execute strict relational upsert via structural keys
        upsert_query = text(f"""
            INSERT INTO "data_refined"."{refined_table}" ({col_str})
            SELECT {col_str} FROM "data_raw"."{raw_table}"
            ON CONFLICT ("{pk_column}") {do_clause};
        """)
        result = conn.execute(upsert_query)
        logger.info(f"📥 Upsert complete for data_refined.{refined_table}. Rows affected: {result.rowcount}")

        # 2. CLEAR SOURCE: Truncate raw landing zone table cleanly now that data is safe
        conn.execute(text(f'TRUNCATE TABLE "data_raw"."{raw_table}";'))
        logger.info(f"🧹 Landing Zone Cleared: data_raw.{raw_table} has been emptied.")

if __name__ == "__main__":
    logger.info("🎬 Initializing Dynamic Raw-to-Refined Pipeline...")
    try:
        # Automatically discover all staging tables inside your data_raw workspace
        inspector = inspect(engine)
        raw_tables = inspector.get_table_names(schema="data_raw")
        
        # Filter exclusively for target operational entities
        target_tables = [t for t in raw_tables if t.startswith("entity_")]
        
        if not target_tables:
            logger.info("⏸️ No operational 'entity_' tables detected inside data_raw.")
            sys.exit(0)
            
        logger.info(f"🔍 Discovered {len(target_tables)} tables in landing zone to process.")
        for table in target_tables:
            move_and_clear_table(table, pk_column="__id")
            
        logger.info(f"✅ All {len(target_tables)} tables successfully processed and cleared.")
            
    except Exception as e:
        logger.critical(f"💥 Load engine halted: {e}", exc_info=True)
        sys.exit(1)
