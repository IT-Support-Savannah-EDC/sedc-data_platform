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

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

def get_shared_columns(conn, source_schema, source_table, target_schema, target_table):
    """Finds the intersection of columns present in both source and target tables."""
    query = text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = :s AND table_name = :t
    """)
    source_cols = {row[0] for row in conn.execute(query, {"s": source_schema, "t": source_table}).fetchall()}
    refined_cols = {row[0] for row in conn.execute(query, {"s": target_schema, "t": target_table}).fetchall()}
    return list(source_cols.intersection(refined_cols))

def sync_refined_schema(conn, staging_table, refined_table):
    """Ensures target table exists, evolves with new staging columns, and preserves audit watermarks."""
    # 1. Handle base table missing (Clones structure from data_staging to preserve proxy columns)
    exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = 'data_refined' AND tablename = :t)")
    if not conn.execute(exists_query, {"t": refined_table}).scalar():
        logger.info(f"✨ Target table data_refined.{refined_table} missing. Building structure...")
        conn.execute(text(f'CREATE TABLE "data_refined"."{refined_table}" (LIKE "data_staging"."{staging_table}" INCLUDING ALL);'))
        try:
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD CONSTRAINT "{refined_table}_pk" PRIMARY KEY ("__id");'))
        except Exception:
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{refined_table}__id_idx" ON "data_refined"."{refined_table}" ("__id");'))

    # 2. Schema Evolution (Compares data_refined against data_staging)
    staging_col_query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_staging' AND table_name = :t")
    refined_col_query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_refined' AND table_name = :t")
    
    staging_cols = {row[0] for row in conn.execute(staging_col_query, {"t": staging_table}).fetchall()}
    refined_cols = {row[0] for row in conn.execute(refined_col_query, {"t": refined_table}).fetchall()}
    
    missing_cols = [col for col in staging_cols if col not in refined_cols]
    if missing_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(missing_cols)} columns to data_refined.{refined_table}")
        for col in missing_cols:
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD COLUMN "{col}" TEXT;'))

    # 3. Watermark Injection Protection
    for col in ["__createdat", "__updatedat"]:
        if col not in refined_cols:
            logger.info(f"➕ Injecting mandatory watermark tracking column '{col}' into data_refined.{refined_table}")
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD COLUMN "{col}" TIMESTAMP WITH TIME ZONE DEFAULT NOW();'))

def move_and_clear_table(staging_table, pk_column="__id"):
    """Upserts cleaned data from staging to refined schema, then flushes the staging table."""
    # Convert 'stage_staff_register' -> 'staff_register'
    refined_table = staging_table.replace("stage_", "")
    
    with engine.begin() as conn:
        row_check = conn.execute(text(f'SELECT COUNT(1) FROM "data_staging"."{staging_table}";')).scalar()
        if row_check == 0:
            logger.info(f"ℹ️ Table data_staging.{staging_table} is empty. Skipping refinement process.")
            return

        # Pass staging_table as the structure source
        sync_refined_schema(conn, staging_table, refined_table)
        
        # Read mutual schema alignment between data_staging and data_refined
        shared_cols = get_shared_columns(conn, "data_staging", staging_table, "data_refined", refined_table)
        
        # Filter out audit timestamps from shared matching list to manually handle them cleanly
        shared_cols = [c for c in shared_cols if c not in ["__createdat", "__updatedat"]]
        
        col_str = ", ".join([f'"{c}"' for c in shared_cols])
        insert_cols = col_str + ', "__createdat", "__updatedat"'
        select_cols = col_str + ', NOW(), NOW()'
        
        update_cols = [c for c in shared_cols if c != pk_column]
        if update_cols:
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            update_str += ', "__updatedat" = NOW()'  # Explicitly advance clock on overwrite
            do_clause = f"DO UPDATE SET {update_str}"
        else:
            do_clause = "DO NOTHING"

        upsert_query = text(f"""
            INSERT INTO "data_refined"."{refined_table}" ({insert_cols})
            SELECT {select_cols} FROM "data_staging"."{staging_table}"
            ON CONFLICT ("{pk_column}") {do_clause};
        """)
        
        result = conn.execute(upsert_query)
        logger.info(f"📥 Upsert complete for data_refined.{refined_table}. Rows affected: {result.rowcount}")

        # Safe purge step: Only clear data_staging after the upsert succeeds
        conn.execute(text(f'TRUNCATE TABLE "data_staging"."{staging_table}" RESTART IDENTITY;'))
        logger.info(f"🧹 Staging Zone Cleared: data_staging.{staging_table} has been emptied.")

if __name__ == "__main__":
    logger.info("🎬 Initializing Dynamic Staging-to-Refined Pipeline...")
    try:
        inspector = inspect(engine)
        # Scan data_staging schema instead of data_raw
        staging_tables = inspector.get_table_names(schema="data_staging")
        target_tables = [t for t in staging_tables if t.startswith("stage_")]
        
        if not target_tables:
            logger.info("⏸️ Staging layer empty. No data to load. Exiting.")
            sys.exit(0)
            
        for table in target_tables:
            move_and_clear_table(table, pk_column="__id")
            
        logger.info("✅ Core dynamic refinement loading sequence complete.")
            
    except Exception as e:
        logger.critical(f"💥 Load engine halted: {e}", exc_info=True)
        sys.exit(1)
