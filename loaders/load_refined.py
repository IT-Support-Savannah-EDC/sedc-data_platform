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

def discover_conflict_key(inspector, table_name, schema="data_raw"):
    """
    Dynamically resolves the optimal conflict/primary key for a table.
    Checks DB primary keys, unique constraints, unique indexes, and falls back to structured naming conventions.
    """
    # 1. Check for physical primary keys assigned in database
    pk_info = inspector.get_pk_constraint(table_name, schema=schema)
    if pk_info and pk_info.get("constrained_columns"):
        return pk_info["constrained_columns"][0]
    
    # 2. Check for explicit unique constraints 
    unique_constraints = inspector.get_unique_constraints(table_name, schema=schema)
    for constraint in unique_constraints:
        if constraint.get("column_names"):
            return constraint["column_names"][0]
            
    # 3. CRITICAL FIX: Check for Unique Indexes (which extract_odk.py generates)
    indexes = inspector.get_indexes(table_name, schema=schema)
    for idx in indexes:
        if idx.get("unique") and idx.get("column_names"):
            # Ensure we only grab single-column unique indexes
            if len(idx["column_names"]) == 1:
                return idx["column_names"][0]

    # 4. Structural inspection fallbacks for ODK/Entity naming schemas
    columns = [c["name"] for c in inspector.get_columns(table_name, schema=schema)]
    
    # Isolate child sub-tables from main tables and entities
    is_sub_table = table_name.startswith("form_") and not table_name.endswith("_main")
    
    if is_sub_table:
        # CRITICAL FIX: Sub-tables must NOT use the parent's '_id' or '__id'
        if "id" in columns: return "id"
        if "subid" in columns: return "subid"
        
        # Find any ID column that isn't a known parent/system identifier
        valid_ids = [c for c in columns if c.endswith('id') and c not in ['_id', '__id', '_submissions_id']]
        if valid_ids:
            return valid_ids[0]
    else:
        # Main forms and Entities safely use top-level IDs
        if "__id" in columns: return "__id"
        if "_id" in columns: return "_id"
        if "meta_instanceid" in columns: return "meta_instanceid"
        
    # Absolute generic fallback matching generic identifier patterns
    generic_ids = [c for c in columns if 'id' in c.lower() and c not in ['_id', '__id']]
    return generic_ids[0] if generic_ids else columns[0]
    
def sync_refined_schema(conn, raw_table, refined_table, conflict_key):
    """Ensures target table exists, evolves with new raw columns, and preserves audit watermarks."""
    # 1. Handle base table missing (Clones structure from data_raw)
    exists_query = text("SELECT exists (SELECT FROM pg_tables WHERE schemaname = 'data_refined' AND tablename = :t)")
    if not conn.execute(exists_query, {"t": refined_table}).scalar():
        logger.info(f"✨ Target table data_refined.{refined_table} missing. Building structure...")
        conn.execute(text(f'CREATE TABLE "data_refined"."{refined_table}" (LIKE "data_raw"."{raw_table}" INCLUDING ALL);'))
        
        # Enforce target indexing matching the source conflict key to support ON CONFLICT handling
        try:
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD CONSTRAINT "{refined_table}_pk" PRIMARY KEY ("{conflict_key}");'))
        except Exception:
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{refined_table}_{conflict_key}_idx" ON "data_refined"."{refined_table}" ("{conflict_key}");'))

    # 2. Schema Evolution (Compares data_refined against data_raw)
    raw_col_query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_raw' AND table_name = :t")
    refined_col_query = text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_refined' AND table_name = :t")
    
    raw_cols = {row[0] for row in conn.execute(raw_col_query, {"t": raw_table}).fetchall()}
    refined_cols = {row[0] for row in conn.execute(refined_col_query, {"t": refined_table}).fetchall()}
    
    missing_cols = [col for col in raw_cols if col not in refined_cols]
    if missing_cols:
        logger.info(f"🧬 Schema Evolution: Adding {len(missing_cols)} columns to data_refined.{refined_table}")
        for col in missing_cols:
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD COLUMN "{col}" TEXT;'))

    # 3. Watermark Injection Protection
    for col in ["__createdat", "__updatedat"]:
        if col not in refined_cols:
            logger.info(f"➕ Injecting mandatory watermark tracking column '{col}' into data_refined.{refined_table}")
            conn.execute(text(f'ALTER TABLE "data_refined"."{refined_table}" ADD COLUMN "{col}" TIMESTAMP WITH TIME ZONE DEFAULT NOW();'))

def upsert_raw_to_refined(inspector, raw_table):
    """Upserts cleaned data from raw to refined schema dynamically without modifying the immutable raw history."""
    refined_table = raw_table  # Direct 1:1 mapping mapping for seamless traceability
    
    # Resolve conflict key dynamically for this specific table structure
    conflict_key = discover_conflict_key(inspector, raw_table, schema="data_raw")
    logger.info(f"🔍 Table tracking: data_raw.{raw_table} uses conflict key: '{conflict_key}'")

    with engine.begin() as conn:
        row_check = conn.execute(text(f'SELECT COUNT(1) FROM "data_raw"."{raw_table}";')).scalar()
        if row_check == 0:
            logger.info(f"ℹ️ Table data_raw.{raw_table} is empty. Skipping refinement process.")
            return

        # Pass raw_table as structural architecture reference
        sync_refined_schema(conn, raw_table, refined_table, conflict_key)
        
        # Read mutual schema alignment between data_raw and data_refined
        shared_cols = get_shared_columns(conn, "data_raw", raw_table, "data_refined", refined_table)
        
        # Filter out audit timestamps from shared matching list to manually handle them cleanly
        shared_cols = [c for c in shared_cols if c not in ["__createdat", "__updatedat"]]
        
        col_str = ", ".join([f'"{c}"' for c in shared_cols])
        insert_cols = col_str + ', "__createdat", "__updatedat"'
        select_cols = col_str + ', NOW(), NOW()'
        
        update_cols = [c for c in shared_cols if c != conflict_key]
        if update_cols:
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            update_str += ', "__updatedat" = NOW()'  # Explicitly advance clock on overwrite
            do_clause = f"DO UPDATE SET {update_str}"
        else:
            do_clause = "DO NOTHING"

        upsert_query = text(f"""
            INSERT INTO "data_refined"."{refined_table}" ({insert_cols})
            SELECT {select_cols} FROM "data_raw"."{raw_table}"
            ON CONFLICT ("{conflict_key}") {do_clause};
        """)
        
        result = conn.execute(upsert_query)
        logger.info(f"📥 Upsert complete for data_refined.{refined_table}. Rows affected: {result.rowcount}")
        logger.info(f"🛡️ Raw ledger preserved: data_raw.{raw_table} data retained intact.")

if __name__ == "__main__":
    logger.info("🎬 Initializing Dynamic Raw-to-Refined Production Pipeline...")
    try:
        inspector = inspect(engine)
        
        # Scan complete data_raw schema comprehensively
        raw_tables = inspector.get_table_names(schema="data_raw")
        
        if not raw_tables:
            logger.info("⏸️ Raw data layer empty. No data to process. Exiting.")
            sys.exit(0)
            
        logger.info(f"Total discovered tables for processing: {len(raw_tables)}")
        for table in raw_tables:
            upsert_raw_to_refined(inspector, table)
            
        logger.info("✅ Production dynamic refinement loading sequence complete.")
            
    except Exception as e:
        logger.critical(f"💥 Load engine halted: {e}", exc_info=True)
        sys.exit(1)
