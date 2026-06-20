import re
import os
import io
import logging
from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv("/opt/data_platform/config/.env")

app = Flask(__name__)

# Fetch standard credentials target directly from project env configurations
DATABASE_URL = os.getenv("DATABASE_URL") # Ensure this points to 'sedc_db'
engine = create_engine(DATABASE_URL)

def sanitize_identifier(name):
    """Prevents SQL injection vulnerabilities in schema and table definitions."""
    if not name:
        return None
    return re.sub(r'[^a-zA-Z0-9_]', '', name).lower()

@app.route('/webhook/sheets', methods=['POST'])
def receive_sheets_data():
    payload = request.json or {}
    
    # 1. Setup in-memory logging capture loop
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    logger = logging.getLogger("sheets_uploader")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    try:
        raw_data = payload.get('data', [])
        schema_param = payload.get('target_schema', 'data_raw_sheets')
        table_param = payload.get('target_table')
        mode_param = payload.get('write_mode', 'append') # 'append' or 'replace'
        
        # 2. Extract and Sanitize Target Architecture Parameters
        schema_name = sanitize_identifier(schema_param)
        table_name = sanitize_identifier(table_param)
        
        if not table_name:
            logger.error("Invalid or missing 'target_table' parameter.")
            return jsonify({
                "status": "error", 
                "message": "Missing destination target table parameter.",
                "logs": log_capture.getvalue().splitlines()
            }), 400
            
        if mode_param not in ['append', 'replace']:
            logger.warning(f"Unrecognized write mode '{mode_param}'. Falling back to safe append pattern.")
            mode_param = 'append'

        if not raw_data:
            logger.info("Empty data array payload encountered. Zero operations written.")
            return jsonify({"status": "success", "message": "No new records detected.", "logs": ["No data sent."]}), 200

        # 3. Process to DataFrame representation
        df = pd.DataFrame(raw_data)
        logger.info(f"Ingested {len(df)} records into memory matrix from client stream.")

        # 4. Enforce Schema Creation Guards
        with engine.begin() as conn:
            logger.info(f"Ensuring target schema storage '{schema_name}' exists...")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))

        # 5. Route Matrix Payload to Postgres Database
        logger.info(f"Dispatching upsert operation to {schema_name}.{table_name} with strategy: {mode_param}")
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists=mode_param,
            index=False
        )
        
        logger.info("🚀 Database transaction successfully committed.")
        return jsonify({
            "status": "success",
            "message": f"Successfully loaded data into {schema_name}.{table_name}.",
            "logs": log_capture.getvalue().splitlines()
        }), 200

    except Exception as e:
        logger.error(f"Execution error crashed payload loop: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Transaction failure: {str(e)}",
            "logs": log_capture.getvalue().splitlines()
        }), 500
        
    finally:
        logger.removeHandler(handler)

if __name__ == '__main__':
    # Listen on internal staging proxy wrapper port
    app.run(host='0.0.0.0', port=5000, debug=False)
