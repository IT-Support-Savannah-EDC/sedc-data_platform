import os
import requests
import urllib.parse
from pathlib import Path
from flask import Flask, Response, stream_with_context, jsonify
from sqlalchemy import create_engine, text
from pyodk.client import Client
from dotenv import load_dotenv
from requests.utils import quote

env_path = Path('/opt/data_platform/config/.env')
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)

# Constants & Connection Engines
DB_URI = os.getenv("DATABASE_URL")
PROJECT_ID = os.getenv("PROJECT_ID", "1")
BASE_ODK_URL = "https://central.savannahedc.com/v1"

engine = create_engine(DB_URI, pool_pre_ping=True)

# Use an absolute path to ensure pyodk finds it
CONFIG_PATH = "/opt/data_platform/config/.pyodk_config.toml"
client = Client(config_path=CONFIG_PATH)

def discover_lineage(dataset_name, entity_id, filename):
    """
    Checks the local database cache for ODK lineage metadata.
    If missing, queries ODK Central's entity versions endpoint, parses out
    the form and submission details, and saves them to the cache.
    """
    # 1. Query Cache
    with engine.connect() as conn:
        cache_query = text("""
            SELECT resolved_form_id, resolved_submission_uuid 
            FROM data_staging.media_lineage_cache 
            WHERE entity_id = :e AND filename = :f
        """)
        cached = conn.execute(cache_query, {"e": entity_id, "f": filename}).fetchone()
        
        if cached:
            return cached[0], cached[1]

    # 2. Cache Miss: Interrogate ODK Central
    print(f"🔍 [CACHE MISS] Tracking lineage for Entity {entity_id} on ODK Central...")
    versions_endpoint = f"projects/{PROJECT_ID}/datasets/{dataset_name}/entities/{entity_id}/versions"
    
    try:
        response = client.get(versions_endpoint)
        response.raise_for_status()
        
        for change_event in response.json():
            source = change_event.get("source", {})
            submission_data = source.get("submission")
            
            if submission_data:
                form_id = submission_data.get("xmlFormId")
                sub_uuid = submission_data.get("instanceId")
                
                if form_id and sub_uuid:
                    # Write back to persistent cache layer
                    with engine.begin() as write_conn:
                        write_conn.execute(text("""
                            INSERT INTO data_staging.media_lineage_cache 
                                (entity_id, dataset_name, filename, resolved_form_id, resolved_submission_uuid)
                            VALUES (:e, :d, :f, :form, :sub)
                            ON CONFLICT (entity_id, filename) DO NOTHING
                        """), {"e": entity_id, "d": dataset_name, "f": filename, "form": form_id, "sub": sub_uuid})
                    
                    print(f"💾 [CACHE SAVED] Form={form_id}, Submission={sub_uuid}")
                    return form_id, sub_uuid
                    
        raise FileNotFoundError("Could not locate a valid form submission source in entity versions history.")
        
    except Exception as e:
        print(f"💥 Lineage discovery failed: {e}")
        raise e

@app.route("/media/<dataset_name>/<entity_id>/<filename>", methods=["GET"])
def proxy_media(dataset_name, entity_id, filename):
    try:
        safe_filename = filename.strip()
        
        # 1. Resolve lineage from cache (preserves the raw 'uuid:...' format)
        form_id, submission_uuid = discover_lineage(dataset_name, entity_id, safe_filename)
        
        safe_form_id = form_id.strip()
        safe_submission_uuid = submission_uuid.strip()

        # 2. URL-encode the parameters (Transforms 'uuid:XYZ' -> 'uuid%3AXYZ')
        encoded_form_id = urllib.parse.quote(safe_form_id)
        encoded_submission_id = urllib.parse.quote(safe_submission_uuid)
        encoded_filename = urllib.parse.quote(safe_filename)

        # 3. Construct the winning URL path structure
        winning_path = f"projects/{PROJECT_ID}/forms/{encoded_form_id}/submissions/{encoded_submission_id}/attachments/{encoded_filename}"
        
        print(f"📡 [STREAMING] Forwarding verified path to ODK Central:\n    👉 {winning_path}")
        odk_response = client.session.get(winning_path, stream=True)

        if odk_response.status_code != 200:
            print(f"❌ ODK Server rejected streaming request with status code: {odk_response.status_code}")
            return Response(f"ODK Central Asset Stream Error: {odk_response.status_code}", status=odk_response.status_code)
            
        return Response(
            stream_with_context(odk_response.iter_content(chunk_size=8192)),
            content_type=odk_response.headers.get("Content-Type")
        )

    except Exception as e:
        print(f"💥 Proxy Layer Execution Failure: {e}")
        return jsonify({"error": "Proxy Error Layer", "details": str(e)}), 500
        
if __name__ == "__main__":
    # Ensure production cache table space exists on init
    with engine.begin() as init_conn:
        init_conn.execute(text("""
            CREATE TABLE IF NOT EXISTS data_staging.media_lineage_cache (
                entity_id TEXT,
                dataset_name TEXT,
                filename TEXT,
                resolved_form_id TEXT,
                resolved_submission_uuid TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (entity_id, filename)
            );
        """))
    # In full production, you will wrap this script with Gunicorn/Systemd running on port 5050
    app.run(host="0.0.0.0", port=5050, debug=False)
