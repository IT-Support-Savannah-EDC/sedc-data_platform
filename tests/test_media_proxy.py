import os
import sys
import requests
from flask import Flask, Response, request, stream_with_context
from sqlalchemy import create_engine, text
from pyodk.client import Client
from dotenv import load_dotenv

# 1. LOAD CONFIGURATIONS (Points to your existing platform configs)
# If testing locally, update these paths to where your .env and .toml files live.
load_dotenv("/opt/data_platform/config/.env")

print("🎬 Initializing Sandbox Media Proxy Test...")

DB_URI = os.getenv("DATABASE_URL")
PROJECT_ID = os.getenv("PROJECT_ID")

if not DB_URI or not PROJECT_ID:
    print("❌ Error: DATABASE_URL or PROJECT_ID missing from .env file!")
    sys.exit(1)

PROJECT_ID = int(PROJECT_ID)

# Initialize Database and ODK Connections
try:
    engine = create_engine(DB_URI, pool_pre_ping=True)
    odk_client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")
    BASE_ODK_URL = odk_client.session.base_url.rstrip('/')
    print("✅ Database connection initialized.")
    print(f"✅ ODK Central connected to: {BASE_ODK_URL}")
except Exception as e:
    print(f"❌ Connection Setup Failed: {e}")
    sys.exit(1)

# 2. INITIALIZE SANDBOX CACHE TABLE
# We create a specific test table so we don't interfere with future production data.
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS data_staging.test_media_lineage_cache (
            entity_id TEXT,
            dataset_name TEXT,
            filename TEXT,
            resolved_form_id TEXT NOT NULL,
            resolved_submission_uuid TEXT NOT NULL,
            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (entity_id, filename)
        );
    """))
    print("✅ Sandbox cache table verified/created: data_staging.test_media_lineage_cache")

# 3. SPIN UP FLASK
app = Flask(__name__)

def discover_lineage(dataset_name, entity_id, filename):
    """Searches local cache first, then hits ODK Central History API if it misses."""
    # Check Cache
    with engine.connect() as conn:
        query = text("""
            SELECT resolved_form_id, resolved_submission_uuid 
            FROM data_staging.test_media_lineage_cache 
            WHERE entity_id = :e AND filename = :f
        """)
        result = conn.execute(query, {"e": entity_id, "f": filename}).fetchone()
        if result:
            print(f"🎯 [CACHE HIT] Found lineage locally: Form={result[0]}, Submission={result[1]}")
            return result[0], result[1]

    # Cache Miss: Call ODK Central
    print(f"🔍 [CACHE MISS] Tracking lineage on ODK Central for Entity: {entity_id}...")
    history_endpoint = f"{BASE_ODK_URL}/projects/{PROJECT_ID}/datasets/{dataset_name}/entities/{entity_id}/versions"
    
    response = odk_client.session.get(history_endpoint, timeout=10)
    response.raise_for_status()
    
    # Let's print the raw payload to the terminal so we can see it!
    print(f"📦 Raw Version History: {response.json()}")

    for change_event in response.json():
        source = change_event.get("source", {})
        if source.get("type") == "submission":
            details = source.get("details", {})
            form_id = details.get("xmlFormId")
            sub_uuid = details.get("instanceId")
            
            if form_id and sub_uuid:
                # Save to Cache
                with engine.begin() as write_conn:
                    write_conn.execute(text("""
                        INSERT INTO data_staging.test_media_lineage_cache (entity_id, dataset_name, filename, resolved_form_id, resolved_submission_uuid)
                        VALUES (:e, :d, :f, :form, :sub)
                        ON CONFLICT (entity_id, filename) DO NOTHING
                    """), {"e": entity_id, "d": dataset_name, "f": filename, "form": form_id, "sub": sub_uuid})
            
            print(f"💾 [CACHE SAVED] Tracked history from ODK: Form={form_id}, Submission={sub_uuid}")
            return form_id, sub_uuid
            
    raise FileNotFoundError("Could not find a submission source in this entity's history log.")

def handle_test_failure(error, dataset, entity_id, filename):
    """Fallback mechanism for errors."""
    print(f"💥 [ERROR ENCOUNTERED]: {error}")
    fallback_dir = "/opt/data_platform/assets/static_errors/"
    
    if isinstance(error, requests.exceptions.HTTPError) and error.response.status_code == 404:
        fallback_file = os.path.join(fallback_dir, "asset_404.svg")
        status_code = 404
        print("📁 Action: Attempting to serve asset_404.svg placeholder.")
    else:
        fallback_file = os.path.join(fallback_dir, "engine_breakdown.svg")
        status_code = 502
        print("📁 Action: Attempting to serve engine_breakdown.svg placeholder.")

    if os.path.exists(fallback_file):
        with open(fallback_file, "rb") as f:
            return Response(f.read(), status=status_code, mimetype="image/svg+xml")
    
    return Response(f"Sandbox Error Layer Caught: {str(error)}", status=status_code, mimetype="text/plain")

# 4. THE LIVE TESTING ROUTE
@app.route('/test-media/<dataset_name>/<entity_id>/<filename>', methods=['GET'])
def test_media_endpoint(dataset_name, entity_id, filename):
    print(f"\n📥 [NEW REQUEST] Dataset: {dataset_name} | Entity ID: {entity_id} | File: {filename}")
    try:
        # Find the metadata
        form_id, submission_uuid = discover_lineage(dataset_name, entity_id, filename)
        
        # Build the exact ODK Central direct attachment endpoint
        clean_uuid = submission_uuid if submission_uuid.startswith("uuid:") else f"uuid:{submission_uuid}"
        binary_url = f"{BASE_ODK_URL}/projects/{PROJECT_ID}/forms/{form_id}/submissions/{clean_uuid}/attachments/{filename}"
        
        # Pull file from ODK
        print(f"📡 Streaming binary chunks directly from ODK Central source URL...")
        odk_stream = odk_client.session.get(binary_url, stream=True, timeout=15)
        odk_stream.raise_for_status()
        
        # Optional force download trigger if parameter '?download=true' is added
        should_download = request.args.get('download', 'false').lower() == 'true'
        disposition = "attachment" if should_download else "inline"
        
        return Response(
            stream_with_context(odk_stream.iter_content(chunk_size=8192)),
            content_type=odk_stream.headers.get('Content-Type', 'image/jpeg'),
            headers={"Content-Disposition": f"{disposition}; filename=\"{filename}\""}
        )
        
    except Exception as e:
        return handle_test_failure(e, dataset_name, entity_id, filename)

if __name__ == '__main__':
    # Run server locally on port 5050 for isolated sandbox testing
    print("🚀 Sandbox server spun up! Ready for manual browser hits.")
    app.run(host='0.0.0.0', port=5050, debug=True)

