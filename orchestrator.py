import os
import subprocess
import logging
import sys
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pyodk.client import Client

# Append framework root to path for fluid local imports
sys.path.append("/opt/data_platform")

load_dotenv("/opt/data_platform/config/.env")

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - [ORCHESTRATOR] - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Engine assets bound directly for Inline Protocol attachment handling
DATABASE_URL = os.getenv("DATABASE_URL")
PROJECT_ID = int(os.getenv("PROJECT_ID")) if os.getenv("PROJECT_ID") else None
engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None
client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml") if os.path.exists("/opt/data_platform/config/.pyodk_config.toml") else None

def send_to_ai_assistant(status, title, message, raw_logs=""):
    """Packages and dispatches execution state telemetry to the Apps Script AI Assistant."""
    if not WEBHOOK_URL:
        logger.warning("⚠️ WEBHOOK_URL target absent from configuration. Aborting telemetry broadcast.")
        return
    
    payload = {
        "status": status,          # "SUCCESS", "CRITICAL", or "WARNING"
        "title": title,            # Component name identifier
        "message": message,        # Context summary
        "raw_logs": raw_logs,      # Captured terminal stdout/stderr streams
        "environment": "Production (ELT-Droplet)"
    }
    
    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=15)
        if response.status_code in [200, 201]:
            logger.info("📱 Telemetry smoothly dispatched to Apps Script AI Assistant.")
        else:
            logger.error(f"❌ Apps Script gateway rejected payload. Status: {response.status_code}")
    except Exception as e:
        logger.error(f"⚠️ Telemetry transport layer breakdown: {e}")

def run_script(script_path):
    script_name = os.path.basename(script_path)
    logger.info(f"🚀 Launching pipeline node: {script_name}")
    
    python_bin = "/opt/data_platform/venv/bin/python"
    
    # Run and capture the exact console outputs
    result = subprocess.run(
        [python_bin, script_path], 
        capture_output=True, 
        text=True
    )
    
    # Keep cron.log happy by printing out what happened
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip(), file=sys.stderr)
        
    combined_logs = f"--- STDOUT ---\n{result.stdout}\n--- STDERR ---\n{result.stderr}"
    
    if result.returncode != 0:
        logger.critical(f"❌ Component Collapse: {script_name}")
        send_to_ai_assistant(
            status="CRITICAL",
            title=f"Pipeline Failure: {script_name}",
            message=f"The node '{script_name}' crashed with exit code {result.returncode}.",
            raw_logs=combined_logs
        )
        sys.exit(1)
        
    logger.info(f"✅ Node Execution Verified: {script_name}")
    return combined_logs

if __name__ == "__main__":
    logger.info("🟢 Starting Operational Data Pipeline Sequence...")
    pipeline_summary = ""
    
    try:
        # Node 1: Extraction Phase
        extractor_logs = run_script("/opt/data_platform/extractors/extract_odk.py")
        pipeline_summary += f"=== Extractor Phase ===\n{extractor_logs}\n\n"
        
        # Node 2: Transform / Load Phase
        loader_logs = run_script("/opt/data_platform/loaders/load_refined.py")
        pipeline_summary += f"=== Loader Phase ===\n{loader_logs}\n\n"
        
        # Inline Execution Attachment: Dynamic Schema Media Extraction Link Protocol
        try:
            logger.info("🎬 Initializing Dynamic Schema Media Extraction Link Protocol...")
            pipeline_summary += "=== Media Link Extraction Protocol Execution ===\n"
            
            # 1. Extraction Phase
            from extractors.extract_odk import extract_all_form_media, discover_forms
            all_forms = discover_forms(PROJECT_ID)
            for form in all_forms:
                extract_all_form_media(client, engine, PROJECT_ID, form)
                
            # 2. Fetch Data for Transformation Phase
            with engine.connect() as conn:
                raw_payloads = conn.execute(text("""
                    SELECT form_id, group_name, raw_json 
                    FROM data_raw.staging_media_payloads
                """)).mappings().all()
                
            if raw_payloads:
                # 3. Transformation Phase
                from transformers.stage_cleaner import transform_media_payloads
                df_main, dict_repeats = transform_media_payloads(raw_payloads)
                
                # 4. Loader Phase
                from loaders.load_refined import load_all_media_assets
                load_all_media_assets(engine, df_main, dict_repeats)
                
            logger.info("🏁 Dynamic Schema Media Extraction Link Protocol concluded successfully.")
            pipeline_summary += "Dynamic Schema Media Extraction Link Protocol concluded successfully.\n\n"
            
        except Exception as e:
            logger.critical(f"💥 Media Extraction Link Protocol failure: {str(e)}")
            pipeline_summary += f"💥 Media Extraction Link Protocol Failure:\n{str(e)}\n\n"
            send_to_ai_assistant(
                status="CRITICAL",
                title="Media Extraction Link Protocol Failure",
                message=f"The inline media extraction and link generation tracking protocol failed: {str(e)}",
                raw_logs=str(e)
            )
            sys.exit(1)
        
        # If we reach here, everything succeeded perfectly
        logger.info("🏁 Pipeline Execution Flawless! Notifying AI Assistant...")
        send_to_ai_assistant(
            status="SUCCESS",
            title="Pipeline Runs Green",
            message="All scheduled sync configurations successfully converged without errors.",
            raw_logs=pipeline_summary
        )
        
    except Exception as e:
        logger.critical(f"💥 Catastrophic Orchestrator Interruption: {e}")
        send_to_ai_assistant(
            status="CRITICAL",
            title="Orchestrator Fatal Interruption",
            message=f"The central controller threw an unhandled top-level exception: {str(e)}",
            raw_logs=str(e)
        )
        sys.exit(1)
