import os
import subprocess
import logging
import sys
import requests
from dotenv import load_dotenv

load_dotenv("/opt/data_platform/config/.env")

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - [ORCHESTRATOR] - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("WEBHOOK_URL")

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
        # Node 1: Extraction Phase (Fetches standard datasets and media payloads to data_raw)
        extractor_logs = run_script("/opt/data_platform/extractors/extract_odk.py")
        pipeline_summary += f"=== Extractor Phase ===\n{extractor_logs}\n\n"
        
        # Node 2: Main Transform / Load Phase (Processes entity data into data_refined)
        loader_logs = run_script("/opt/data_platform/loaders/load_refined.py")
        pipeline_summary += f"=== Main Loader Phase ===\n{loader_logs}\n\n"
        
        # Node 3: Dedicated Media Link Phase (Transforms and binds media from data_raw)
        media_logs = run_script("/opt/data_platform/loaders/load_media.py")
        pipeline_summary += f"=== Media Link Loader Phase ===\n{media_logs}\n\n"
        
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
