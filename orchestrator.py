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

def send_alert(status, title, message):
    """Dispatches a real-time HTTP POST payload to your notification server."""
    if not WEBHOOK_URL:
        logger.warning("⚠️ WEBHOOK_URL not found in .env. Skipping alert transmission.")
        return
    
    payload = {
        "status": status,
        "title": title,
        "message": message,
        "environment": "Production (ELT-Droplet)"
    }
    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code in [200, 201]:
            logger.info("📱 Incident notification successfully broadcasted.")
        else:
            logger.error(f"Failed to transmit alert. Endpoint returned status: {response.status_code}")
    except Exception as e:
        logger.error(f"⚠️ Notification engine failure: {e}")

def run_script(script_path):
    logger.info(f"🚀 Launching job: {script_path}")
    print("\n" + "="*50 + f" START: {script_path} " + "="*50)
    
    python_bin = "/opt/data_platform/venv/bin/python"
    result = subprocess.run([python_bin, script_path], capture_output=False)
    
    print("="*50 + f" END: {script_path} " + "="*50 + "\n")
    
    if result.returncode != 0:
        script_name = os.path.basename(script_path)
        logger.critical(f"❌ Job Failed: {script_path}")
        send_alert("CRITICAL", f"Pipeline Failure: {script_name}", f"The component script {script_name} crashed with exit code {result.returncode}. Review cron.log immediately.")
        sys.exit(1)
        
    logger.info(f"✅ Job Succeeded: {script_path}")

if __name__ == "__main__":
    logger.info("🟢 Starting Operational Data Pipeline...")
    try:
        # 1. Ingest
        run_script("/opt/data_platform/extractors/extract_odk.py")
        
        # 2. Process & Clear landing zone
        run_script("/opt/data_platform/loaders/load_refined.py")
        
        logger.info("🏁 Pipeline Execution Complete!")
        
    except Exception as e:
        logger.critical(f"💥 Orchestrator Core Breakdown: {e}")
        send_alert("CRITICAL", "Orchestrator Core Breakdown", f"The pipeline controller experienced a catastrophic shutdown: {str(e)}")
        sys.exit(1)
