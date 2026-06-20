import os
import subprocess
import logging
import sys
import requests
import time
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
        "status": status,
        "title": title,
        "message": message,
        "raw_logs": raw_logs,
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
    """
    Executes a standalone script via subprocess and streams its stdout/stderr 
    directly to the terminal in real-time line-by-line.
    """
    script_name = os.path.basename(script_path)
    logger.info(f"🚀 Launching pipeline node: {script_name}")
    
    python_bin = "/opt/data_platform/venv/bin/python"
    
    # Redirect stderr to stdout to preserve chronologically ordered logging output
    process = subprocess.Popen(
        [python_bin, script_path], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        text=True,
        bufsize=1
    )
    
    combined_logs = []
    
    # Stream logs line-by-line to the console immediately as they generate
    for line in iter(process.stdout.readline, ''):
        print(line, end='', flush=True)
        combined_logs.append(line)
        
    process.stdout.close()
    return_code = process.wait()
    
    full_logs_str = "".join(combined_logs)
    
    if return_code != 0:
        logger.critical(f"❌ Component Collapse: {script_name}")
        send_to_ai_assistant(
            status="CRITICAL",
            title=f"Pipeline Failure: {script_name}",
            message=f"The node '{script_name}' crashed with exit code {return_code}.",
            raw_logs=full_logs_str
        )
        sys.exit(1)
        
    logger.info(f"✅ Node Execution Verified: {script_name}")
    return full_logs_str

if __name__ == "__main__":
    logger.info("🟢 Starting Operational Data Pipeline Sequence...")
    pipeline_summary = ""
    
    try:
        # Node 1: Extraction Phase
        extractor_logs = run_script("/opt/data_platform/extractors/extract_odk.py")
        pipeline_summary += f"=== Extractor Phase ===\n{extractor_logs}\n\n"
        
        # 30-Second Isolation Delay
        logger.info("⏳ Cooling down. Pausing for 30 seconds before launching next stage...")
        time.sleep(30)
        
        # Node 2: Main Transform / Load Phase
        loader_logs = run_script("/opt/data_platform/loaders/load_refined.py")
        pipeline_summary += f"=== Main Loader Phase ===\n{loader_logs}\n\n"
        
        # Success Telemetry Target
        logger.info("🏁 Pipeline Execution Flawless! Notifying AI Assistant...")
        send_to_ai_assistant(
            status="SUCCESS",
            title="Pipeline Runs Green",
            message="ODK Extraction and Refined Loading sync loops successfully completed without errors.",
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
