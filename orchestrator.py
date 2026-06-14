import subprocess
import logging
import sys

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - [ORCHESTRATOR] - %(message)s',
    datefmt='%H:%M:%S %d-%m-%Y',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def run_script(script_path):
    logger.info(f"🚀 Launching job: {script_path}")
    print("\n" + "="*50 + f" START: {script_path} " + "="*50)
    
    python_bin = "/opt/data_platform/venv/bin/python"
    
    # Removing capture_output lets the script logs stream directly to the terminal screen
    result = subprocess.run([python_bin, script_path])
    
    print("="*50 + f" END: {script_path} " + "="*50 + "\n")
    
    if result.returncode != 0:
        logger.critical(f"❌ Job Failed: {script_path}")
        sys.exit(1)
        
    logger.info(f"✅ Job Succeeded: {script_path}")

if __name__ == "__main__":
    logger.info("🟢 Starting Nightly Data Pipeline...")
    
    # 1. Extract from ODK to data_raw
    run_script("/opt/data_platform/extractors/extract_odk.py")
    
    # 2. Load from data_raw to data_refined
    run_script("/opt/data_platform/loaders/load_refined.py")
    
    logger.info("🏁 Pipeline Execution Complete!")
