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
    """Executes a python script and monitors for failure."""
    logger.info(f"🚀 Launching job: {script_path}")
    
    # We use the python binary inside your virtual environment specifically
    python_bin = "/opt/data_platform/venv/bin/python"
    
    result = subprocess.run([python_bin, script_path], capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.critical(f"❌ Job Failed: {script_path}\nError Output:\n{result.stderr}")
        # Stop the entire pipeline so we don't load bad data
        sys.exit(1)
        
    logger.info(f"✅ Job Succeeded: {script_path}")

if __name__ == "__main__":
    logger.info("🟢 Starting Nightly Data Pipeline...")
    
    # 1. Extract
    run_script("/opt/data_platform/extractors/extract_odk.py")
    
    # 2. Load
    run_script("/opt/data_platform/loaders/load_refined.py")
    
    logger.info("🏁 Pipeline Execution Complete!")
