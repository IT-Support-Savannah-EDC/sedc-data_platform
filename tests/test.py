from pyodk.client import Client
import urllib.parse

client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")

PROJECT_ID = 1
FORM_ID = "1001"
RAW_SUB_ID = "uuid:d173e71f-d609-457e-aa91-b226d6de9ba9"
FILENAME = "1780565177959.jpg"

# Encode the ID to turn "uuid:" into "uuid%3A"
ENCODED_SUB_ID = urllib.parse.quote(RAW_SUB_ID)

variants = [
    ("Standard Encoded Path", f"projects/{PROJECT_ID}/forms/{FORM_ID}/submissions/{ENCODED_SUB_ID}/attachments/{FILENAME}"),
    ("Raw Path (No Encoding)", f"projects/{PROJECT_ID}/forms/{FORM_ID}/submissions/{RAW_SUB_ID}/attachments/{FILENAME}"),
    ("Stripped ID Path", f"projects/{PROJECT_ID}/forms/{FORM_ID}/submissions/{RAW_SUB_ID.replace('uuid:', '')}/attachments/{FILENAME}")
]

print("🚀 Testing exact attachment routing variants directly against ODK Central...\n")

for description, path in variants:
    print(f"Testing: {description}")
    print(f"👉 Path: {path}")
    try:
        res = client.session.get(path)
        print(f"💥 Status Received: {res.status_code}\n")
        if res.status_code == 200:
            print(f"🎯 WINNER! Use this format.\n")
            break
    except Exception as e:
        print(f"❌ Connection error on this variant: {e}\n")
