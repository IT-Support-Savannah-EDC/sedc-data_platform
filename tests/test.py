from pyodk.client import Client
import json

# Initialize pyodk with your absolute config path
client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")

project_id = 1
form_id = "Commercial"
submission_uuid = "5409e563-fbbc-464c-adbf-c2c97a14e31c"

# Target the manifest list endpoint for this submission
endpoint = f"projects/{project_id}/forms/{form_id}/submissions/{submission_uuid}/attachments"

print(f"Checking manifest list on ODK Central for submission: {submission_uuid}...")
try:
    response = client.get(endpoint)
    if response.status_code == 200:
        print("\n🎉 Success! Found attachment manifest records on server:")
        print(json.dumps(response.json(), indent=4))
    else:
        print(f"\n❌ Server responded with code {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"\n💥 Script execution failed: {e}")
