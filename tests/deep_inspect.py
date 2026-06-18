from pyodk.client import Client

client = Client(config_path="/opt/data_platform/config/.pyodk_config.toml")

PROJECT_ID = 1
FORM_ID = "Commercial"
SUBMISSION_UUID = "8c05b32f-ffc3-46b1-85f6-879f32e72900"

print("🕵️‍♂️ Probing ODK Central API Hierarchy...\n")

endpoints = [
    ("1. Project Existence", f"projects/{PROJECT_ID}"),
    ("2. Form Existence", f"projects/{PROJECT_ID}/forms/{FORM_ID}"),
    ("3. Submission Existence", f"projects/{PROJECT_ID}/forms/{FORM_ID}/submissions/{SUBMISSION_UUID}"),
    ("4. Attachment Manifest", f"projects/{PROJECT_ID}/forms/{FORM_ID}/submissions/{SUBMISSION_UUID}/attachments")
]

for step_name, path in endpoints:
    print(f"Testing {step_name}: {path}")
    try:
        res = client.get(path)
        print(f" -> Status: {res.status_code}")
        if res.status_code != 200:
            print(f" -> Error Detail: {res.text}\n")
            break # Stop if a parent level fails
        print(" -> ✅ Found\n")
    except Exception as e:
        print(f" -> 💥 Request Failed: {e}\n")
        break
