# Conceptual framework for extract_sheets.py
from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)
engine = create_engine("YOUR_DATABASE_URL")

@app.route('/webhook/sheets', methods=['POST'])
def receive_sheets_data():
    payload = request.json
    df = pd.DataFrame(payload['data'])
    table_name = payload['table_name']
    
    # Force upload strictly to raw sheets schema
    df.to_sql(table_name, engine, schema='data_raw_sheets', if_exists='append', index=False)
    return jsonify({"status": "success", "message": "Raw sheets data cached successfully."}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
