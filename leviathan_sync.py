import os
import json
import yfinance as yf
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[LEVIATHAN Node] Initializing Commodities & Logistics extraction...")
    
    # BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    
    # Dedicated flat table for LEVIATHAN
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.leviathan_logistics"
    
    # LEVIATHAN targets: Raw Materials + Ocean Freight Capacity Providers
    assets = {
        "Crude Oil": "CL=F",
        "Copper (Industrial Proxy)": "HG=F",
        "Natural Gas": "NG=F",
        "Wheat": "ZW=F",
        "Global Container Freight (ZIM)": "ZIM",
        "Dry Bulk Freight (Star Bulk)": "SBLK",
        "Ocean Tanker Capacity (Scorpio)": "STNG"
    }
    
    timestamp_iso = datetime.utcnow().isoformat()
    bq_payload = []
    
    for asset_name, ticker in assets.items():
        try:
            # Fetch last 5 days to ensure we capture the most recent close
            data = yf.download(ticker, period="5d", progress=False)
            if data.empty or len(data) < 2:
                continue
            
            # Extract final metrics
            current_close = round(float(data['Close'].iloc[-1]), 2)
            prev_close = round(float(data['Close'].iloc[-2]), 2)
            pct_change = round(((current_close - prev_close) / prev_close) * 100, 2)
            
            # Note the flattened dictionary structure without the "raw_data" wrapper
            bq_payload.append({
                "timestamp": timestamp_iso,
                "domain": "LEVIATHAN",
                "entity_id": asset_name,
                "ticker": ticker,
                "signal_type": "Daily Commodity/Freight Settlement",
                "close_price": current_close,
                "percent_change": pct_change
            })
        except Exception as e:
            print(f"[LEVIATHAN ERROR] Failed to fetch {asset_name}: {e}")

    if bq_payload:
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True, # Automatically creates the flat leviathan_logistics table
            )
            job = client.load_table_from_json(bq_payload, table_id, job_config=job_config)
            job.result()  
            print(f"[LEVIATHAN] Successfully loaded {len(bq_payload)} physical supply chain metrics into BigQuery.")
        except Exception as e:
            print(f"[LEVIATHAN ERROR] BigQuery push failed: {e}")
    else:
        print("[LEVIATHAN] No supply chain metrics pulled.")

if __name__ == "__main__":
    main()
  
