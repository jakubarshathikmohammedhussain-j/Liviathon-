import os
import json
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def calculate_rsi_series(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)

def main():
    print("[LEVIATHAN BACKFILL] Initializing 10-Year Maritime & Macro History...")
    
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.leviathan_logistics"

    # Expanded 10-year maritime, dry bulk, tanker, and commodity basket
    target_basket = {
        # Container Liners & Charters
        "ZIM": "Container Carrier",
        "MATX": "Pacific Container Chokepoint",
        "DAC": "Containership Charter",
        "ATCO": "Asset Management / Container Leasing",
        # Dry Bulk (Iron Ore, Coal, Grains)
        "BDRY": "Dry Bulk Shipping ETF",
        "SBLK": "Global Dry Bulk",
        "GNK": "Dry Bulk Carrier",
        "EGLE": "Dry Bulk Fleet",
        # Tankers (Crude & Refined Products)
        "FRO": "Crude Tanker Operations",
        "STNG": "Clean Product Tankers",
        "EURN": "Large Crude Carrier",
        "FLNG": "LNG Maritime Carrier",
        # Macro Commodities & Energy Chokepoints
        "CL=F": "WTI Crude Oil Futures",
        "BZ=F": "Brent Crude Oil Futures",
        "NG=F": "Natural Gas Futures",
        "HG=F": "Copper Futures (Industrial Barometer)",
        "GC=F": "Gold Futures (Macro Hedge)",
        "ZW=F": "Wheat Futures (Food Supply Chain)"
    }

    tickers = list(target_basket.keys())
    print(f"[LEVIATHAN] Fetching 10-year history for {len(tickers)} assets...")

    try:
        df = yf.download(
            tickers=tickers,
            period="10y",
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=False
        )
    except Exception as e:
        print(f"[LEVIATHAN ERROR] Download failed: {e}")
        return

    if df.empty:
        print("[LEVIATHAN ERROR] Empty dataset received.")
        return

    all_records = []

    for ticker in tickers:
        try:
            if ticker not in df.columns.levels[0]:
                continue
            
            sub_df = df[ticker].dropna(subset=['Close'])
            if len(sub_df) < 15:
                continue

            close_series = sub_df['Close']
            pct_series = close_series.pct_change() * 100
            rsi_series = calculate_rsi_series(close_series, period=14)
            volume_series = sub_df['Volume'].fillna(0)

            for dt, close_val in close_series.items():
                prev_dt_pct = pct_series.get(dt, 0.0)
                pct_val = 0.0 if pd.isna(prev_dt_pct) else round(float(prev_dt_pct), 2)
                rsi_val = round(float(rsi_series.get(dt, 50.0)), 2)
                vol_val = int(volume_series.get(dt, 0))

                # Exact schema match to existing leviathan_logistics table
                all_records.append({
                    "timestamp": dt.strftime('%Y-%m-%dT00:00:00Z'),
                    "ticker": ticker,
                    "close_price": round(float(close_val), 2),
                    "percent_change": pct_val,
                    "volume": vol_val,
                    "rsi_14d": rsi_val,
                    "signal_type": target_basket[ticker]
                })

        except Exception as e:
            print(f"[LEVIATHAN ERROR] Error parsing {ticker}: {e}")
            continue

    total_records = len(all_records)
    print(f"[LEVIATHAN] Ingesting {total_records} historical logistics records...")

    if total_records > 0:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True
        )

        try:
            job = client.load_table_from_json(all_records, table_id, job_config=job_config)
            job.result()
            print(f"[LEVIATHAN] Successfully loaded {total_records} rows into BigQuery.")
        except Exception as e:
            print(f"[LEVIATHAN ERROR] BigQuery upload failed: {e}")

if __name__ == "__main__":
    main()
  
