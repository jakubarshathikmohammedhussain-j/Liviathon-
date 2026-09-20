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

def classify_supply_chain_signal(ticker, pct_change, rsi):
    # Classify shock patterns across freight and commodity corridors
    if pct_change >= 5.0 or rsi >= 75:
        return "CHOKEPOINT_STRESS_SPIKE"
    elif pct_change <= -5.0 or rsi <= 25:
        return "DEMAND_CONTRACTION"
    elif abs(pct_change) >= 2.5:
        return "HIGH_VOLATILITY_FLOW"
    return "STABLE_FLOW"

def main():
    print("[LEVIATHAN BACKFILL] Initializing 5-Year Maritime & Freight Telemetry Pipeline...")
    
    # 1. BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.leviathan_logistics"

    # 2. Comprehensive Supply Chain & Freight Universe
    # Container shipping, dry bulk, crude/product tankers, and core commodities
    target_basket = {
        # Freight & Shipping Pure-Plays
        "BDRY": "Breakwave Dry Bulk Shipping ETF",
        "ZIM": "ZIM Integrated Shipping (Container)",
        "MATX": "Matson (Pacific Container Chokepoint)",
        "SBLK": "Star Bulk Carriers (Global Dry Bulk)",
        "GNK": "Genco Shipping & Trading (Dry Bulk)",
        "DAC": "Danaos Corp (Containership Charter)",
        "FRO": "Frontline plc (Crude Tankers)",
        "STNG": "Scorpio Tankers (Clean Product Tankers)",
        "FLNG": "Flex LNG (LNG Maritime Carrier)",
        # Commodity Chokepoints & Raw Inputs
        "CL=F": "WTI Crude Oil Futures",
        "BZ=F": "Brent Crude Oil Futures",
        "NG=F": "Natural Gas Futures",
        "HG=F": "Copper Futures (Industrial Barometer)",
        "GC=F": "Gold Futures (Macro Hedge)"
    }

    tickers = list(target_basket.keys())
    print(f"[LEVIATHAN BACKFILL] Ingesting 5-year timeline for {len(tickers)} core logistics assets...")

    # 3. Batch Download
    try:
        df = yf.download(
            tickers=tickers,
            period="5y",
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=False
        )
    except Exception as e:
        print(f"[LEVIATHAN ERROR] Batch download failed: {e}")
        return

    if df.empty:
        print("[LEVIATHAN ERROR] Download returned empty dataset.")
        return

    # 4. Transform into Telemetry Stream
    all_records = []
    timestamp_iso = datetime.utcnow().isoformat()

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

                all_records.append({
                    "timestamp": dt.strftime('%Y-%m-%dT00:00:00Z'),
                    "domain": "LEVIATHAN",
                    "entity_id": ticker,
                    "signal_type": target_basket[ticker],
                    "close_price": round(float(close_val), 2),
                    "percent_change": pct_val,
                    "volume": vol_val,
                    "rsi_14d": rsi_val,
                    "supply_chain_status": classify_supply_chain_signal(ticker, pct_val, rsi_val)
                })

        except Exception as e:
            print(f"[LEVIATHAN ERROR] Failed parsing {ticker}: {e}")
            continue

    total_records = len(all_records)
    print(f"[LEVIATHAN BACKFILL] Successfully structured {total_records} rows of supply chain history.")

    # 5. Ingestion to BigQuery
    if total_records > 0:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
            autodetect=True
        )

        try:
            job = client.load_table_from_json(all_records, table_id, job_config=job_config)
            job.result()
            print(f"[LEVIATHAN] Committed {total_records} historical logistics records to BigQuery.")
        except Exception as e:
            print(f"[LEVIATHAN ERROR] BigQuery load failed: {e}")

if __name__ == "__main__":
    main()
  
