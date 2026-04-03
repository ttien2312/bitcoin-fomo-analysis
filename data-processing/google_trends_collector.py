from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime, timedelta
import time
import random

pytrends = TrendReq(hl='en-US', tz=0) # UTC
kw_list = ['bitcoin', 'btc']
start_date = datetime(2017, 5, 1)
end_date   = datetime(2026, 6, 20)

trend_data = []
current = start_date

while current < end_date:
    segment_end = min(current + timedelta(days=240), end_date)   # 8 months

    timeframe = f"{current.strftime('%Y-%m-%d')} {segment_end.strftime('%Y-%m-%d')}"

    for attempt in range(4):
        try:
            pytrends.build_payload(kw_list, cat=0, timeframe=timeframe, geo='', gprop='')
            temp_data = pytrends.interest_over_time()

            if not temp_data.empty:
                temp_data = temp_data.reset_index()
                trend_data.append(temp_data)
                print(f"From {current.date()} to {segment_end.date()}: {len(temp_data)} rows")
                break
        except Exception as e:
            print(f"Error attempt {attempt+1}: {e}")
            time.sleep(30 + random.randint(20, 60))

    time.sleep(random.uniform(12, 20))      # delay to avoid rate limit 429
    current = segment_end + timedelta(days=1)

# Merge and clean
if trend_data:
    df_trends = pd.concat(trend_data, ignore_index=True)
    df_trends = df_trends.drop_duplicates(subset=['date'])
    df_trends['date'] = pd.to_datetime(df_trends['date'])
    df_trends = df_trends.sort_values('date').reset_index(drop=True)

    df_trends['search_volume'] = df_trends.get('bitcoin', 0) + df_trends.get('btc', 0)
    df_trends['Media_Saturation'] = df_trends['search_volume'].pct_change(periods=7) * 100

    df_trends.to_csv('google_trends_bitcoin_daily.csv', index=False)