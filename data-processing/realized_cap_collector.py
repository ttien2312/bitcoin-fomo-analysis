import requests
import pandas as pd

url = "https://bitview.space/api/series/realized_cap/day1"
res = requests.get(url).json()
data = res['data']
df_realized = pd.DataFrame({"realized_cap": data})

start_date = pd.to_datetime("2009-01-01")
df_realized['date'] = start_date + pd.to_timedelta(df_realized.index, unit='D')
df_realized["realized_cap"] = pd.to_numeric(df_realized["realized_cap"], errors="coerce")
df_realized = df_realized.dropna()
df_realized = df_realized[df_realized["realized_cap"] > 0]
print(df_realized.head())
print(df_realized.tail())
df_realized.to_csv("realized_cap.csv", index=False)


df_market = history[["marketCap", "timestamp"]].copy().reset_index(drop=True)
df_market["date"] = pd.to_datetime(df_market["timestamp"])
df_market.drop(columns=["timestamp"], inplace=True)
df_market = df_market.sort_values("date").reset_index(drop=True)



df_realized['date'] = pd.to_datetime(df_realized['date'])
df_cap = pd.merge(df_market, df_realized, on="date", how="left")
df_cap = df_cap.dropna(subset=["marketCap", "realized_cap"])
df_cap.head()



df_cap["mvrv_ratio"] = df_cap["marketCap"] / df_cap["realized_cap"]
df_cap["market_cap_std"] = df_cap["marketCap"].rolling(window=365).std()
df_cap["mvrv_z_score"] = (df_cap["marketCap"] - df_cap["realized_cap"]) / df_cap["market_cap_std"]

print(df_cap.head)
print(df_cap.tail)