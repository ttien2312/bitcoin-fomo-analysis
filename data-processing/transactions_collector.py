import requests
import time
import pandas as pd
import datetime

# CUT-OFF TIME
CUTOFF_DATE = datetime.datetime(2017, 5, 11)
CUTOFF_TIMESTAMP = int(CUTOFF_DATE.timestamp())
print(f"Chỉ lấy dữ liệu từ ngày: {CUTOFF_DATE.strftime('%Y-%m-%d')} (Timestamp: {CUTOFF_TIMESTAMP})")

# 1. LOAD ADDRESSES
def load_addresses(file_path="remain_address.txt"):
    try:
        with open(file_path, "r") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Không tìm thấy file {file_path}. Vui lòng kiểm tra lại.")
        return []

addresses = load_addresses()
target_set = set(addresses)
print(f"Số lượng tài khoản: {len(addresses)}")

# 2. BATCH
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# 3. FETCH API 
def fetch_all_txs(addresses_batch):
    addr_str = "|".join(addresses_batch)
    limit = 100
    offset = 0
    all_txs = []
    
    while True:
        url = f"https://blockchain.info/multiaddr?active={addr_str}&n={limit}&offset={offset}"
        reached_old_tx = False
        
        try:
            r = requests.get(url, timeout=15)
            
            if r.status_code == 200:
                data = r.json()
                txs = data.get("txs", [])
                
                if not txs:
                    return all_txs, False # Hết giao dịch, không bị Rate Limit
                
                valid_txs = []
                for tx in txs:
                    tx_time = tx.get("time", 0)
                    if tx_time >= CUTOFF_TIMESTAMP:
                        valid_txs.append(tx)
                    else:
                        reached_old_tx = True
                        
                all_txs.extend(valid_txs)
            
            elif r.status_code == 429:
                print(f"   [!] Bị Rate Limit (HTTP 429) tại offset {offset}. Đang dừng quá trình fetch...")
                return all_txs, True # Trả về cờ True báo hiệu đã bị Rate Limit
            
            else:
                print(f"   [!] Error {r.status_code} tại offset {offset}")
                return all_txs, False # Lỗi khác, có thể dừng hoặc đi tiếp tùy logic
                
        except Exception as e:
            print(f"Lỗi kết nối: {e}")
            return all_txs, False
            
        # Dừng sớm nếu đã chạm tới giao dịch cũ hơn 11/05/2017
        if reached_old_tx:
            print(f"   -> Đã chạm mốc thời gian 11/05/2017, dừng fetch trang tiếp theo.")
            break

        if len(txs) < limit:
            break
            
        offset += limit
        time.sleep(1) # Nghỉ giữa các trang để hạn chế spam
        
    return all_txs, False

# 4. CRAWL DATA
all_txs = []
is_rate_limited_global = False

if addresses:
    for i, batch in enumerate(chunks(addresses, 2)):
        print(f"Đang xử lý Batch {i+1}...")
        
        txs_data, is_rate_limited = fetch_all_txs(batch)
        
        if txs_data:
            all_txs.extend(txs_data)
            print(f" -> Lấy thành công {len(txs_data)} giao dịch")
            
        if is_rate_limited:
            print("\n[CẢNH BÁO] Đã chạm giới hạn API (Rate Limit). Dừng toàn bộ chương trình và tiến hành lưu dữ liệu đã lấy được!")
            is_rate_limited_global = True
            break # Phá vỡ vòng lặp duyệt batch
        
        time.sleep(2) # Nghỉ giữa các batch

print("Hoàn tất bước thu thập dữ liệu.")

# 5. FLATTEN DATA
rows = []

for tx in all_txs:
    tx_time = tx.get("time", 0)
    
    if tx_time < CUTOFF_TIMESTAMP:
        continue
        
    inputs = tx.get("inputs", [])
    outputs = tx.get("out", [])

    addr_flow = {}

    for inp in inputs:
        prev_out = inp.get("prev_out", {})
        addr = prev_out.get("addr")
        value = prev_out.get("value", 0)
        
        if addr in target_set:
            addr_flow[addr] = addr_flow.get(addr, 0) - value

    for out in outputs:
        addr = out.get("addr")
        value = out.get("value", 0)
        if addr:
            if addr in target_set:
                addr_flow[addr] = addr_flow.get(addr, 0) + value
    
    for addr, net in addr_flow.items():
        rows.append({
            "tx_hash": tx.get("hash"),
            "time": datetime.datetime.fromtimestamp(tx_time),
            "block_height": tx.get("block_height"),
            "address": addr,
            "net_flow": net,
            "direction": "buy" if net > 0 else ("sell" if net < 0 else ""),
            "n_inputs": len(inputs),
            "n_outputs": len(outputs),
            "fee_rate": tx.get("fee", 0) / tx.get("size", 1) if tx.get("size", 1) > 0 else 0
        })          

# 6. CLEAN DATA & SAVE
df = pd.DataFrame(rows)

if not df.empty:
    df.drop_duplicates(subset=["tx_hash", "address"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # Bạn có thể đổi tên file nếu muốn đánh dấu đây là bản dừng do Rate Limit
    file_name = "transactions_latest.csv"
    df.to_csv(file_name, index=False)
    
    print(f"Đã lưu: {file_name}")
    print(f"Tổng số giao dịch thu thập được: {len(df)}")
    print(df.head())
else:
    print("Không có dữ liệu giao dịch nào được tìm thấy hoặc tất cả giao dịch đều trước ngày 11/05/2017.")