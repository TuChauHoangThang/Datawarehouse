"""
Người số 2 - Data Prep Lead (Staging)
Chạy lệnh: python staging\\load_to_staging.py
Nhiệm vụ:
1. Kiểm tra rawData, hợp nhất tất cả *_raw.csv
2. Chuẩn hóa schema, thêm metadata/source_file
3. Đẩy snapshot mới nhất vào staging_dw.stg_stream_snapshot (REPLACE)
"""

import os
import pandas as pd
import yaml
import sqlalchemy

# ===============================
# ⚙️ 1. ĐỌC FILE CẤU HÌNH YAML
# ===============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # => D:\DW\staging
CONFIG_PATH = os.path.join(BASE_DIR, "config_dw.yaml")

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f" Không tìm thấy file cấu hình tại: {CONFIG_PATH}")

# Bước 2.1 (Người 2) - Đọc cấu hình YAML để lấy thông tin database/nguồn
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# ===============================
# 🧩 2. TẠO CHUỖI KẾT NỐI POSTGRESQL
# ===============================
# Bước 2.2 (Người 2) - Chuẩn bị chuỗi kết nối PostgreSQL staging_dw
staging_info = config.get("staging_db")
if not staging_info:
    raise KeyError(" Không tìm thấy khóa 'staging_db' trong file cấu hình YAML.")

connection_url = (
    f"postgresql+psycopg2://{staging_info['user']}:{staging_info['password']}"
    f"@{staging_info['host']}:{staging_info['port']}/{staging_info['dbname']}"
)

engine = sqlalchemy.create_engine(connection_url)

# ===============================
# 📦 3. TÌM VÀ ĐỌC TOÀN BỘ FILE RAW
# ===============================
# Bước 2.3 (Người 2) - Quét thư mục rawData tìm các file *_raw.csv
raw_data_dir = os.path.join(BASE_DIR, "rawData")
if not os.path.exists(raw_data_dir):
    raise FileNotFoundError(f" Không tìm thấy thư mục rawData tại: {raw_data_dir}")

csv_files = [f for f in os.listdir(raw_data_dir) if f.endswith("_raw.csv")]
if not csv_files:
    raise FileNotFoundError(f" Không có file *_raw.csv nào trong {raw_data_dir}")

csv_files.sort(key=lambda f: os.path.getctime(os.path.join(raw_data_dir, f)))

dataframes = []
total_records = 0
# Bước 2.4 (Người 2) - Đọc từng file raw và chuẩn hóa schema
print(" Đang đọc các file dữ liệu:")
for csv_name in csv_files:
    path = os.path.join(raw_data_dir, csv_name)
    print(f"  • {path}")
    df_part = pd.read_csv(path)
    df_part.columns = [c.strip().lower().replace(" ", "_") for c in df_part.columns]

    if "platform" not in df_part.columns:
        if "youtube" in csv_name.lower():
            df_part["platform"] = "YouTube"
        elif "twitch" in csv_name.lower():
            df_part["platform"] = "Twitch"
        else:
            df_part["platform"] = ""

    df_part["source_file"] = csv_name
    dataframes.append(df_part)
    total_records += len(df_part)

if not dataframes:
    raise ValueError(" Không đọc được dữ liệu từ bất kỳ file raw nào.")

df = pd.concat(dataframes, ignore_index=True).drop_duplicates()
print(f" Đã đọc {len(df)} dòng dữ liệu từ {len(dataframes)} file (tổng bản ghi gốc: {total_records}).")

# ===============================
#  4. GHI DỮ LIỆU VÀO POSTGRES (STAGING)
# ===============================
table_name = "stg_stream_snapshot"

# Bước 2.5 (Người 2) - Ghi snapshot hợp nhất vào bảng stg_stream_snapshot (REPLACE)
df.to_sql(table_name, engine, if_exists="replace", index=False)
print(f" Đã nạp {len(df)} dòng vào bảng '{table_name}' trong database staging_dw")
print(" Quá trình nạp dữ liệu vào STAGING hoàn tất!")
