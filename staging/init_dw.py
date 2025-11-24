import os
import psycopg2
import yaml

# ===============================
# ⚙️ 1. ĐỌC FILE CẤU HÌNH YAML
# ===============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # => D:\DW\warehouse
CONFIG_PATH = os.path.join(BASE_DIR, "..", "staging", "config_dw.yaml")

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f" Không tìm thấy file cấu hình tại: {CONFIG_PATH}")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Lấy thông tin các DB
stg_cfg = config["staging_db"]
dw_cfg = config["dw_db"]
control_cfg = config.get("control_db", stg_cfg)  # Fallback về staging nếu không có

# Hàm kết nối PostgreSQL
def connect_db(cfg):
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"]
    )
    return conn

# ========================
# 1️⃣ Tạo database nếu chưa có
# ========================
def create_database(dbname, conn):
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{dbname}'")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {dbname}")
        print(f" Database {dbname} created.")
    else:
        print(f" Database {dbname} already exists.")
    cur.close()

# Kết nối vào postgres gốc để tạo DB
root_conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="113004"
)
create_database(stg_cfg["dbname"], root_conn)
create_database(dw_cfg["dbname"], root_conn)
create_database(control_cfg.get("dbname", "control_dw"), root_conn)
root_conn.close()

# ========================
# 2️⃣ Tạo bảng trong STAGING
# ========================
stg_conn = connect_db(stg_cfg)
stg_cur = stg_conn.cursor()

stg_cur.execute("""
CREATE TABLE IF NOT EXISTS stg_stream_snapshot (
    stream_id VARCHAR(100),
    platform VARCHAR(20),
    streamer_name VARCHAR(200),
    platform_id VARCHAR(100),
    game_name VARCHAR(200),
    game_id VARCHAR(100),
    category VARCHAR(100),
    viewer_count INT,
    follower_count BIGINT,
    language VARCHAR(20),
    start_time TIMESTAMP,
    capture_time TIMESTAMP,
    stream_title TEXT
);
""")
print("✅ Table stg_stream_snapshot created in staging_dw.")

stg_conn.commit()
stg_cur.close()
stg_conn.close()

# ========================
# 3️⃣ Tạo bảng trong DATA WAREHOUSE
# ========================
dw_conn = connect_db(dw_cfg)
dw_cur = dw_conn.cursor()

dw_cur.execute("""
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_id SERIAL PRIMARY KEY,
    platform_name VARCHAR(50) UNIQUE
);
""")

dw_cur.execute("""
CREATE TABLE IF NOT EXISTS dim_game (
    game_id SERIAL PRIMARY KEY,
    game_name VARCHAR(100) UNIQUE
);
""")

dw_cur.execute("""
CREATE TABLE IF NOT EXISTS dim_streamer (
    streamer_id SERIAL PRIMARY KEY,
    streamer_name VARCHAR(100) UNIQUE
);
""")

dw_cur.execute("""
CREATE TABLE IF NOT EXISTS fact_stream_snapshot (
    snapshot_id SERIAL PRIMARY KEY,
    platform_id INT REFERENCES dim_platform(platform_id),
    game_id INT REFERENCES dim_game(game_id),
    streamer_id INT REFERENCES dim_streamer(streamer_id),
    viewers INT,
    capture_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

print("✅ Data warehouse tables created successfully.")

dw_conn.commit()
dw_cur.close()
dw_conn.close()

# ========================
# 4️⃣ Tạo bảng trong CONTROL DB
# ========================
control_dbname = control_cfg.get("dbname", "control_dw")
control_conn = psycopg2.connect(
    host=control_cfg["host"],
    port=control_cfg["port"],
    dbname=control_dbname,
    user=control_cfg["user"],
    password=control_cfg["password"]
)
control_conn.autocommit = True
control_cur = control_conn.cursor()


def execute_sql_file(cursor, path):
    """Thực thi file SQL, bỏ qua comment."""
    with open(path, "r", encoding="utf-8") as f:
        buffer = []
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            buffer.append(line)
            if stripped.endswith(";"):
                statement = "".join(buffer).strip()
                buffer = []
                if not statement:
                    continue
                try:
                    cursor.execute(statement)
                except Exception as exc:
                    if "already exists" in str(exc).lower():
                        continue
                    print(f"⚠️ Warning executing statement:\n{statement}\nError: {exc}")
                    raise
        if buffer:
            cursor.execute("".join(buffer))


# Đọc và thực thi script tạo Control DB
control_sql_path = os.path.join(BASE_DIR, "..", "staging", "control", "create_control_db.sql")
if os.path.exists(control_sql_path):
    try:
        execute_sql_file(control_cur, control_sql_path)
        print("✅ Control DB tables created successfully.")
    except Exception as exc:
        control_conn.rollback()
        raise exc
else:
    # Tạo bảng nếu không có file SQL
    control_cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_job_control (
            job_id SERIAL PRIMARY KEY,
            job_name VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            records_processed INT DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    control_cur.execute("""
        CREATE TABLE IF NOT EXISTS file_audit_log (
            log_id SERIAL PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            file_path TEXT NOT NULL,
            file_size BIGINT,
            records_count INT,
            status VARCHAR(20) NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_id INT,
            error_message TEXT
        );
    """)
    
    control_cur.execute("""
        CREATE TABLE IF NOT EXISTS batch_run_history (
            batch_id SERIAL PRIMARY KEY,
            batch_date DATE NOT NULL,
            batch_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_end_time TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            files_processed INT DEFAULT 0,
            total_records INT DEFAULT 0,
            error_summary TEXT
        );
    """)
    print("✅ Control DB tables created successfully (fallback).")

control_conn.commit()
control_cur.close()
control_conn.close()

print("\n🎉 Hoàn tất khởi tạo toàn bộ hệ thống Data Warehouse!")