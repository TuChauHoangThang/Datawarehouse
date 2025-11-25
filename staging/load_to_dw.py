"""
Người số 3 - Data Warehouse & Monitoring Lead
Chạy lệnh: python staging\\load_to_dw.py
Nhiệm vụ:
1. Đọc fact_stream_snapshot_ready (DW)
2. Đồng bộ dimension tables (UPSERT)
3. Append fact_stream_snapshot và log tiến trình vào Control DB
"""

import os
import sys
import psycopg2
import pandas as pd
import yaml
from sqlalchemy import create_engine
import traceback

# Import control utilities ngay trong staging
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # => D:\DW\staging
CONTROL_DIR = os.path.join(BASE_DIR, "control")
if CONTROL_DIR not in sys.path:
    sys.path.insert(0, CONTROL_DIR)

from control_connect import (  # type: ignore  # noqa: E402
    get_control_connection, ensure_control_tables,
    create_etl_job, update_etl_job
)

# ===============================
# 1. ĐỌC FILE CẤU HÌNH YAML
# ===============================
CONFIG_PATH = os.path.join(BASE_DIR, "config_dw.yaml")

if not os.path.exists(CONFIG_PATH):
    error_msg = f"Khong tim thay file cau hinh tai: {CONFIG_PATH}"
    print(error_msg, file=sys.stderr)
    print(error_msg)
    exit(1)

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
except Exception as e:
    error_msg = f"Loi khi doc file cau hinh: {e}"
    print(error_msg, file=sys.stderr)
    print(error_msg)
    exit(1)

# ===============================
# 2. KẾT NỐI DW VÀ CONTROL DB
# ===============================
try:
    if 'dw_db' not in config:
        error_msg = "Khong tim thay khoa 'dw_db' trong file cau hinh YAML."
        print(error_msg, file=sys.stderr)
        print(error_msg)
        exit(1)

    # Bước 3.1 (Người 3) - Kết nối DW và Control DB để log trạng thái
    dw_info = config["dw_db"]
    dw_conn = psycopg2.connect(
        host=dw_info["host"],
        port=dw_info["port"],
        dbname=dw_info["dbname"],
        user=dw_info["user"],
        password=dw_info["password"]
    )
    dw_cur = dw_conn.cursor()

    # Kết nối Control DB để log
    control_conn = None
    job_id = None
    try:
        control_conn = get_control_connection()
        ensure_control_tables(control_conn)
        job_id = create_etl_job("Load to Data Warehouse", control_conn)
        print(f"Da tao ETL job trong Control DB: job_id={job_id}")
    except Exception as e:
        print(f"Canh bao: Khong the ket noi Control DB: {e}")
        print("Tiep tuc chay load_to_dw ma khong ghi log vao Control DB")

    # ===============================
    # 3. ĐỌC DỮ LIỆU TỪ fact_stream_snapshot_ready
    # ===============================
    # Bước 3.2 (Người 3) - Đọc bảng tạm fact_stream_snapshot_ready làm nguồn load
    print("Dang doc du lieu tu fact_stream_snapshot_ready...")
    dw_engine = create_engine(
        f"postgresql+psycopg2://{dw_info['user']}:{dw_info['password']}@{dw_info['host']}:{dw_info['port']}/{dw_info['dbname']}"
    )
    df = pd.read_sql("SELECT * FROM fact_stream_snapshot_ready", dw_engine)
    print(f"Da doc {len(df)} dong tu fact_stream_snapshot_ready")

    if df.empty:
        error_msg = "Khong co du lieu trong bang fact_stream_snapshot_ready. Co the chua co du lieu duoc transform."
        print(error_msg, file=sys.stderr)
        print(error_msg)
        dw_cur.close()
        dw_conn.close()
        if control_conn:
            try:
                if job_id:
                    update_etl_job(job_id, "failed", 0, error_msg, control_conn)
                control_conn.close()
            except:
                pass
        exit(1)

    # Chuẩn hóa tên cột
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Chuẩn hóa capture_time và start_time về datetime naive (UTC -> naive)
    if "capture_time" in df.columns:
        df["capture_time"] = pd.to_datetime(df["capture_time"], errors="coerce", utc=True).dt.tz_convert(None)
    else:
        df["capture_time"] = pd.NaT

    if "start_time" in df.columns:
        df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True).dt.tz_convert(None)

    # Lấy thời gian snapshot mới nhất đã được load
    dw_cur.execute("SELECT MAX(capture_time) FROM fact_stream_snapshot")
    latest_snapshot_time = dw_cur.fetchone()[0]
    if latest_snapshot_time is not None:
        latest_snapshot_time = pd.Timestamp(latest_snapshot_time)
        if latest_snapshot_time.tzinfo is not None:
            latest_snapshot_time = latest_snapshot_time.tz_localize(None)

    if latest_snapshot_time:
        df_to_load = df[df["capture_time"].isna() | (df["capture_time"] > latest_snapshot_time)].copy()
    else:
        df_to_load = df.copy()

    # Loại bỏ trùng lặp trong cùng một lần load
    if not df_to_load.empty:
        df_to_load = df_to_load.drop_duplicates(subset=["stream_id", "capture_time"])

    if df_to_load.empty:
        print("Khong co snapshot moi can tai vao fact_stream_snapshot.")
        if control_conn and job_id:
            try:
                update_etl_job(job_id, "completed", 0, None, control_conn)
            except:
                pass
        dw_cur.close()
        dw_conn.close()
        if control_conn:
            try:
                control_conn.close()
            except:
                pass
        exit(0)  # Không phải lỗi, chỉ là không có dữ liệu mới
    else:
        print(f"Co {len(df_to_load)} dong snapshot moi se duoc tai vao fact_stream_snapshot.")

except Exception as e:
    error_msg = f"Loi khi ket noi database hoac doc du lieu: {e}"
    error_traceback = "".join(traceback.format_exc())
    full_error = f"{error_msg}\n{error_traceback}"
    print(full_error, file=sys.stderr)
    print(full_error)
    try:
        dw_cur.close()
        dw_conn.close()
    except:
        pass
    if control_conn:
        try:
            if job_id:
                update_etl_job(job_id, "failed", 0, error_msg, control_conn)
            control_conn.close()
        except:
            pass
    exit(1)

try:
    # ===============================
    # 4. TẢI VÀO DIMENSION TABLES
    # ===============================
    # Bước 3.4 (Người 3) - Đồng bộ các dimension tables bằng chiến lược UPSERT
    print("Dang tai du lieu vao cac bang Dimension...")

    # Tải vào dim_platform
    if "platform" in df.columns:
        platforms = df[["platform"]].dropna().drop_duplicates()
        for _, row in platforms.iterrows():
            platform_name = str(row["platform"]).strip()
            if not platform_name or platform_name.lower() == "nan":
                continue
            dw_cur.execute("""
                INSERT INTO dim_platform (platform_name)
                VALUES (%s)
                ON CONFLICT (platform_name) DO NOTHING
            """, (platform_name,))

    # Tải vào dim_game
    if "game_name" in df.columns:
        games = df[["game_name"]].dropna().drop_duplicates()
        for _, row in games.iterrows():
            game_name = str(row["game_name"]).strip()
            if not game_name or game_name.lower() == "nan":
                continue
            game_name = game_name[:100]
            dw_cur.execute("""
                INSERT INTO dim_game (game_name)
                VALUES (%s)
                ON CONFLICT (game_name) DO NOTHING
            """, (game_name,))

    # Tải vào dim_streamer
    if "streamer_name" in df.columns:
        streamers = df[["streamer_name"]].dropna().drop_duplicates()
        for _, row in streamers.iterrows():
            streamer_name = str(row["streamer_name"]).strip()
            if not streamer_name or streamer_name.lower() == "nan":
                continue
            streamer_name = streamer_name[:100]
            dw_cur.execute("""
                INSERT INTO dim_streamer (streamer_name)
                VALUES (%s)
                ON CONFLICT (streamer_name) DO NOTHING
            """, (streamer_name,))

    print("Da tai xong cac bang Dimension")

    # ===============================
    # 5. TẢI VÀO FACT TABLE
    # ===============================
    # Bước 3.5 (Người 3) - Append dữ liệu mới vào fact_stream_snapshot
    print("Dang tai du lieu vao bang Fact...")

    rows_loaded = 0

    for _, row in df_to_load.iterrows():
        # Lấy ID từ dimension tables
        # Platform ID
        platform_lookup = str(row.get("platform")).strip()
        if not platform_lookup or platform_lookup.lower() == "nan":
            platform_id = None
        else:
            dw_cur.execute("SELECT platform_id FROM dim_platform WHERE platform_name = %s", (platform_lookup,))
            platform_result = dw_cur.fetchone()
            platform_id = platform_result[0] if platform_result else None

        # Game ID
        game_id = None
        if "game_name" in df.columns and pd.notna(row.get("game_name")):
            game_lookup = str(row.get("game_name")).strip()
            if game_lookup and game_lookup.lower() != "nan":
                game_lookup = game_lookup[:100]
                dw_cur.execute("SELECT game_id FROM dim_game WHERE game_name = %s", (game_lookup,))
                game_result = dw_cur.fetchone()
                game_id = game_result[0] if game_result else None

        # Streamer ID
        streamer_id = None
        if "streamer_name" in df.columns and pd.notna(row.get("streamer_name")):
            streamer_lookup = str(row.get("streamer_name")).strip()
            if streamer_lookup and streamer_lookup.lower() != "nan":
                streamer_lookup = streamer_lookup[:100]
                dw_cur.execute("SELECT streamer_id FROM dim_streamer WHERE streamer_name = %s", (streamer_lookup,))
                streamer_result = dw_cur.fetchone()
                streamer_id = streamer_result[0] if streamer_result else None

        # Viewer count
        viewer_count = row.get("viewer_count")
        if pd.isna(viewer_count):
            viewer_count = row.get("viewers")
        if pd.isna(viewer_count):
            viewer_count = 0
        try:
            viewer_count = int(float(viewer_count))
        except (ValueError, TypeError):
            viewer_count = 0

        # Capture time
        capture_time_value = row.get("capture_time")
        if pd.isna(capture_time_value):
            capture_time_value = row.get("start_time")
        if isinstance(capture_time_value, pd.Timestamp):
            capture_time_value = capture_time_value.to_pydatetime()
        elif isinstance(capture_time_value, str):
            capture_time_value = pd.to_datetime(capture_time_value, errors="coerce", utc=True)
            if pd.notna(capture_time_value):
                capture_time_value = capture_time_value.tz_convert(None).to_pydatetime()
            else:
                capture_time_value = None
        else:
            capture_time_value = capture_time_value

        # Insert vào fact table
        dw_cur.execute("""
            INSERT INTO fact_stream_snapshot (
                platform_id, game_id, streamer_id,
                viewers, capture_time
            ) VALUES (%s, %s, %s, %s, %s)
        """, (platform_id, game_id, streamer_id, viewer_count, capture_time_value))
        rows_loaded += 1

    print(f"Da tai {rows_loaded} dong vao fact_stream_snapshot")

    # ===============================
    # 6. COMMIT VÀ ĐÓNG KẾT NỐI
    # ===============================
    # Bước 3.6 (Người 3) - Commit giao dịch và cập nhật trạng thái job
    dw_conn.commit()
    dw_cur.close()
    dw_conn.close()

    # Cập nhật Control DB
    if control_conn and job_id:
        try:
            update_etl_job(job_id, "completed", rows_loaded, None, control_conn)
            print("Da cap nhat ETL job trong Control DB: completed")
        except Exception as e:
            print(f"Canh bao: Khong the cap nhat Control DB: {e}")
        finally:
            try:
                control_conn.close()
            except:
                pass

    print("Load du lieu vao DW thanh cong!")

except Exception as e:
    error_msg = f"Loi khi tai du lieu vao DW: {e}"
    error_traceback = "".join(traceback.format_exc())
    full_error = f"{error_msg}\n{error_traceback}"
    print(full_error, file=sys.stderr)
    print(full_error)

    # Đóng kết nối nếu có
    try:
        dw_conn.rollback()
        dw_cur.close()
        dw_conn.close()
    except:
        pass

    # Ghi lỗi vào Control DB
    if control_conn and job_id:
        try:
            update_etl_job(job_id, "failed", 0, error_msg, control_conn)
        except Exception as log_error:
            print(f"Canh bao: Khong the ghi loi vao Control DB: {log_error}")
        finally:
            try:
                control_conn.close()
            except:
                pass

    exit(1)

