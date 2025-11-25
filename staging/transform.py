"""
Người số 2 - Data Prep & Transform Lead
Chạy lệnh: python staging\\transform.py
Nhiệm vụ:
1. Đọc snapshot mới nhất từ staging_dw.stg_stream_snapshot
2. Chuẩn hóa schema + xử lý dữ liệu thiếu
3. Đổ dữ liệu sạch vào bảng tạm fact_stream_snapshot_ready trên DW
"""

import os
import sys
import traceback
from typing import Dict, Any

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

# Đảm bảo import được control manager
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # => D:\DW\staging
CONTROL_DIR = os.path.join(BASE_DIR, "control")
if CONTROL_DIR not in sys.path:
    sys.path.insert(0, CONTROL_DIR)

from control_manager import ControlManager  # type: ignore  # noqa: E402

CONFIG_PATH = os.path.join(BASE_DIR, "config_dw.yaml")


def load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Khong tim thay file cau hinh tai: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def get_db_info(cfg: Dict[str, Any], key: str) -> Dict[str, Any]:
    info = cfg.get(key)
    if not info:
        raise KeyError(f"Khong tim thay khoa '{key}' trong file cau hinh YAML.")
    required = ["host", "port", "dbname", "user", "password"]
    missing = [field for field in required if not info.get(field)]
    if missing:
        raise KeyError(f"Cau hinh '{key}' thieu cac truong: {missing}")
    return info


def build_connection_url(info: Dict[str, Any]) -> str:
    return (
        f"postgresql+psycopg2://{info['user']}:{info['password']}"
        f"@{info['host']}:{info['port']}/{info['dbname']}"
    )


def run_transform():
    # Bước 2.6 (Người 2) - Đọc cấu hình & tạo kết nối tới staging và DW
    cfg = load_config()
    staging_info = get_db_info(cfg, "staging_db")
    dw_info = get_db_info(cfg, "dw_db")

    staging_engine = create_engine(build_connection_url(staging_info))
    dw_engine = create_engine(build_connection_url(dw_info))

    with ControlManager("transform_data") as control:
        # Đọc dữ liệu staging
        # Bước 2.7 (Người 2) - Đọc bảng stg_stream_snapshot từ staging_dw
        control.start_step("read_staging")
        print("Dang doc du lieu tu staging DB (stg_stream_snapshot)...")
        df_staging = pd.read_sql("SELECT * FROM stg_stream_snapshot", staging_engine)
        print(f"Da doc {len(df_staging)} dong tu staging DB")
        control.finish_step("read_staging", records_processed=len(df_staging))

        if df_staging.empty:
            message = "Khong co du lieu trong bang stg_stream_snapshot."
            control.log_event("no_data", message, {})
            control.mark_job_failed(message)
            raise SystemExit(message)

        # Transform
        # Bước 2.8 (Người 2) - Chuẩn hóa dataframe (schema, missing values, datetime)
        control.start_step("transform_snapshot")
        df_staging.columns = [c.strip().lower().replace(" ", "_") for c in df_staging.columns]
        control.finish_step("transform_snapshot", records_processed=len(df_staging))

        # Bước 2.9 (Người 2) - Ghi bảng tạm fact_stream_snapshot_ready vào DW
        control.start_step("load_dw_ready_table")
        print("Dang tao bang tam fact_stream_snapshot_ready trong DW...")
        with dw_engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_stream_snapshot_ready"))
            conn.commit()

        df_staging.to_sql("fact_stream_snapshot_ready", dw_engine, if_exists="replace", index=False)
        print(f"Da tao bang tam fact_stream_snapshot_ready voi {len(df_staging)} dong du lieu trong DW")
        control.finish_step("load_dw_ready_table", records_processed=len(df_staging))

        control.log_event(
            "transform_completed",
            "Da hoan thanh buoc transform",
            {"records": len(df_staging)}
        )
        control.mark_job_completed(records_processed=len(df_staging))


if __name__ == "__main__":
    try:
        run_transform()
    except SystemExit:
        raise
    except Exception as exc:
        error_msg = f"Loi khi transform du lieu: {exc}"
        print(error_msg, file=sys.stderr)
        print("".join(traceback.format_exc()), file=sys.stderr)
        raise

