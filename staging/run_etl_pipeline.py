"""
Người số 1 - Orchestrator & Extract Lead
Chạy lệnh: python staging\\run_etl_pipeline.py
Vai trò:
1. Lên lịch (5 phút/lần) và theo dõi batch trong Control DB
2. Gọi lần lượt các script extract, staging, transform, load DW trong thư mục staging
3. Tổng hợp kết quả, cập nhật batch_run_history
"""

import subprocess
import sys
import time
import os
from datetime import datetime, timedelta

# ===============================
# CAU HINH
# ===============================
# Interval giữa các lần chạy (giây)
INTERVAL_SECONDS = 300  # 5 phút = 300 giây

# Đường dẫn các script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # => D:\DW\staging
ROOT_DIR = os.path.dirname(BASE_DIR)  # => D:\DW

# Import control utilities
sys.path.insert(0, os.path.join(BASE_DIR, "control"))
try:
    from control_connect import create_batch_run, update_batch_run, get_file_audit_logs  # type: ignore  # noqa: E402
    CONTROL_AVAILABLE = True
except ImportError:
    CONTROL_AVAILABLE = False
    print("Warning: Control database utilities not available")

SCRIPTS = {
    "extract_twitch": os.path.join(BASE_DIR, "extract_twitch_data.py"),
    "extract_youtube": os.path.join(BASE_DIR, "extract_youtube_data.py"),
    "load_staging": os.path.join(BASE_DIR, "load_to_staging.py"),
    "transform": os.path.join(BASE_DIR, "transform.py"),
    "load_dw": os.path.join(BASE_DIR, "load_to_dw.py"),
}

# ===============================
# HAM CHAY SCRIPT
# ===============================
def run_script(script_path, script_name):
    """Chay mot script Python va tra ve ket qua"""
    print(f"\n{'='*60}")
    print(f" [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Dang chay: {script_name}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=ROOT_DIR
        )
        
        if result.returncode == 0:
            print(f" {script_name} - THANH CONG")
            if result.stdout:
                print(result.stdout)
            return True
        else:
            print(f" {script_name} - THAT BAI")
            print(f"Return code: {result.returncode}")
            if result.stdout:
                print(f"STDOUT:\n{result.stdout}")
            if result.stderr:
                print(f"STDERR:\n{result.stderr}")
            if not result.stdout and not result.stderr:
                print("Khong co thong bao loi nao duoc tra ve")
            return False
            
    except Exception as e:
        print(f" Loi khi chay {script_name}: {e}")
        return False

# ===============================
# HAM CHAY TOAN BO PIPELINE
# ===============================
def run_etl_pipeline():
    """Chay toan bo pipeline ETL"""
    print(f"\n{'#'*60}")
    print(f" BAT DAU ETL PIPELINE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    # Tạo batch record nếu control DB available
    batch_id = None
    batch_start_time = datetime.now()
    if CONTROL_AVAILABLE:
        try:
            # Bước 1.1 (Người 1) - Khởi tạo batch run mới trong Control DB
            batch_id = create_batch_run()
            print(f" Da tao batch record: batch_id={batch_id}")
        except Exception as e:
            print(f" Loi khi tao batch record: {e}")
    
    results = {}
    error_summary = []
    
    # Bước 1.2 (Người 1) - Gọi script extract Twitch
    results["extract_twitch"] = run_script(SCRIPTS["extract_twitch"], "Extract Twitch Data")
    if not results["extract_twitch"]:
        error_summary.append("Extract Twitch failed")
    
    # Bước 1.3 (Người 1) - Gọi script extract YouTube
    results["extract_youtube"] = run_script(SCRIPTS["extract_youtube"], "Extract YouTube Data")
    if not results["extract_youtube"]:
        error_summary.append("Extract YouTube failed")
    
    # Bước 1.4 (Người 1) - Kiểm tra rawData để chuẩn bị cho bước load staging
    raw_data_dir = os.path.join(BASE_DIR, "rawData")
    has_data_to_load = False
    csv_files = []
    if os.path.exists(raw_data_dir):
        csv_files = [f for f in os.listdir(raw_data_dir) if f.endswith("_raw.csv")]
        has_data_to_load = len(csv_files) > 0
        if csv_files:
            print(f" Tim thay {len(csv_files)} file CSV trong rawData: {', '.join(csv_files)}")
    
    # Bước 1.5 (Người 1) - Trigger script load staging (Người 2 chịu trách nhiệm chính)
    if has_data_to_load:
        results["load_staging"] = run_script(SCRIPTS["load_staging"], "Load to Staging DB")
        if not results["load_staging"]:
            error_summary.append("Load to Staging failed")
    else:
        print(" Bo qua buoc Load to Staging: Khong co file CSV nao trong rawData.")
        results["load_staging"] = False
    
    # Bước 1.6 (Người 1) - Trigger transform staging -> ready table
    if results.get("load_staging"):
        results["transform"] = run_script(SCRIPTS["transform"], "Transform Data")
        if not results["transform"]:
            error_summary.append("Transform failed")
    else:
        results["transform"] = False
    
    # Bước 1.7 (Người 1) - Trigger load Data Warehouse
    if results.get("transform"):
        results["load_dw"] = run_script(SCRIPTS["load_dw"], "Load to Data Warehouse")
        if not results["load_dw"]:
            error_summary.append("Load to DW failed")
    else:
        results["load_dw"] = False
    
    # Bước 1.8 (Người 1) - Tổng hợp kết quả và in báo cáo lô chạy
    print(f"\n{'#'*60}")
    print(f" TOM TAT ETL PIPELINE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    for step, success in results.items():
        if success is True:
            status = "THANH CONG"
        elif success is False:
            status = "THAT BAI"
        else:
            status = "KHONG CHAY"
        print(f"{step}: {status}")
    
    # Bước 1.9 (Người 1) - Cập nhật batch_run_history trong Control DB
    if batch_id is not None and CONTROL_AVAILABLE:
        try:
            # Đếm số file đã xử lý
            files_processed = len(csv_files) if csv_files else 0
            total_records = 0
            
            # Lấy file audit logs gần đây để đếm records
            try:
                file_logs = get_file_audit_logs(limit=100)
                # Lọc các file được load sau khi batch bắt đầu
                for log in file_logs:
                    # Lọc các file được load gần đây (trong batch này)
                    loaded_at_str = log.get("loaded_at")
                    if loaded_at_str:
                        try:
                            if isinstance(loaded_at_str, str):
                                loaded_at = datetime.fromisoformat(loaded_at_str.replace('Z', '+00:00'))
                            else:
                                loaded_at = loaded_at_str
                            # Chỉ đếm file được load sau khi batch bắt đầu (trong vòng 15 phút)
                            time_diff = (loaded_at - batch_start_time).total_seconds()
                            if time_diff >= -60 and time_diff <= 900 and log.get("status") == "loaded":
                                total_records += log.get("records_count", 0)
                        except Exception:
                            # Nếu không parse được thời gian, đếm tất cả file loaded
                            if log.get("status") == "loaded":
                                total_records += log.get("records_count", 0)
            except Exception as e:
                print(f" Loi khi lay file audit logs: {e}")
                # Fallback: đếm từ số file CSV
                if csv_files:
                    # Ước tính records từ số file (mỗi file thường có vài trăm đến vài nghìn records)
                    total_records = files_processed * 1000  # Ước tính
            
            # Xác định status batch
            all_success = all(results.values())
            has_partial = any(results.values()) and not all_success
            if all_success:
                batch_status = "success"
            elif has_partial:
                batch_status = "partial"
            else:
                batch_status = "failed"
            
            error_msg = "; ".join(error_summary) if error_summary else None
            update_batch_run(
                batch_id=batch_id,
                status=batch_status,
                files_processed=files_processed,
                total_records=total_records,
                error_summary=error_msg
            )
            print(f" Da cap nhat batch record: batch_id={batch_id}, status={batch_status}, files={files_processed}, records={total_records}")
        except Exception as e:
            print(f" Loi khi cap nhat batch record: {e}")
    
    return all(results.values())

# ===============================
# MAIN - CHAY LIEN TUC MOI 5 PHUT
# ===============================
if __name__ == "__main__":
    print("="*60)
    print(" ETL PIPELINE SCHEDULER")
    print("="*60)
    print(f" Interval: {INTERVAL_SECONDS} giay ({INTERVAL_SECONDS//60} phut)")
    print(f" Thu muc lam viec: {ROOT_DIR}")
    print("="*60)
    print("\n Nhan Ctrl+C de dung scheduler")
    print("="*60)
    
    try:
        while True:
            # Chay pipeline
            run_etl_pipeline()
            
            # Dem nguoc den lan chay tiep theo
            print(f"\n Cho {INTERVAL_SECONDS} giay ({INTERVAL_SECONDS//60} phut) truoc lan chay tiep theo...")
            print(f" Lan chay tiep theo: {(datetime.now() + timedelta(seconds=INTERVAL_SECONDS)).strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Cho den lan chay tiep theo
            time.sleep(INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n\n Da dung scheduler theo yeu cau nguoi dung")
        print(" Tam biet!")

