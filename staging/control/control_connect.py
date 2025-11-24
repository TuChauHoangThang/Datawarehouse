"""
Control Database Connection Utilities
Các hàm tiện ích để kết nối và làm việc với Control Database
"""

import os
import yaml
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Optional, Dict, List, Any
import sys


def get_config_path():
    """Lấy đường dẫn đến file config"""
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # => D:\DW\staging
    return os.path.join(BASE_DIR, "config_dw.yaml")


def get_control_connection():
    """Kết nối đến Control Database và trả về connection object"""
    CONFIG_PATH = get_config_path()
    
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Khong tim thay file cau hinh tai: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    control_info = config.get("control_db")
    if not control_info:
        # Fallback về staging_db nếu không có control_db
        control_info = config.get("staging_db")
        if not control_info:
            raise ValueError("Khong tim thay 'control_db' hoac 'staging_db' trong file cau hinh")
        control_info = control_info.copy()
        control_info["dbname"] = control_info.get("dbname", "control_dw")
    
    return psycopg2.connect(
        host=control_info["host"],
        port=control_info["port"],
        dbname=control_info.get("dbname", "control_dw"),
        user=control_info["user"],
        password=control_info["password"]
    )


def load_sources_from_yaml() -> Dict[str, Dict[str, Any]]:
    """Đọc block 'sources' trong config_dw.yaml để lấy cấu hình nguồn"""
    config_path = get_config_path()
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    sources = config.get("sources", {})
    if not isinstance(sources, dict):
        return {}
    return sources


def ensure_control_tables(conn=None):
    """Đảm bảo các bảng Control DB tồn tại (tạo nếu thiếu)"""
    should_close = False
    if conn is None:
        conn = get_control_connection()
        should_close = True
    
    cur = conn.cursor()
    
    try:
        # Tạo bảng etl_job_control
        cur.execute("""
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
        
        # Tạo bảng file_audit_log
        cur.execute("""
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
        
        # Tạo bảng batch_run_history
        cur.execute("""
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
        
        # Tạo indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_audit_batch ON file_audit_log(batch_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_audit_status ON file_audit_log(status);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_etl_job_status ON etl_job_control(status);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_batch_date ON batch_run_history(batch_date);
        """)
        
        # Bảng log chi tiết từng bước ETL
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_step_log (
                step_id SERIAL PRIMARY KEY,
                job_id INT NOT NULL REFERENCES etl_job_control(job_id) ON DELETE CASCADE,
                step_name VARCHAR(100) NOT NULL,
                status VARCHAR(20) NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                records_processed INT DEFAULT 0,
                info TEXT,
                error_message TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_step_job ON etl_step_log(job_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_step_status ON etl_step_log(status);")
        
        # Bảng log sự kiện job
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_job_event (
                event_id SERIAL PRIMARY KEY,
                job_id INT NOT NULL REFERENCES etl_job_control(job_id) ON DELETE CASCADE,
                event_type VARCHAR(50) NOT NULL,
                message TEXT,
                payload JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_event_job ON etl_job_event(job_id);")
        
        # Bảng cấu hình nguồn dữ liệu
        cur.execute("""
            CREATE TABLE IF NOT EXISTS external_source_config (
                config_id SERIAL PRIMARY KEY,
                source_name VARCHAR(100) NOT NULL,
                config_key VARCHAR(100) NOT NULL,
                config_value TEXT NOT NULL,
                is_secret BOOLEAN DEFAULT FALSE,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (source_name, config_key)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_external_source ON external_source_config(source_name);")
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def create_etl_job(job_name: str, conn=None) -> int:
    """Tạo một ETL job mới và trả về job_id"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO etl_job_control (job_name, status, start_time)
            VALUES (%s, 'running', %s)
            RETURNING job_id;
        """, (job_name, datetime.now()))
        job_id = cur.fetchone()[0]
        conn.commit()
        return job_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def update_etl_job(job_id: int, status: str, records_processed: int = 0, 
                   error_message: Optional[str] = None, conn=None):
    """Cập nhật trạng thái ETL job"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE etl_job_control
            SET status = %s,
                end_time = %s,
                records_processed = %s,
                error_message = %s
            WHERE job_id = %s;
        """, (status, datetime.now(), records_processed, error_message, job_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def start_etl_step(job_id: int, step_name: str, conn=None) -> int:
    """Bắt đầu một step mới cho job"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO etl_step_log (job_id, step_name, status, start_time)
            VALUES (%s, %s, 'running', %s)
            RETURNING step_id;
        """, (job_id, step_name, datetime.now()))
        step_id = cur.fetchone()[0]
        conn.commit()
        return step_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def update_etl_step(step_id: int, status: str, records_processed: int = 0,
                    info: Optional[str] = None, error_message: Optional[str] = None,
                    conn=None):
    """Cập nhật trạng thái của step"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE etl_step_log
            SET status = %s,
                end_time = %s,
                records_processed = %s,
                info = %s,
                error_message = %s
            WHERE step_id = %s;
        """, (status, datetime.now(), records_processed, info, error_message, step_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def log_job_event(job_id: int, event_type: str, message: str,
                  payload: Optional[Dict[str, Any]] = None, conn=None):
    """Ghi log sự kiện của job (stdout, stderr, config snapshot, ...)"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        payload_json = Json(payload) if payload is not None else None
        cur.execute("""
            INSERT INTO etl_job_event (job_id, event_type, message, payload)
            VALUES (%s, %s, %s, %s);
        """, (job_id, event_type, message, payload_json))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def get_source_config(source_name: str, required_keys: Optional[List[str]] = None,
                      conn=None) -> Dict[str, str]:
    """Đọc cấu hình của nguồn dữ liệu từ Control DB"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT config_key, config_value
            FROM external_source_config
            WHERE source_name = %s;
        """, (source_name,))
        rows = cur.fetchall()
        db_config = {row[0]: row[1] for row in rows}

        yaml_sources = load_sources_from_yaml()
        yaml_config = yaml_sources.get(source_name, {})
        if yaml_config and not isinstance(yaml_config, dict):
            raise ValueError(f"Cau hinh source '{source_name}' trong YAML khong hop le (phai la dict)")

        merged_config: Dict[str, Any] = {}
        if yaml_config:
            merged_config.update({k: v for k, v in yaml_config.items() if v not in (None, "")})
        if db_config:
            merged_config.update({k: v for k, v in db_config.items() if v not in (None, "")})

        if required_keys:
            missing = [k for k in required_keys if k not in merged_config or merged_config[k] in (None, "")]
            if missing:
                raise KeyError(f"Thieu cac truong cau hinh bat buoc {missing} cho source '{source_name}'")
        
        return merged_config
    finally:
        cur.close()
        if should_close:
            conn.close()


def upsert_source_config(source_name: str, config_key: str, config_value: str,
                         is_secret: bool = False, description: Optional[str] = None,
                         conn=None):
    """Cập nhật hoặc tạo mới cấu hình nguồn dữ liệu"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO external_source_config (source_name, config_key, config_value, is_secret, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_name, config_key)
            DO UPDATE SET
                config_value = EXCLUDED.config_value,
                is_secret = EXCLUDED.is_secret,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP;
        """, (source_name, config_key, config_value, is_secret, description))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def create_batch_run(batch_date: Optional[datetime] = None, conn=None) -> int:
    """Tạo một batch run mới và trả về batch_id"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    if batch_date is None:
        batch_date = datetime.now().date()
    elif isinstance(batch_date, datetime):
        batch_date = batch_date.date()
    
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO batch_run_history (batch_date, batch_start_time, status)
            VALUES (%s, %s, 'running')
            RETURNING batch_id;
        """, (batch_date, datetime.now()))
        batch_id = cur.fetchone()[0]
        conn.commit()
        return batch_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def update_batch_run(batch_id: int, status: str, files_processed: int = 0,
                     total_records: int = 0, error_summary: Optional[str] = None, conn=None):
    """Cập nhật trạng thái batch run"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE batch_run_history
            SET batch_end_time = %s,
                status = %s,
                files_processed = %s,
                total_records = %s,
                error_summary = %s
            WHERE batch_id = %s;
        """, (datetime.now(), status, files_processed, total_records, error_summary, batch_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def log_file_audit(file_name: str, file_path: str, file_size: int, 
                   records_count: int, status: str, batch_id: Optional[int] = None,
                   error_message: Optional[str] = None, conn=None):
    """Ghi log file audit"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO file_audit_log 
            (file_name, file_path, file_size, records_count, status, batch_id, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (file_name, file_path, file_size, records_count, status, batch_id, error_message))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        if should_close:
            conn.close()


def get_recent_jobs(limit: int = 20, conn=None) -> List[Dict[str, Any]]:
    """Lấy danh sách các ETL jobs gần đây"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT job_id, job_name, status, start_time, end_time, 
                   records_processed, error_message, created_at
            FROM etl_job_control
            ORDER BY start_time DESC
            LIMIT %s;
        """, (limit,))
        
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            job = {}
            for col, val in zip(columns, row):
                if hasattr(val, 'isoformat'):
                    job[col] = val.isoformat()
                else:
                    job[col] = val
            result.append(job)
        
        return result
    finally:
        cur.close()
        if should_close:
            conn.close()


def get_recent_batches(limit: int = 20, conn=None) -> List[Dict[str, Any]]:
    """Lấy danh sách các batch runs gần đây"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT batch_id, batch_date, batch_start_time, batch_end_time,
                   status, files_processed, total_records, error_summary
            FROM batch_run_history
            ORDER BY batch_start_time DESC
            LIMIT %s;
        """, (limit,))
        
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            batch = {}
            for col, val in zip(columns, row):
                if hasattr(val, 'isoformat'):
                    batch[col] = val.isoformat()
                else:
                    batch[col] = val
            result.append(batch)
        
        return result
    finally:
        cur.close()
        if should_close:
            conn.close()


def get_file_audit_logs(batch_id: Optional[int] = None, limit: int = 50, conn=None) -> List[Dict[str, Any]]:
    """Lấy danh sách file audit logs"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        if batch_id:
            cur.execute("""
                SELECT log_id, file_name, file_path, file_size, records_count,
                       status, loaded_at, batch_id, error_message
                FROM file_audit_log
                WHERE batch_id = %s
                ORDER BY loaded_at DESC
                LIMIT %s;
            """, (batch_id, limit))
        else:
            cur.execute("""
                SELECT log_id, file_name, file_path, file_size, records_count,
                       status, loaded_at, batch_id, error_message
                FROM file_audit_log
                ORDER BY loaded_at DESC
                LIMIT %s;
            """, (limit,))
        
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            log = {}
            for col, val in zip(columns, row):
                if hasattr(val, 'isoformat'):
                    log[col] = val.isoformat()
                else:
                    log[col] = val
            result.append(log)
        
        return result
    finally:
        cur.close()
        if should_close:
            conn.close()


def get_etl_statistics(conn=None) -> Dict[str, Any]:
    """Lấy thống kê tổng quan về ETL"""
    if conn is None:
        conn = get_control_connection()
        should_close = True
    else:
        should_close = False
    
    cur = conn.cursor()
    try:
        stats = {}
        
        # Tổng số jobs
        cur.execute("SELECT COUNT(*) FROM etl_job_control;")
        stats['total_jobs'] = cur.fetchone()[0]
        
        # Jobs theo trạng thái
        cur.execute("""
            SELECT status, COUNT(*) 
            FROM etl_job_control 
            GROUP BY status;
        """)
        stats['jobs_by_status'] = {row[0]: row[1] for row in cur.fetchall()}
        
        # Tổng số batches
        cur.execute("SELECT COUNT(*) FROM batch_run_history;")
        stats['total_batches'] = cur.fetchone()[0]
        
        # Batches theo trạng thái
        cur.execute("""
            SELECT status, COUNT(*) 
            FROM batch_run_history 
            GROUP BY status;
        """)
        stats['batches_by_status'] = {row[0]: row[1] for row in cur.fetchall()}
        
        # Tổng số files
        cur.execute("SELECT COUNT(*) FROM file_audit_log;")
        stats['total_files'] = cur.fetchone()[0]
        
        # Files theo trạng thái
        cur.execute("""
            SELECT status, COUNT(*) 
            FROM file_audit_log 
            GROUP BY status;
        """)
        stats['files_by_status'] = {row[0]: row[1] for row in cur.fetchall()}
        
        # Tổng records đã xử lý
        cur.execute("SELECT SUM(records_processed) FROM etl_job_control WHERE records_processed > 0;")
        result = cur.fetchone()[0]
        stats['total_records_processed'] = result if result else 0
        
        # Job gần nhất
        cur.execute("""
            SELECT job_name, status, start_time, end_time 
            FROM etl_job_control 
            ORDER BY start_time DESC 
            LIMIT 1;
        """)
        latest_job = cur.fetchone()
        if latest_job:
            stats['latest_job'] = {
                'job_name': latest_job[0],
                'status': latest_job[1],
                'start_time': latest_job[2].isoformat() if latest_job[2] else None,
                'end_time': latest_job[3].isoformat() if latest_job[3] else None
            }
        
        return stats
    finally:
        cur.close()
        if should_close:
            conn.close()

